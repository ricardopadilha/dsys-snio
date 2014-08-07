/**
 * Copyright 2014 Ricardo Padilha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dsys.snio.impl.channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import net.dsys.commons.api.future.CallbackFuture;
import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.future.SettableCallbackFuture;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.channel.AcceptListener;
import net.dsys.snio.api.channel.CloseListener;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.limit.RateLimiter;
import net.dsys.snio.api.pool.KeyAcceptor;
import net.dsys.snio.api.pool.SelectorExecutor;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.api.pool.SelectorThread;

/**
 * @author Ricardo Padilha
 */
final class SSLAcceptor implements KeyAcceptor<ByteBuffer> {

	@Nonnull
	private final SelectorPool pool;
	@Nonnull
	private final Factory<MessageCodec> codecs;
	@Nonnull
	private final Factory<RateLimiter> limiters;
	@Nonnull
	private final Factory<MessageBufferProvider<ByteBuffer>> providers;
	@Nonnull
	private final int sendSize;
	@Nonnull
	private final int receiveSize;
	@Nonnull
	private final SSLContext context;
	@Nonnull
	private final SettableCallbackFuture<Void> bindFuture;
	@Nonnull
	private final SettableCallbackFuture<Void> closeFuture;

	@Nonnull
	private AcceptListener<ByteBuffer> accept;
	@Nonnull
	private CloseListener<ByteBuffer> close;
	private SelectionKey acceptKey;

	SSLAcceptor(@Nonnull final SelectorPool pool, @Nonnull final Factory<MessageCodec> codecs,
			@Nonnull final Factory<RateLimiter> limiters,
			@Nonnull final Factory<MessageBufferProvider<ByteBuffer>> providers,
			@Nonnegative final int sendSize,
			@Nonnegative final int receiveSize,
			@Nonnull final SSLContext context) {
		if (pool == null) {
			throw new NullPointerException("pool == null");
		}
		if (codecs == null) {
			throw new IllegalArgumentException("codecs == null");
		}
		if (limiters == null) {
			throw new IllegalArgumentException("limiters == null");
		}
		if (providers == null) {
			throw new IllegalArgumentException("providers == null");
		}
		if (sendSize < 1) {
			throw new IllegalArgumentException("sendSize < 1");
		}
		if (receiveSize < 1) {
			throw new IllegalArgumentException("receiveSize < 1");
		}
		if (context == null) {
			throw new NullPointerException("context == null");
		}
		this.pool = pool;
		this.codecs = codecs;
		this.limiters = limiters;
		this.providers = providers;
		this.sendSize = sendSize;
		this.receiveSize = receiveSize;
		this.context = context;
		this.bindFuture = new SettableCallbackFuture<>();
		this.closeFuture = new SettableCallbackFuture<>();
		this.accept = MessageChannels.dummyAcceptListener();
		this.close = MessageChannels.dummyCloseListener();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onAccept(final AcceptListener<ByteBuffer> listener) {
		if (listener == null) {
			throw new NullPointerException("listener == null");
		}
		this.accept = listener;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onClose(final CloseListener<ByteBuffer> listener) {
		if (listener == null) {
			throw new NullPointerException("listener == null");
		}
		this.close = listener;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CallbackFuture<Void> getBindFuture() {
		return bindFuture;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registered(final SelectorThread thread, final SelectionKey key) {
		if (thread == null) {
			throw new NullPointerException("thread == null");
		}
		if (key == null) {
			throw new NullPointerException("key == null");
		}
		this.acceptKey = key;
		bindFuture.success(null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void accept(final SelectionKey key) throws IOException {
		final ServerSocketChannel server = (ServerSocketChannel) key.channel();
		final SocketChannel client = server.accept();
		client.configureBlocking(false);

		final MessageCodec codec = codecs.newInstance();
		final RateLimiter limiter = limiters.newInstance();
		final MessageBufferProvider<ByteBuffer> provider = providers.newInstance();
		final SSLEngine engine = context.createSSLEngine();
		engine.setUseClientMode(false);
		final SSLProcessor processor = new SSLProcessor(codec, limiter, provider, sendSize, receiveSize, engine);
		final TCPChannel<ByteBuffer> channel = new TCPChannel<>(pool.next(), processor, client, close);
		channel.open();
		channel.register();
		final Future<Void> future = channel.getConnectFuture();
		try {
			future.get();
		} catch (final InterruptedException | ExecutionException e) {
			channel.close();
			throw new IOException(e);
		}
		accept.connectionAccepted(client.getRemoteAddress(), channel);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close(final SelectorExecutor executor, final Callable<Void> closeTask) {
		executor.cancelBind(acceptKey, closeFuture, closeTask);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CallbackFuture<Void> getCloseFuture() {
		return closeFuture;
	}

}
