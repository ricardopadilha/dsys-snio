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
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.Callable;

import net.dsys.commons.api.future.CallbackFuture;
import net.dsys.commons.impl.future.SettableCallbackFuture;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.channel.CloseListener;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.pool.KeyProcessor;
import net.dsys.snio.api.pool.Processor;
import net.dsys.snio.api.pool.SelectorExecutor;

/**
 * @author Ricardo Padilha
 */
final class TCPChannel<T> implements MessageChannel<T>, Processor {

	private final SelectorExecutor selector;
	private final KeyProcessor<T> processor;
	private CloseListener<T> close;
	private SocketChannel channel;

	TCPChannel(final SelectorExecutor selector, final KeyProcessor<T> processor, final SocketChannel channel,
			final CloseListener<T> onClose) {
		if (selector == null) {
			throw new NullPointerException("selector == null");
		}
		if (processor == null) {
			throw new NullPointerException("processor == null");
		}
		this.selector = selector;
		this.processor = processor;
		this.channel = channel;
		this.close = onClose;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public KeyProcessor<?> getProcessor() {
		return processor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageChannel<T> onClose(final CloseListener<T> listener) {
		this.close = listener;
		return this;
	}

	void open() throws IOException {
		if (channel == null) {
			channel = SocketChannel.open();
			channel.configureBlocking(false);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isOpen() {
		return channel != null && channel.isOpen();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageChannel<T> bind(final SocketAddress local) throws IOException {
		assert isOpen();
		channel.bind(local);
		return this;
	}

	/**
	 * Same as {@link #bind(SocketAddress)}, i.e., {@code backlog} is ignored.
	 * 
	 * {@inheritDoc}
	 */
	@Override
	public NetworkChannel bind(final SocketAddress local, final int backlog) throws IOException {
		return bind(local);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CallbackFuture<Void> getBindFuture() {
		final SettableCallbackFuture<Void> future = new SettableCallbackFuture<>();
		future.success(null);
		return future;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SocketAddress getLocalAddress() throws IOException {
		if (channel == null) {
			return null;
		}
		return channel.getLocalAddress();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void connect(final SocketAddress remote) throws IOException {
		assert isOpen();
		channel.connect(remote);
		selector.connect(channel, this);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CallbackFuture<Void> getConnectFuture() {
		return processor.getConnectionFuture();
	}

	void register() {
		assert isOpen();
		selector.register(channel, this);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageBufferConsumer<T> getInputBuffer() {
		return processor.getInputBuffer();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageBufferProducer<T> getOutputBuffer() {
		return processor.getOutputBuffer();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		final SocketChannel channel = this.channel;
		final CloseListener<T> close = this.close;
		final Callable<Void> task = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				if (channel != null) {
					channel.close();
				}
				if (close != null) {
					close.connectionClosed(TCPChannel.this);
				}
				return null;
			}
		};
		processor.close(selector, task);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CallbackFuture<Void> getCloseFuture() {
		return processor.getCloseFuture();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<SocketOption<?>> supportedOptions() {
		assert isOpen();
		return channel.supportedOptions();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <E> MessageChannel<?> setOption(final SocketOption<E> name, final E value) throws IOException {
		assert isOpen();
		channel.setOption(name, value);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <E> E getOption(final SocketOption<E> name) throws IOException {
		assert isOpen();
		return channel.getOption(name);
	}

}
