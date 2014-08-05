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
import java.nio.channels.ServerSocketChannel;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import net.dsys.commons.api.future.CallbackFuture;
import net.dsys.snio.api.channel.AcceptListener;
import net.dsys.snio.api.channel.CloseListener;
import net.dsys.snio.api.channel.MessageServerChannel;
import net.dsys.snio.api.pool.Acceptor;
import net.dsys.snio.api.pool.KeyAcceptor;
import net.dsys.snio.api.pool.SelectorExecutor;
import net.dsys.snio.api.pool.SelectorPool;

/**
 * @author Ricardo Padilha
 */
final class TCPServerChannel<T> implements MessageServerChannel<T>, Acceptor {

	private final SelectorExecutor selector;
	private final KeyAcceptor<T> acceptor;
	private ServerSocketChannel channel;

	TCPServerChannel(@Nonnull final SelectorPool pool, @Nonnull final KeyAcceptor<T> acceptor) {
		if (pool == null) {
			throw new NullPointerException("pool == null");
		}
		if (acceptor == null) {
			throw new NullPointerException("acceptor == null");
		}
		this.selector = pool.next();
		this.acceptor = acceptor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public KeyAcceptor<?> getAcceptor() {
		return acceptor;
	}

	void open() throws IOException {
		if (channel != null) {
			return;
		}
		channel = ServerSocketChannel.open();
		channel.configureBlocking(false);
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
	public TCPServerChannel<T> bind(final SocketAddress local) throws IOException {
		assert isOpen();
		channel.bind(local);
		selector.bind(channel, this);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkChannel bind(final SocketAddress local, final int backlog) throws IOException {
		assert isOpen();
		channel.bind(local, backlog);
		selector.bind(channel, this);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CallbackFuture<Void> getBindFuture() {
		return acceptor.getBindFuture();
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
	public void close() throws IOException {
		final ServerSocketChannel channel = this.channel;
		final Callable<Void> task = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				if (channel != null) {
					channel.close();
				}
				return null;
			}
		};
		acceptor.close(selector, task);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CallbackFuture<Void> getCloseFuture() {
		return acceptor.getCloseFuture();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TCPServerChannel<T> onAccept(final AcceptListener<T> listener) {
		acceptor.onAccept(listener);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TCPServerChannel<T> onClose(final CloseListener<T> listener) {
		acceptor.onClose(listener);
		return this;
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
	public <E> MessageServerChannel<?> setOption(final SocketOption<E> name, final E value) throws IOException {
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
