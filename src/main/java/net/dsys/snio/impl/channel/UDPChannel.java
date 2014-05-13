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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.DatagramChannel;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.channel.CloseListener;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.pool.KeyProcessor;
import net.dsys.snio.api.pool.Processor;
import net.dsys.snio.api.pool.SelectorExecutor;
import net.dsys.snio.api.pool.SelectorPool;

/**
 * @author Ricardo Padilha
 */
final class UDPChannel<T> implements MessageChannel<T>, Processor {

	private final SelectorExecutor selector;
	private final KeyProcessor<T> processor;
	private CloseListener<T> close;
	private DatagramChannel channel;

	UDPChannel(final SelectorPool pool, final KeyProcessor<T> processor) {
		if (pool == null) {
			throw new NullPointerException("pool == null");
		}
		if (processor == null) {
			throw new NullPointerException("processor == null");
		}
		this.selector = pool.next();
		this.processor = processor;
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
			channel = DatagramChannel.open();
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
	 * If a multicast address is given, the channel will join the group and bind to the port.
	 * 
	 * {@inheritDoc}
	 */
	@Override
	public MessageChannel<T> bind(final SocketAddress local) throws IOException {
		assert isOpen();
		if (local instanceof InetSocketAddress && ((InetSocketAddress) local).getAddress().isMulticastAddress()) {
			final InetSocketAddress inet = (InetSocketAddress) local;
			final InetSocketAddress port = new InetSocketAddress(inet.getPort());
			final InetAddress addr = inet.getAddress();
			channel.bind(port);
			channel.join(addr, null);
		} else {
			channel.bind(local);
		}
		selector.register(channel, this);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Future<Void> getBindFuture() {
		return processor.getConnectionFuture();
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
	 * If a multicast address is given, the channel will join the group instead of connecting to it.
	 * 
	 * {@inheritDoc}
	 */
	@Override
	public void connect(final SocketAddress remote) throws IOException {
		assert isOpen();
		if (remote instanceof InetSocketAddress && ((InetSocketAddress) remote).getAddress().isMulticastAddress()) {
			final InetAddress addr = ((InetSocketAddress) remote).getAddress();
			channel.join(addr, null);
		} else {
			channel.connect(remote);
		}
		selector.register(channel, this);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Future<Void> getConnectFuture() {
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
		final DatagramChannel channel = this.channel;
		final CloseListener<T> close = this.close;
		final Callable<Void> task = new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				if (channel != null) {
					channel.close();
				}
				if (close != null) {
					close.connectionClosed(UDPChannel.this);
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
	public Future<Void> getCloseFuture() {
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
	public <E> MessageChannel<T> setOption(final SocketOption<E> name, final E value) throws IOException {
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
