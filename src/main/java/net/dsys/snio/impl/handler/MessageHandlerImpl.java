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

package net.dsys.snio.impl.handler;

import static net.dsys.snio.impl.handler.HandlerType.MULTI_THREADED;
import static net.dsys.snio.impl.handler.HandlerType.SINGLE_THREADED;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.channel.AcceptListener;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.handler.MessageConsumer;
import net.dsys.snio.api.handler.MessageConsumerFactory;
import net.dsys.snio.api.handler.MessageHandler;

/**
 * @author Ricardo Padilha
 */
final class MessageHandlerImpl<T> implements MessageHandler<T> {

	private final HandlerType type;
	private final ExecutorService executor;
	private final ConsumerThreadFactory<T> threads;
	private final MessageConsumerFactory<T> factory;
	private final MessageConsumer<T> consumer;
	private final AcceptListener<T> delegate;
	private final AcceptListener<T> listener;
	private final AtomicBoolean started;

	MessageHandlerImpl(final ExecutorService executor, final ConsumerThreadFactory<T> threads,
			final MessageConsumerFactory<T> factory, final AcceptListener<T> delegate) {
		if (executor == null) {
			throw new NullPointerException("executor == null");
		}
		if (threads == null) {
			throw new NullPointerException("threads == null");
		}
		if (factory == null) {
			throw new NullPointerException("factory == null");
		}
		this.type = MULTI_THREADED;
		this.executor = executor;
		this.threads = threads;
		this.factory = factory;
		this.consumer = null;
		this.delegate = delegate;
		this.listener = new DefaultListener();
		this.started = null;
	}

	MessageHandlerImpl(final ExecutorService executor, final ConsumerThreadFactory<T> factory,
			final MessageConsumer<T> consumer, final AcceptListener<T> delegate) {
		if (executor == null) {
			throw new NullPointerException("executor == null");
		}
		if (factory == null) {
			throw new NullPointerException("factory == null");
		}
		if (consumer == null) {
			throw new NullPointerException("consumer == null");
		}
		this.type = SINGLE_THREADED;
		this.executor = executor;
		this.threads = factory;
		this.consumer = consumer;
		this.factory = null;
		this.delegate = delegate;
		this.listener = new DefaultListener();
		this.started = new AtomicBoolean();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AcceptListener<T> getAcceptListener() {
		return listener;
	}

	void accept(final SocketAddress remote, final MessageChannel<T> channel) {
		if (delegate != null) {
			delegate.connectionAccepted(remote, channel);
		}
		switch (type) {
			case MULTI_THREADED: {
				final MessageBufferConsumer<T> in = channel.getInputBuffer();
				final MessageConsumer<T> handler = factory.newInstance(remote, channel);
				final Runnable runnable = threads.newInstance(in, handler);
				executor.execute(runnable);
				break;
			}
			case SINGLE_THREADED:
				if (started.compareAndSet(false, true)) {
					final MessageBufferConsumer<T> in = channel.getInputBuffer();
					final Runnable runnable = threads.newInstance(in, consumer);
					executor.execute(runnable);
				}
				break;
			default: {
				throw new AssertionError("Unsupported HandlerType + " + type);
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		executor.shutdownNow();
	}

	/**
	 * @author Ricardo Padilha
	 */
	private class DefaultListener implements AcceptListener<T> {

		DefaultListener() {
			return;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void connectionAccepted(final SocketAddress remote, final MessageChannel<T> channel) {
			accept(remote, channel);
		}
	}
}
