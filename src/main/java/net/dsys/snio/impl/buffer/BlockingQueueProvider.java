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

package net.dsys.snio.impl.buffer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.api.lang.Factory;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.pool.KeyProcessor;

/**
 * @author Ricardo Padilha
 */
public final class BlockingQueueProvider<T> implements MessageBufferProvider<T> {

	static final int INITIAL_SEQUENCE_VALUE = -1;

	private final BlockingBuffer<T> out; // app -> channel
	private final BlockingBuffer<T> in; // channel -> app
	private final BlockingQueueProducer<T> appOut; // app producer
	private final BlockingQueueConsumer<T> chnIn; // channel consumer
	private final MessageBufferProducer<T> chnOut; // channel producer
	private final MessageBufferConsumer<T> appIn; // app consumer
	private final boolean internalConsumer;

	public BlockingQueueProvider(@Nonnegative final int capacity, @Nonnull final Factory<T> factory) {
		this.out = new BlockingBuffer<>(capacity);
		this.in = new BlockingBuffer<>(capacity);
		this.appOut = new BlockingQueueProducer<>(out, factory);
		this.chnIn = new BlockingQueueConsumer<>(out, factory);
		this.chnOut = new BlockingQueueProducer<>(in, factory);
		this.appIn = new BlockingQueueConsumer<>(in, factory);
		this.internalConsumer = true;
	}

	public BlockingQueueProvider(@Nonnegative final int capacity, @Nonnull final Factory<T> factory,
			@Nonnull final MessageBufferConsumer<T> consumer) {
		if (consumer == null) {
			throw new NullPointerException("appIn == null");
		}
		this.out = new BlockingBuffer<>(capacity);
		this.in = null;
		this.appOut = new BlockingQueueProducer<>(out, factory);
		this.chnIn = new BlockingQueueConsumer<>(out, factory);
		this.chnOut = consumer.createProducer();
		this.appIn = consumer;
		this.internalConsumer = false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageBufferProducer<T> getAppOutput(final KeyProcessor<T> processor) {
		appOut.setProcessor(processor);
		return appOut;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageBufferConsumer<T> getChannelInput() {
		return chnIn;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageBufferProducer<T> getChannelOutput() {
		return chnOut;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageBufferConsumer<T> getAppInput() {
		return appIn;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		appOut.close();
		chnIn.close();
		if (internalConsumer) {
			((BlockingQueueProducer<?>) chnOut).close();
			((BlockingQueueConsumer<?>) appIn).close();
		}
	}

	public static <T> BlockingQueueConsumer<T> createConsumer(final int capacity, final Factory<T> factory) {
		final BlockingBuffer<T> buffer = new BlockingBuffer<>(capacity);
		final BlockingQueueConsumer<T> consumer = new BlockingQueueConsumer<>(buffer, factory);
		return consumer;
	}

	public static <T> Factory<MessageBufferProvider<T>> createFactory(final int capacity, final Factory<T> factory) {
		return new ProviderFactory<>(capacity, factory);
	}

	public static <T> Factory<MessageBufferProvider<T>> createFactory(final int capacity, final Factory<T> factory,
			final MessageBufferConsumer<T> consumer) {
		return new ProviderFactory<>(capacity, factory, consumer);
	}

	private static final class ProviderFactory<T> implements Factory<MessageBufferProvider<T>> {

		private final int capacity;
		private final Factory<T> factory;
		private final MessageBufferConsumer<T> consumer;

		ProviderFactory(@Nonnegative final int capacity, @Nonnull final Factory<T> factory) {
			if (capacity < 1) {
				throw new IllegalArgumentException("capacity < 1");
			}
			if (factory == null) {
				throw new NullPointerException("factory == null");
			}
			this.capacity = capacity;
			this.factory = factory;
			this.consumer = null;
		}

		ProviderFactory(@Nonnegative final int capacity, @Nonnull final Factory<T> factory,
				@Nonnull final MessageBufferConsumer<T> consumer) {
			if (capacity < 1) {
				throw new IllegalArgumentException("capacity < 1");
			}
			if (factory == null) {
				throw new NullPointerException("factory == null");
			}
			if (consumer == null) {
				throw new NullPointerException("consumer == null");
			}
			this.capacity = capacity;
			this.factory = factory;
			this.consumer = consumer;
		}

		@Override
		public MessageBufferProvider<T> newInstance() {
			if (consumer != null) {
				return new BlockingQueueProvider<>(capacity, factory, consumer);
			}
			return new BlockingQueueProvider<>(capacity, factory);
		}
	}
}
