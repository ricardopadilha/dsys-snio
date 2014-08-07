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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;

/**
 * @author Ricardo Padilha
 */
public final class RingBufferProvider<T> implements MessageBufferProvider<T> {

	private final WakeupWaitStrategy waitOut;
	private final WaitStrategy waitIn;
	private final RingBuffer<T> out; // app -> channel
	private final RingBuffer<T> in; // channel -> app
	private final Object[] attachOut;
	private final Object[] attachIn;
	private final RingBufferProducer<T> appOut; // app producer
	private final RingBufferConsumer<T> chnIn; // channel consumer
	private final MessageBufferProducer<T> chnOut; // channel producer
	private final MessageBufferConsumer<T> appIn; // app consumer
	private final boolean internalConsumer;

	RingBufferProvider(@Nonnegative final int capacity, @Nonnull final Factory<T> factory) {
		this.waitOut = new WakeupWaitStrategy();
		this.waitIn = new BlockingWaitStrategy();
		final EventFactory<T> evfactory = wrapFactory(factory);
		this.out = RingBuffer.createMultiProducer(evfactory, capacity, waitOut);
		this.in = RingBuffer.createSingleProducer(evfactory, capacity, waitIn);
		this.attachOut = new Object[capacity];
		this.attachIn = new Object[capacity];
		this.appOut = new RingBufferProducer<>(out, attachOut);
		this.chnIn = new RingBufferConsumer<>(out, attachOut);
		this.chnOut = new RingBufferProducer<>(in, attachIn);
		this.appIn = new RingBufferConsumer<>(in, attachIn);
		this.internalConsumer = true;
	}

	RingBufferProvider(@Nonnegative final int capacity, @Nonnull final Factory<T> factory,
			@Nonnull final MessageBufferConsumer<T> appIn) {
		if (appIn == null) {
			throw new NullPointerException("appIn == null");
		}
		this.waitOut = new WakeupWaitStrategy();
		this.waitIn = null;
		final EventFactory<T> evfactory = wrapFactory(factory);
		this.out = RingBuffer.createMultiProducer(evfactory, capacity, waitOut);
		this.in = null;
		this.attachOut = new Object[capacity];
		this.attachIn = null;
		this.appOut = new RingBufferProducer<>(out, attachOut);
		this.chnIn = new RingBufferConsumer<>(out, attachOut);
		this.chnOut = appIn.createProducer();
		this.appIn = appIn;
		this.internalConsumer = false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageBufferProducer<T> getAppOutput(final KeyProcessor<T> processor) {
		waitOut.setProcessor(processor);
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
		chnIn.close();
		appOut.close();
		if (internalConsumer) {
			((RingBufferProducer<?>) chnOut).close();
			((RingBufferConsumer<?>) appIn).close();
		}
	}

	/**
	 * Convert a {@link Factory} into an {@link EventFactory}
	 */
	private static <T> EventFactory<T> wrapFactory(@Nonnull final Factory<T> factory) {
		if (factory == null) {
			throw new NullPointerException("factory == null");
		}
		return new EventFactory<T>() {
			@Override
			public T newInstance() {
				return factory.newInstance();
			}
		};
	}

	public static <T> RingBufferConsumer<T> createConsumer(@Nonnegative final int capacity,
			@Nonnull final Factory<T> factory) {
		final EventFactory<T> evfactory = wrapFactory(factory);
		final RingBuffer<T> buffer = RingBuffer.createMultiProducer(evfactory, capacity);
		final Object[] attachments = new Object[capacity];
		final RingBufferConsumer<T> consumer = new RingBufferConsumer<>(buffer, attachments);
		return consumer;
	}

	public static <T> MessageBufferProvider<T> createProvider(@Nonnegative final int capacity,
			@Nonnull final Factory<T> factory) {
		return new RingBufferProvider<>(capacity, factory);
	}

	public static <T> Factory<MessageBufferProvider<T>> createProviderFactory(@Nonnegative final int capacity,
			@Nonnull final Factory<T> factory) {
		return new ProviderFactory<>(capacity, factory);
	}

	public static <T> MessageBufferProvider<T> createProvider(@Nonnegative final int capacity,
			@Nonnull final Factory<T> factory, @Nonnull final MessageBufferConsumer<T> consumer) {
		return new RingBufferProvider<>(capacity, factory, consumer);
	}

	public static <T> Factory<MessageBufferProvider<T>> createProviderFactory(@Nonnegative final int capacity,
			@Nonnull final Factory<T> factory, @Nonnull final MessageBufferConsumer<T> consumer) {
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
				return new RingBufferProvider<>(capacity, factory, consumer);
			}
			return new RingBufferProvider<>(capacity, factory);
		}
		
	}
}
