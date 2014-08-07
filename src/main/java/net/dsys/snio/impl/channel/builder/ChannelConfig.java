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

package net.dsys.snio.impl.channel.builder;

import java.nio.ByteBuffer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.builder.Mandatory;
import net.dsys.commons.impl.builder.OptionGroup;
import net.dsys.commons.impl.builder.Optional;
import net.dsys.commons.impl.lang.ByteBufferFactory;
import net.dsys.commons.impl.lang.DirectByteBufferFactory;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.impl.buffer.BlockingQueueProvider;
import net.dsys.snio.impl.buffer.RingBufferProvider;

/**
 * @author Ricardo Padilha
 */
public final class ChannelConfig<T> {

	/**
	 * Value obtained empirically on a 3rd gen Core i7. Lower values means
	 * lower throughput, higher values mean more erratic throughput.
	 */
	private static final int DEFAULT_CAPACITY = 256;
	private static final int DEFAULT_BUFFER_SIZE = 0xFFFF;

	private SelectorPool pool;
	private int capacity;
	private int sendBufferSize;
	private int receiveBufferSize;
	private boolean useDirectBuffer;
	private boolean useRingBuffer;
	private boolean singleInputBuffer;
	private MessageBufferConsumer<T> consumer;

	public ChannelConfig() {
		this.pool = null;
		this.capacity = DEFAULT_CAPACITY;
		this.sendBufferSize = DEFAULT_BUFFER_SIZE;
		this.receiveBufferSize = DEFAULT_BUFFER_SIZE;
		this.useDirectBuffer = false;
		this.useRingBuffer = false;
		this.singleInputBuffer = false;
		this.consumer = null;
	}

	@Nonnull
	@Mandatory(restrictions = "pool != null")
	public ChannelConfig<T> setPool(@Nonnull final SelectorPool pool) {
		if (pool == null) {
			throw new NullPointerException("pool == null");
		}
		this.pool = pool;
		return this;
	}

	@Nonnull
	@Optional(defaultValue = "256", restrictions = "capacity > 0")
	public ChannelConfig<T> setBufferCapacity(@Nonnegative final int capacity) {
		if (capacity < 1) {
			throw new IllegalArgumentException("capacity < 1");
		}
		this.capacity = capacity;
		return this;
	}

	@Nonnull
	@Optional(defaultValue = "0xFFFF", restrictions = "sendBufferSize > 0")
	public ChannelConfig<T> setSendBufferSize(@Nonnegative final int sendBufferSize) {
		if (sendBufferSize < 1) {
			throw new IllegalArgumentException("sendBufferSize < 1");
		}
		this.sendBufferSize = sendBufferSize;
		return this;
	}

	@Nonnull
	@Optional(defaultValue = "0xFFFF", restrictions = "receiveBufferSize > 0")
	public ChannelConfig<T> setReceiveBufferSize(@Nonnegative final int receiveBufferSize) {
		if (receiveBufferSize < 1) {
			throw new IllegalArgumentException("receiveBufferSize < 1");
		}
		this.receiveBufferSize = receiveBufferSize;
		return this;
	}

	@Nonnull
	@Optional(defaultValue = "useHeapBuffer()")
	@OptionGroup(name = "bufferType", seeAlso = "useHeapBuffer()")
	public ChannelConfig<T> useDirectBuffer() {
		this.useDirectBuffer = true;
		return this;
	}

	@Nonnull
	@Optional(defaultValue = "useHeapBuffer()")
	@OptionGroup(name = "bufferType", seeAlso = "useDirectBuffer()")
	public ChannelConfig<T> useHeapBuffer() {
		this.useDirectBuffer = false;
		return this;
	}

	@Nonnull
	@Optional(defaultValue = "useBlockingQueue()", restrictions = "requires disruptor library")
	@OptionGroup(name = "bufferImplementation", seeAlso = "useBlockingQueue()")
	public ChannelConfig<T> useRingBuffer() {
		this.useRingBuffer = true;
		return this;
	}

	@Nonnull
	@Optional(defaultValue = "useBlockingQueue()")
	@OptionGroup(name = "bufferImplementation", seeAlso = "useRingBuffer()")
	public ChannelConfig<T> useBlockingQueue() {
		this.useRingBuffer = false;
		return this;
	}

	@Nonnull
	@Optional(defaultValue = "useMultipleInputBuffers()")
	@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(consumer), useMultipleInputBuffers()")
	public ChannelConfig<T> useSingleInputBuffer() {
		this.singleInputBuffer = true;
		this.consumer = null;
		return this;
	}

	@Nonnull
	@Optional(defaultValue = "useMultipleInputBuffers()", restrictions = "consumer != null")
	@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useMultipleInputBuffers()")
	public ChannelConfig<T> useSingleInputBuffer(@Nonnull final MessageBufferConsumer<T> consumer) {
		if (consumer == null) {
			throw new NullPointerException("consumer == null");
		}
		this.singleInputBuffer = true;
		this.consumer = consumer;
		return this;
	}

	@Nonnull
	@Optional(defaultValue = "useMultipleInputBuffers()")
	@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useSingleInputBuffer(consumer)")
	public ChannelConfig<T> useMultipleInputBuffers() {
		this.singleInputBuffer = false;
		this.consumer = null;
		return this;
	}

	@Nonnull
	public SelectorPool getPool() {
		if (pool == null) {
			throw new IllegalStateException("pool is undefined");
		}
		return pool;
	}

	@Nonnegative
	public int getCapacity() {
		return capacity;
	}

	@Nonnegative
	public int getSendBufferSize() {
		return sendBufferSize;
	}

	@Nonnegative
	public int getReceiveBufferSize() {
		return receiveBufferSize;
	}

	public boolean isDirectBuffer() {
		return useDirectBuffer;
	}

	@Nonnull
	public Factory<ByteBuffer> getFactory(@Nonnegative final int length) {
		if (useDirectBuffer) {
			return new DirectByteBufferFactory(length);
		}
		return new ByteBufferFactory(length);
	}

	public boolean isRingBuffer() {
		return useRingBuffer;
	}

	@Nonnull
	public MessageBufferProvider<T> getProvider(@Nonnull final Factory<T> factory) {
		final MessageBufferProvider<T> provider;
		if (useRingBuffer) {
			if (singleInputBuffer) {
				MessageBufferConsumer<T> cons = consumer;
				if (cons == null) {
					cons = RingBufferProvider.createConsumer(capacity, factory);
				}
				provider = RingBufferProvider.createProvider(capacity, factory, cons);
			} else {
				provider = RingBufferProvider.createProvider(capacity, factory);
			}
		} else {
			if (singleInputBuffer) {
				MessageBufferConsumer<T> cons = consumer;
				if (cons == null) {
					cons = BlockingQueueProvider.createConsumer(capacity, factory);
				}
				provider = RingBufferProvider.createProvider(capacity, factory, cons);
			} else {
				provider = RingBufferProvider.createProvider(capacity, factory);
			}
		}
		return provider;
	}

	@Nonnull
	public Factory<MessageBufferProvider<T>> getProviderFactory(@Nonnull final Factory<T> factory) {
		final Factory<MessageBufferProvider<T>> provider;
		if (useRingBuffer) {
			if (singleInputBuffer) {
				MessageBufferConsumer<T> cons = consumer;
				if (cons == null) {
					cons = RingBufferProvider.createConsumer(capacity, factory);
				}
				provider = RingBufferProvider.createProviderFactory(capacity, factory, cons);
			} else {
				provider = RingBufferProvider.createProviderFactory(capacity, factory);
			}
		} else {
			if (singleInputBuffer) {
				MessageBufferConsumer<T> cons = consumer;
				if (cons == null) {
					cons = BlockingQueueProvider.createConsumer(capacity, factory);
				}
				provider = BlockingQueueProvider.createProviderFactory(capacity, factory, cons);
			} else {
				provider = BlockingQueueProvider.createProviderFactory(capacity, factory);
			}
		}
		return provider;
	}
}
