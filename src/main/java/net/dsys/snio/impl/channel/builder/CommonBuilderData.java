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
public final class CommonBuilderData<T> {

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

	public CommonBuilderData() {
		capacity = DEFAULT_CAPACITY;
		sendBufferSize = DEFAULT_BUFFER_SIZE;
		receiveBufferSize = DEFAULT_BUFFER_SIZE;
		useDirectBuffer = false;
		useRingBuffer = false;
		singleInputBuffer = false;
		consumer = null;
	}

	@Mandatory(restrictions = "pool != null")
	public CommonBuilderData<T> setPool(final SelectorPool pool) {
		if (pool == null) {
			throw new NullPointerException("pool == null");
		}
		this.pool = pool;
		return this;
	}

	@Optional(defaultValue = "256", restrictions = "capacity > 0")
	public CommonBuilderData<T> setBufferCapacity(final int capacity) {
		if (capacity < 1) {
			throw new IllegalArgumentException("capacity < 1");
		}
		this.capacity = capacity;
		return this;
	}

	@Optional(defaultValue = "0xFFFF", restrictions = "sendBufferSize > 0")
	public CommonBuilderData<T> setSendBufferSize(final int sendBufferSize) {
		if (sendBufferSize < 1) {
			throw new IllegalArgumentException("sendBufferSize < 1");
		}
		this.sendBufferSize = sendBufferSize;
		return this;
	}

	@Optional(defaultValue = "0xFFFF", restrictions = "receiveBufferSize > 0")
	public CommonBuilderData<T> setReceiveBufferSize(final int receiveBufferSize) {
		if (receiveBufferSize < 1) {
			throw new IllegalArgumentException("receiveBufferSize < 1");
		}
		this.receiveBufferSize = receiveBufferSize;
		return this;
	}

	@Optional(defaultValue = "useHeapBuffer()")
	@OptionGroup(name = "bufferType", seeAlso = "useHeapBuffer()")
	public CommonBuilderData<T> useDirectBuffer() {
		this.useDirectBuffer = true;
		return this;
	}

	@Optional(defaultValue = "useHeapBuffer()")
	@OptionGroup(name = "bufferType", seeAlso = "useDirectBuffer()")
	public CommonBuilderData<T> useHeapBuffer() {
		this.useDirectBuffer = false;
		return this;
	}

	@Optional(defaultValue = "useBlockingQueue()", restrictions = "requires disruptor library")
	@OptionGroup(name = "bufferImplementation", seeAlso = "useBlockingQueue()")
	public CommonBuilderData<T> useRingBuffer() {
		this.useRingBuffer = true;
		return this;
	}

	@Optional(defaultValue = "useBlockingQueue()")
	@OptionGroup(name = "bufferImplementation", seeAlso = "useRingBuffer()")
	public CommonBuilderData<T> useBlockingQueue() {
		this.useRingBuffer = false;
		return this;
	}

	@Optional(defaultValue = "useMultipleInputBuffers()")
	@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(consumer), useMultipleInputBuffers()")
	public CommonBuilderData<T> useSingleInputBuffer() {
		this.singleInputBuffer = true;
		this.consumer = null;
		return this;
	}

	@Optional(defaultValue = "useMultipleInputBuffers()", restrictions = "consumer != null")
	@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useMultipleInputBuffers()")
	public CommonBuilderData<T> useSingleInputBuffer(final MessageBufferConsumer<T> consumer) {
		this.singleInputBuffer = true;
		this.consumer = consumer;
		return this;
	}

	@Optional(defaultValue = "useMultipleInputBuffers()")
	@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useSingleInputBuffer(consumer)")
	public CommonBuilderData<T> useMultipleInputBuffers() {
		this.singleInputBuffer = false;
		this.consumer = null;
		return this;
	}

	public SelectorPool getPool() {
		return pool;
	}

	public int getCapacity() {
		return capacity;
	}

	public int getSendBufferSize() {
		return sendBufferSize;
	}

	public int getReceiveBufferSize() {
		return receiveBufferSize;
	}

	public boolean isDirectBuffer() {
		return useDirectBuffer;
	}

	public Factory<ByteBuffer> getFactory(final int length) {
		if (useDirectBuffer) {
			return new DirectByteBufferFactory(length);
		}
		return new ByteBufferFactory(length);
	}

	public boolean isRingBuffer() {
		return useRingBuffer;
	}

	public MessageBufferProvider<T> getProvider(final Factory<T> factory) {
		final MessageBufferProvider<T> provider;
		if (useRingBuffer) {
			if (singleInputBuffer) {
				MessageBufferConsumer<T> cons = consumer;
				if (cons == null) {
					cons = RingBufferProvider.createConsumer(capacity, factory);
				}
				provider = new RingBufferProvider<>(capacity, factory, cons);
			} else {
				provider = new RingBufferProvider<>(capacity, factory);
			}
		} else {
			if (singleInputBuffer) {
				MessageBufferConsumer<T> cons = consumer;
				if (cons == null) {
					cons = BlockingQueueProvider.createConsumer(capacity, factory);
				}
				provider = new BlockingQueueProvider<>(capacity, factory, cons);
			} else {
				provider = new BlockingQueueProvider<>(capacity, factory);
			}
		}
		return provider;
	}

	public Factory<MessageBufferProvider<T>> getProviderFactory(final Factory<T> factory) {
		final Factory<MessageBufferProvider<T>> provider;
		if (useRingBuffer) {
			if (singleInputBuffer) {
				MessageBufferConsumer<T> cons = consumer;
				if (cons == null) {
					cons = RingBufferProvider.createConsumer(capacity, factory);
				}
				provider = RingBufferProvider.createFactory(capacity, factory, cons);
			} else {
				provider = RingBufferProvider.createFactory(capacity, factory);
			}
		} else {
			if (singleInputBuffer) {
				MessageBufferConsumer<T> cons = consumer;
				if (cons == null) {
					cons = BlockingQueueProvider.createConsumer(capacity, factory);
				}
				provider = BlockingQueueProvider.createFactory(capacity, factory, cons);
			} else {
				provider = BlockingQueueProvider.createFactory(capacity, factory);
			}
		}
		return provider;
	}
}
