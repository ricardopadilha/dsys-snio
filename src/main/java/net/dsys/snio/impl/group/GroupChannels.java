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

package net.dsys.snio.impl.group;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.dsys.commons.api.lang.BinaryUnit;
import net.dsys.commons.api.lang.Copier;
import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.builder.Mandatory;
import net.dsys.commons.impl.builder.OptionGroup;
import net.dsys.commons.impl.builder.Optional;
import net.dsys.commons.impl.lang.ByteBufferCopier;
import net.dsys.commons.impl.lang.ByteBufferFactory;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.channel.RateLimiter;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.impl.buffer.BlockingQueueProvider;
import net.dsys.snio.impl.buffer.RingBufferProvider;
import net.dsys.snio.impl.channel.MessageChannels;
import net.dsys.snio.impl.channel.MessageChannels.TCPChannelBuilder;
import net.dsys.snio.impl.channel.builder.CommonBuilderData;
import net.dsys.snio.impl.channel.builder.ServerBuilderData;

/**
 * @author Ricardo Padilha
 */
public final class GroupChannels {

	private GroupChannels() {
		// no instantiation
		return;
	}

	public static TCPGroupBuilder newTCPGroup() {
		return new TCPGroupBuilder();
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class TCPGroupBuilder {

		private final CommonBuilderData<ByteBuffer> common;
		private final ServerBuilderData server;
		private int size;

		TCPGroupBuilder() {
			this.common = new CommonBuilderData<>();
			this.server = new ServerBuilderData();
		}

		@Mandatory(restrictions = "pool != null")
		public TCPGroupBuilder setPool(final SelectorPool pool) {
			common.setPool(pool);
			return this;
		}

		@Optional(defaultValue = "256", restrictions = "capacity > 0")
		public TCPGroupBuilder setBufferCapacity(final int capacity) {
			common.setBufferCapacity(capacity);
			return this;
		}

		@Optional(defaultValue = "0xFFFF", restrictions = "sendBufferSize > 0")
		public TCPGroupBuilder setSendBufferSize(final int sendBufferSize) {
			common.setSendBufferSize(sendBufferSize);
			return this;
		}

		@Optional(defaultValue = "0xFFFF", restrictions = "receiveBufferSize > 0")
		public TCPGroupBuilder setReceiveBufferSize(final int receiveBufferSize) {
			common.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useHeapBuffer()")
		public TCPGroupBuilder useDirectBuffer() {
			common.useDirectBuffer();
			return this;
		}

		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useDirectBuffer()")
		public TCPGroupBuilder useHeapBuffer() {
			common.useHeapBuffer();
			return this;
		}

		@Optional(defaultValue = "useBlockingQueue()", restrictions = "requires disruptor library")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useBlockingQueue()")
		public TCPGroupBuilder useRingBuffer() {
			common.useRingBuffer();
			return this;
		}

		@Optional(defaultValue = "useBlockingQueue()")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useRingBuffer()")
		public TCPGroupBuilder useBlockingQueue() {
			common.useBlockingQueue();
			return this;
		}

		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(consumer), useMultipleInputBuffers()")
		public TCPGroupBuilder useSingleInputBuffer() {
			common.useSingleInputBuffer();
			return this;
		}

		@Optional(defaultValue = "useMultipleInputBuffers()", restrictions = "consumer != null")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useMultipleInputBuffers()")
		public TCPGroupBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			common.useSingleInputBuffer(consumer);
			return this;
		}

		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useSingleInputBuffer(consumer)")
		public TCPGroupBuilder useMultipleInputBuffers() {
			common.useMultipleInputBuffers();
			return this;
		}

		@Mandatory(restrictions = "codecs != null")
		@OptionGroup(name = "codec", seeAlso = "setMessageLength(length)")
		public TCPGroupBuilder setMessageCodec(final Factory<MessageCodec> codecs) {
			server.setMessageCodec(codecs);
			return this;
		}

		@Mandatory(restrictions = "length > 0")
		@OptionGroup(name = "codec", seeAlso = "setMessageCodec(codecs)")
		public TCPGroupBuilder setMessageLength(final int length) {
			server.setMessageLength(length);
			return this;
		}

		@Mandatory(restrictions = "limiters != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimit(value, unit)")
		public TCPGroupBuilder setRateLimiter(final Factory<RateLimiter> limiters) {
			server.setRateLimiter(limiters);
			return this;
		}

		@Mandatory(restrictions = "value >= 1 && unit != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimiter(limiters)")
		public TCPGroupBuilder setRateLimit(final long value, final BinaryUnit unit) {
			server.setRateLimit(value, unit);
			return this;
		}

		@Mandatory(restrictions = "size > 0")
		public TCPGroupBuilder setGroupSize(final int size) {
			if (size < 1) {
				throw new IllegalArgumentException("size < 1");
			}
			this.size = size;
			return this;
		}

		public GroupChannel<ByteBuffer> open() throws IOException {
			final SelectorPool pool = common.getPool();
			final int capacity = common.getCapacity();
			final int sendBufferSize = common.getSendBufferSize();
			final int receiveBufferSize = common.getReceiveBufferSize();
			final boolean directBuffer = common.isDirectBuffer();
			final boolean ringBuffer = common.isRingBuffer();
			final Factory<MessageCodec> codecs = server.getMessageCodecs();
			final Factory<RateLimiter> limiters = server.getRateLimiters();
			final Factory<ByteBuffer> factory = new ByteBufferFactory(codecs.newInstance().getBodyLength());
			final MessageBufferConsumer<ByteBuffer> consumer;
			if (common.isRingBuffer()) {
				consumer = RingBufferProvider.createConsumer(common.getCapacity(), factory);
			} else {
				consumer = BlockingQueueProvider.createConsumer(common.getCapacity(), factory);
			}
			final Copier<ByteBuffer> copier = new ByteBufferCopier();
			final ChannelFactory<ByteBuffer> builder = new ChannelFactory<ByteBuffer>() {
				@Override
				public MessageChannel<ByteBuffer> open() throws IOException {
					final TCPChannelBuilder builder = MessageChannels.newTCPChannel();
					builder.setPool(pool)
							.setBufferCapacity(capacity)
							.setSendBufferSize(sendBufferSize)
							.setReceiveBufferSize(receiveBufferSize)
							.setMessageCodec(codecs.newInstance())
							.setRateLimiter(limiters.newInstance())
							.useSingleInputBuffer(consumer);
					if (directBuffer) {
						builder.useDirectBuffer();
					}
					if (ringBuffer) {
						builder.useRingBuffer();
					}
					return builder.open();
				}
			};
			final GroupChannel<ByteBuffer> channel = new GroupChannel<>(consumer, builder, copier);
			channel.open(size);
			return channel;
		}
	}
}
