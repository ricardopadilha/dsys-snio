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

import javax.annotation.Nonnull;

import net.dsys.commons.api.lang.BinaryUnit;
import net.dsys.commons.api.lang.Copier;
import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.lang.ByteBufferCopier;
import net.dsys.commons.impl.lang.ByteBufferFactory;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.limit.RateLimiter;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.impl.buffer.BlockingQueueProvider;
import net.dsys.snio.impl.buffer.RingBufferProvider;
import net.dsys.snio.impl.channel.MessageChannels;
import net.dsys.snio.impl.channel.builder.ChannelConfig;
import net.dsys.snio.impl.channel.builder.ClientConfig;
import net.dsys.snio.impl.group.builder.GroupConfig;

/**
 * @author Ricardo Padilha
 */
public final class GroupChannels {

	private GroupChannels() {
		// no instantiation
		return;
	}

	@Nonnull
	public static GroupChannel<ByteBuffer> openTCPGroup(final ChannelConfig<ByteBuffer> common,
			final GroupConfig group) throws IOException {
		final Factory<MessageCodec> codecs = group.getMessageCodecs();
		final Factory<RateLimiter> limiters = group.getRateLimiters();
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
				final ClientConfig client = new ClientConfig()
					.setMessageCodec(codecs.newInstance())
					.setRateLimiter(limiters.newInstance());
				return MessageChannels.openTCPChannel(common, client);
			}
		};
		final GroupChannel<ByteBuffer> channel = new GroupChannel<>(consumer, builder, copier);
		channel.open(group.getSize());
		return channel;
	}

	public static TCPGroupBuilder newTCPGroup() {
		return new TCPGroupBuilder();
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class TCPGroupBuilder {

		private final ChannelConfig<ByteBuffer> common;
		private final GroupConfig group;

		TCPGroupBuilder() {
			this.common = new ChannelConfig<>();
			this.group = new GroupConfig();
		}

		/**
		 * @see ChannelConfig#setPool(SelectorPool)
		 */
		public TCPGroupBuilder setPool(final SelectorPool pool) {
			common.setPool(pool);
			return this;
		}

		/**
		 * @see ChannelConfig#setBufferCapacity(int)
		 */
		public TCPGroupBuilder setBufferCapacity(final int capacity) {
			common.setBufferCapacity(capacity);
			return this;
		}

		/**
		 * @see ChannelConfig#setSendBufferSize(int)
		 */
		public TCPGroupBuilder setSendBufferSize(final int sendBufferSize) {
			common.setSendBufferSize(sendBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#setReceiveBufferSize(int)
		 */
		public TCPGroupBuilder setReceiveBufferSize(final int receiveBufferSize) {
			common.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#useDirectBuffer()
		 */
		public TCPGroupBuilder useDirectBuffer() {
			common.useDirectBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useHeapBuffer()
		 */
		public TCPGroupBuilder useHeapBuffer() {
			common.useHeapBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useRingBuffer()
		 */
		public TCPGroupBuilder useRingBuffer() {
			common.useRingBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useBlockingQueue()
		 */
		public TCPGroupBuilder useBlockingQueue() {
			common.useBlockingQueue();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer()
		 */
		public TCPGroupBuilder useSingleInputBuffer() {
			common.useSingleInputBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer(net.dsys.snio.api.buffer.MessageBufferConsumer)
		 */
		public TCPGroupBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			common.useSingleInputBuffer(consumer);
			return this;
		}

		/**
		 * @see ChannelConfig#useMultipleInputBuffers()
		 */
		public TCPGroupBuilder useMultipleInputBuffers() {
			common.useMultipleInputBuffers();
			return this;
		}

		/**
		 * @see GroupConfig#setMessageCodec(Factory)
		 */
		public TCPGroupBuilder setMessageCodec(final Factory<MessageCodec> codecs) {
			group.setMessageCodec(codecs);
			return this;
		}

		/**
		 * @see GroupConfig#setMessageLength(int)
		 */
		public TCPGroupBuilder setMessageLength(final int length) {
			group.setMessageLength(length);
			return this;
		}

		/**
		 * @see GroupConfig#setRateLimiter(Factory)
		 */
		public TCPGroupBuilder setRateLimiter(final Factory<RateLimiter> limiters) {
			group.setRateLimiter(limiters);
			return this;
		}

		/**
		 * @see GroupConfig#setRateLimit(long, BinaryUnit)
		 */
		public TCPGroupBuilder setRateLimit(final long value, final BinaryUnit unit) {
			group.setRateLimit(value, unit);
			return this;
		}

		/**
		 * @see GroupConfig#setGroupSize(int)
		 */
		public TCPGroupBuilder setGroupSize(final int size) {
			group.setGroupSize(size);
			return this;
		}

		public GroupChannel<ByteBuffer> open() throws IOException {
			return openTCPGroup(common, group);
		}
	}
}
