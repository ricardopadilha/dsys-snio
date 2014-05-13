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

import net.dsys.commons.api.lang.Copier;
import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.builder.Mandatory;
import net.dsys.commons.impl.lang.ByteBufferCopier;
import net.dsys.commons.impl.lang.ByteBufferFactory;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.impl.buffer.BlockingQueueProvider;
import net.dsys.snio.impl.buffer.RingBufferProvider;
import net.dsys.snio.impl.channel.AbstractBuilder;
import net.dsys.snio.impl.channel.MessageChannels;
import net.dsys.snio.impl.channel.MessageChannels.TCPChannelBuilder;

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
	public static final class TCPGroupBuilder extends
			AbstractBuilder<GroupChannel<ByteBuffer>, ByteBuffer> {

		private int size;

		TCPGroupBuilder() {
			return;
		}

		@Mandatory(restrictions = "size > 0")
		public TCPGroupBuilder setGroupSize(final int size) {
			if (size < 1) {
				throw new IllegalArgumentException("size < 1");
			}
			this.size = size;
			return this;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public GroupChannel<ByteBuffer> open() throws IOException {
			final MessageCodec codec = getMessageCodec();
			final Factory<ByteBuffer> factory = new ByteBufferFactory(codec.getBodyLength());
			final MessageBufferConsumer<ByteBuffer> consumer;
			if (isRingBuffer()) {
				consumer = RingBufferProvider.createConsumer(getCapacity(), factory);
			} else {
				consumer = BlockingQueueProvider.createConsumer(getCapacity(), factory);
			}
			final Copier<ByteBuffer> copier = new ByteBufferCopier();
			final TCPChannelBuilder builder = MessageChannels.newTCPChannel();
			builder.setBufferCapacity(getCapacity())
				.setMessageCodec(getCodec())
				.setPool(getPool())
				.setReceiveBufferSize(getReceiveBufferSize())
				.setSendBufferSize(getSendBufferSize())
				.useSingleInputBuffer(consumer);
			if (isDirectBuffer()) {
				builder.useDirectBuffer();
			}
			if (isRingBuffer()) {
				builder.useRingBuffer();
			}
			final GroupChannel<ByteBuffer> channel = new GroupChannel<>(consumer, builder, copier);
			channel.open(size);
			return channel;
		}
	}
}
