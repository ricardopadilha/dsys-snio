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
import java.nio.ByteBuffer;

import javax.net.ssl.SSLContext;

import net.dsys.commons.api.lang.BinaryUnit;
import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.builder.Mandatory;
import net.dsys.commons.impl.builder.OptionGroup;
import net.dsys.commons.impl.builder.Optional;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.channel.MessageServerChannel;
import net.dsys.snio.api.channel.RateLimiter;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.pool.KeyAcceptor;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.impl.channel.builder.CommonBuilderData;
import net.dsys.snio.impl.channel.builder.ServerBuilderData;

/**
 * Helper class to create {@link MessageServerChannel} instances.
 * 
 * @author Ricardo Padilha
 */
public final class MessageServerChannels {

	private MessageServerChannels() {
		// no instantiation
	}

	public static TCPServerChannelBuilder newTCPServerChannel() {
		return new TCPServerChannelBuilder();
	}

	public static SSLServerChannelBuilder newSSLServerChannel() {
		return new SSLServerChannelBuilder();
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class TCPServerChannelBuilder {

		private final CommonBuilderData<ByteBuffer> common;
		private final ServerBuilderData server;

		TCPServerChannelBuilder() {
			this.common = new CommonBuilderData<>();
			this.server = new ServerBuilderData();
		}

		@Mandatory(restrictions = "pool != null")
		public TCPServerChannelBuilder setPool(final SelectorPool pool) {
			common.setPool(pool);
			return this;
		}

		@Optional(defaultValue = "256", restrictions = "capacity > 0")
		public TCPServerChannelBuilder setBufferCapacity(final int capacity) {
			common.setBufferCapacity(capacity);
			return this;
		}

		@Optional(defaultValue = "0xFFFF", restrictions = "sendBufferSize > 0")
		public TCPServerChannelBuilder setSendBufferSize(final int sendBufferSize) {
			common.setSendBufferSize(sendBufferSize);
			return this;
		}

		@Optional(defaultValue = "0xFFFF", restrictions = "receiveBufferSize > 0")
		public TCPServerChannelBuilder setReceiveBufferSize(final int receiveBufferSize) {
			common.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useHeapBuffer()")
		public TCPServerChannelBuilder useDirectBuffer() {
			common.useDirectBuffer();
			return this;
		}

		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useDirectBuffer()")
		public TCPServerChannelBuilder useHeapBuffer() {
			common.useHeapBuffer();
			return this;
		}

		@Optional(defaultValue = "useBlockingQueue()", restrictions = "requires disruptor library")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useBlockingQueue()")
		public TCPServerChannelBuilder useRingBuffer() {
			common.useRingBuffer();
			return this;
		}

		@Optional(defaultValue = "useBlockingQueue()")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useRingBuffer()")
		public TCPServerChannelBuilder useBlockingQueue() {
			common.useBlockingQueue();
			return this;
		}

		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(consumer), useMultipleInputBuffers()")
		public TCPServerChannelBuilder useSingleInputBuffer() {
			common.useSingleInputBuffer();
			return this;
		}

		@Optional(defaultValue = "useMultipleInputBuffers()", restrictions = "consumer != null")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useMultipleInputBuffers()")
		public TCPServerChannelBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			common.useSingleInputBuffer(consumer);
			return this;
		}

		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useSingleInputBuffer(consumer)")
		public TCPServerChannelBuilder useMultipleInputBuffers() {
			common.useMultipleInputBuffers();
			return this;
		}

		@Mandatory(restrictions = "codecs != null")
		@OptionGroup(name = "codec", seeAlso = "setMessageLength(length)")
		public TCPServerChannelBuilder setMessageCodec(final Factory<MessageCodec> codecs) {
			server.setMessageCodec(codecs);
			return this;
		}

		@Mandatory(restrictions = "length > 0")
		@OptionGroup(name = "codec", seeAlso = "setMessageCodec(codecs)")
		public TCPServerChannelBuilder setMessageLength(final int length) {
			server.setMessageLength(length);
			return this;
		}

		@Mandatory(restrictions = "limiters != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimit(value, unit)")
		public TCPServerChannelBuilder setRateLimiter(final Factory<RateLimiter> limiters) {
			server.setRateLimiter(limiters);
			return this;
		}

		@Mandatory(restrictions = "value >= 1 && unit != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimiter(limiters)")
		public TCPServerChannelBuilder setRateLimit(final long value, final BinaryUnit unit) {
			server.setRateLimit(value, unit);
			return this;
		}

		public MessageServerChannel<ByteBuffer> open() throws IOException {
			final Factory<MessageCodec> codecs = server.getMessageCodecs();
			final Factory<RateLimiter> limiters = server.getRateLimiters();
			final Factory<ByteBuffer> factory = common.getFactory(codecs.newInstance().getBodyLength());
			final Factory<MessageBufferProvider<ByteBuffer>> provider = common.getProviderFactory(factory);
			final SelectorPool pool = common.getPool();
			final KeyAcceptor<ByteBuffer> acceptor = new TCPAcceptor(pool, codecs, limiters, provider,
					common.getSendBufferSize(), common.getReceiveBufferSize());
			final TCPServerChannel<ByteBuffer> channel = new TCPServerChannel<>(pool, acceptor);
			channel.open();
			return channel;
		}
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class SSLServerChannelBuilder {

		private final CommonBuilderData<ByteBuffer> common;
		private final ServerBuilderData server;
		private SSLContext context;

		SSLServerChannelBuilder() {
			this.common = new CommonBuilderData<>();
			this.server = new ServerBuilderData();
		}

		@Mandatory(restrictions = "pool != null")
		public SSLServerChannelBuilder setPool(final SelectorPool pool) {
			common.setPool(pool);
			return this;
		}

		@Optional(defaultValue = "256", restrictions = "capacity > 0")
		public SSLServerChannelBuilder setBufferCapacity(final int capacity) {
			common.setBufferCapacity(capacity);
			return this;
		}

		@Optional(defaultValue = "0xFFFF", restrictions = "sendBufferSize > 0")
		public SSLServerChannelBuilder setSendBufferSize(final int sendBufferSize) {
			common.setSendBufferSize(sendBufferSize);
			return this;
		}

		@Optional(defaultValue = "0xFFFF", restrictions = "receiveBufferSize > 0")
		public SSLServerChannelBuilder setReceiveBufferSize(final int receiveBufferSize) {
			common.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useHeapBuffer()")
		public SSLServerChannelBuilder useDirectBuffer() {
			common.useDirectBuffer();
			return this;
		}

		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useDirectBuffer()")
		public SSLServerChannelBuilder useHeapBuffer() {
			common.useHeapBuffer();
			return this;
		}

		@Optional(defaultValue = "useBlockingQueue()", restrictions = "requires disruptor library")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useBlockingQueue()")
		public SSLServerChannelBuilder useRingBuffer() {
			common.useRingBuffer();
			return this;
		}

		@Optional(defaultValue = "useBlockingQueue()")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useRingBuffer()")
		public SSLServerChannelBuilder useBlockingQueue() {
			common.useBlockingQueue();
			return this;
		}

		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(consumer), useMultipleInputBuffers()")
		public SSLServerChannelBuilder useSingleInputBuffer() {
			common.useSingleInputBuffer();
			return this;
		}

		@Optional(defaultValue = "useMultipleInputBuffers()", restrictions = "consumer != null")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useMultipleInputBuffers()")
		public SSLServerChannelBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			common.useSingleInputBuffer(consumer);
			return this;
		}

		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useSingleInputBuffer(consumer)")
		public SSLServerChannelBuilder useMultipleInputBuffers() {
			common.useMultipleInputBuffers();
			return this;
		}

		@Mandatory(restrictions = "codecs != null")
		@OptionGroup(name = "codec", seeAlso = "setMessageLength(length)")
		public SSLServerChannelBuilder setMessageCodec(final Factory<MessageCodec> codecs) {
			server.setMessageCodec(codecs);
			return this;
		}

		@Mandatory(restrictions = "length > 0")
		@OptionGroup(name = "codec", seeAlso = "setMessageCodec(codecs)")
		public SSLServerChannelBuilder setMessageLength(final int length) {
			server.setMessageLength(length);
			return this;
		}

		@Mandatory(restrictions = "limiters != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimit(value, unit)")
		public SSLServerChannelBuilder setRateLimiter(final Factory<RateLimiter> limiters) {
			server.setRateLimiter(limiters);
			return this;
		}

		@Mandatory(restrictions = "value >= 1 && unit != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimiter(limiters)")
		public SSLServerChannelBuilder setRateLimit(final long value, final BinaryUnit unit) {
			server.setRateLimit(value, unit);
			return this;
		}

		@Mandatory(restrictions = "context != null")
		public SSLServerChannelBuilder setContext(final SSLContext context) {
			if (context == null) {
				throw new NullPointerException("context == null");
			}
			this.context = context;
			return this;
		}

		public MessageServerChannel<ByteBuffer> open() throws IOException {
			final Factory<MessageCodec> codecs = server.getMessageCodecs();
			final Factory<RateLimiter> limiters = server.getRateLimiters();
			final Factory<ByteBuffer> factory = common.getFactory(codecs.newInstance().getBodyLength());
			final Factory<MessageBufferProvider<ByteBuffer>> provider = common.getProviderFactory(factory);
			final SelectorPool pool = common.getPool();
			final KeyAcceptor<ByteBuffer> acceptor = new SSLAcceptor(pool, codecs, limiters, provider,
					common.getSendBufferSize(), common.getReceiveBufferSize(), context);
			final TCPServerChannel<ByteBuffer> channel = new TCPServerChannel<>(pool, acceptor);
			channel.open();
			return channel;
		}
	}
}
