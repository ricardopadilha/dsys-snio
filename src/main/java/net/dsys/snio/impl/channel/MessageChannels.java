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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import net.dsys.commons.api.lang.BinaryUnit;
import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.builder.Mandatory;
import net.dsys.commons.impl.builder.OptionGroup;
import net.dsys.commons.impl.builder.Optional;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.limit.RateLimiter;
import net.dsys.snio.api.pool.KeyProcessor;
import net.dsys.snio.api.pool.SelectorExecutor;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.impl.channel.builder.ClientBuilderData;
import net.dsys.snio.impl.channel.builder.CommonBuilderData;

/**
 * Helper class to create {@link MessageChannel} instances.
 * 
 * @author Ricardo Padilha
 */
public final class MessageChannels {

	private MessageChannels() {
		// no instantiation
	}

	@Nonnull
	public static TCPChannelBuilder newTCPChannel() {
		return new TCPChannelBuilder();
	}

	@Nonnull
	public static SSLChannelBuilder newSSLChannel() {
		return new SSLChannelBuilder();
	}

	@Nonnull
	public static UDPChannelBuilder newUDPChannel() {
		return new UDPChannelBuilder();
	}

	/**
	 * @author Ricardo Padilha
	 */
	@ParametersAreNonnullByDefault
	public static final class TCPChannelBuilder {

		private final CommonBuilderData<ByteBuffer> common;
		private final ClientBuilderData client;

		TCPChannelBuilder() {
			this.common = new CommonBuilderData<>();
			this.client = new ClientBuilderData();
		}

		@Nonnull
		@Mandatory(restrictions = "pool != null")
		public TCPChannelBuilder setPool(final SelectorPool pool) {
			common.setPool(pool);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "256", restrictions = "capacity > 0")
		public TCPChannelBuilder setBufferCapacity(@Nonnegative final int capacity) {
			common.setBufferCapacity(capacity);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "0xFFFF", restrictions = "sendBufferSize > 0")
		public TCPChannelBuilder setSendBufferSize(@Nonnegative final int sendBufferSize) {
			common.setSendBufferSize(sendBufferSize);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "0xFFFF", restrictions = "receiveBufferSize > 0")
		public TCPChannelBuilder setReceiveBufferSize(@Nonnegative final int receiveBufferSize) {
			common.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useHeapBuffer()")
		public TCPChannelBuilder useDirectBuffer() {
			common.useDirectBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useDirectBuffer()")
		public TCPChannelBuilder useHeapBuffer() {
			common.useHeapBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useBlockingQueue()", restrictions = "requires disruptor library")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useBlockingQueue()")
		public TCPChannelBuilder useRingBuffer() {
			common.useRingBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useBlockingQueue()")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useRingBuffer()")
		public TCPChannelBuilder useBlockingQueue() {
			common.useBlockingQueue();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(consumer), useMultipleInputBuffers()")
		public TCPChannelBuilder useSingleInputBuffer() {
			common.useSingleInputBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useMultipleInputBuffers()", restrictions = "consumer != null")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useMultipleInputBuffers()")
		public TCPChannelBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			common.useSingleInputBuffer(consumer);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useSingleInputBuffer(consumer)")
		public TCPChannelBuilder useMultipleInputBuffers() {
			common.useMultipleInputBuffers();
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "codec != null")
		@OptionGroup(name = "codec", seeAlso = "setMessageLength(length)")
		public TCPChannelBuilder setMessageCodec(final MessageCodec codec) {
			client.setMessageCodec(codec);
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "length > 0")
		@OptionGroup(name = "codec", seeAlso = "setMessageCodec(codec)")
		public TCPChannelBuilder setMessageLength(@Nonnegative final int length) {
			client.setMessageLength(length);
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "limiter != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimit(value, unit)")
		public TCPChannelBuilder setRateLimiter(final RateLimiter limiter) {
			client.setRateLimiter(limiter);
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "value >= 1 && unit != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimiter(limiter)")
		public TCPChannelBuilder setRateLimit(@Nonnegative final long value, final BinaryUnit unit) {
			client.setRateLimit(value, unit);
			return this;
		}

		@Nonnull
		public MessageChannel<ByteBuffer> open() throws IOException {
			final MessageCodec codec = client.getMessageCodec();
			final RateLimiter limiter = client.getRateLimiter();
			final Factory<ByteBuffer> factory = common.getFactory(codec.getBodyLength());
			final MessageBufferProvider<ByteBuffer> provider = common.getProvider(factory);
			final KeyProcessor<ByteBuffer> processor = new TCPProcessor(codec, limiter, provider,
					common.getSendBufferSize(), common.getReceiveBufferSize());
			final SelectorExecutor executor = common.getPool().next();
			final TCPChannel<ByteBuffer> channel = new TCPChannel<>(executor, processor);
			channel.open();
			return channel;
		}
	}

	/**
	 * @author Ricardo Padilha
	 */
	@ParametersAreNonnullByDefault
	public static final class SSLChannelBuilder {

		private final CommonBuilderData<ByteBuffer> common;
		private final ClientBuilderData client;
		private SSLContext context;

		SSLChannelBuilder() {
			this.common = new CommonBuilderData<>();
			this.client = new ClientBuilderData();
		}

		@Nonnull
		@Mandatory(restrictions = "pool != null")
		public SSLChannelBuilder setPool(final SelectorPool pool) {
			common.setPool(pool);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "256", restrictions = "capacity > 0")
		public SSLChannelBuilder setBufferCapacity(@Nonnegative final int capacity) {
			common.setBufferCapacity(capacity);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "0xFFFF", restrictions = "sendBufferSize > 0")
		public SSLChannelBuilder setSendBufferSize(@Nonnegative final int sendBufferSize) {
			common.setSendBufferSize(sendBufferSize);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "0xFFFF", restrictions = "receiveBufferSize > 0")
		public SSLChannelBuilder setReceiveBufferSize(@Nonnegative final int receiveBufferSize) {
			common.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useHeapBuffer()")
		public SSLChannelBuilder useDirectBuffer() {
			common.useDirectBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useDirectBuffer()")
		public SSLChannelBuilder useHeapBuffer() {
			common.useHeapBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useBlockingQueue()", restrictions = "requires disruptor library")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useBlockingQueue()")
		public SSLChannelBuilder useRingBuffer() {
			common.useRingBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useBlockingQueue()")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useRingBuffer()")
		public SSLChannelBuilder useBlockingQueue() {
			common.useBlockingQueue();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(consumer), useMultipleInputBuffers()")
		public SSLChannelBuilder useSingleInputBuffer() {
			common.useSingleInputBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useMultipleInputBuffers()", restrictions = "consumer != null")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useMultipleInputBuffers()")
		public SSLChannelBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			common.useSingleInputBuffer(consumer);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useSingleInputBuffer(consumer)")
		public SSLChannelBuilder useMultipleInputBuffers() {
			common.useMultipleInputBuffers();
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "codec != null")
		@OptionGroup(name = "codec", seeAlso = "setMessageLength(length)")
		public SSLChannelBuilder setMessageCodec(final MessageCodec codec) {
			client.setMessageCodec(codec);
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "length > 0")
		@OptionGroup(name = "codec", seeAlso = "setMessageCodec(codec)")
		public SSLChannelBuilder setMessageLength(@Nonnegative final int length) {
			client.setMessageLength(length);
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "limiter != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimit(value, unit)")
		public SSLChannelBuilder setRateLimiter(final RateLimiter limiter) {
			client.setRateLimiter(limiter);
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "value >= 1 && unit != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimiter(limiter)")
		public SSLChannelBuilder setRateLimit(@Nonnegative final long value, final BinaryUnit unit) {
			client.setRateLimit(value, unit);
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "context != null")
		public SSLChannelBuilder setContext(final SSLContext context) {
			if (context == null) {
				throw new NullPointerException("context == null");
			}
			this.context = context;
			return this;
		}

		@Nonnull
		public MessageChannel<ByteBuffer> open() throws IOException {
			final SSLEngine engine = context.createSSLEngine();
			engine.setUseClientMode(true);

			final MessageCodec codec = client.getMessageCodec();
			final RateLimiter limiter = client.getRateLimiter();
			final Factory<ByteBuffer> factory = common.getFactory(codec.getBodyLength());
			final MessageBufferProvider<ByteBuffer> provider = common.getProvider(factory);
			final KeyProcessor<ByteBuffer> processor = new SSLProcessor(codec, limiter, provider,
					common.getSendBufferSize(), common.getReceiveBufferSize(), engine);
			final SelectorExecutor executor = common.getPool().next();
			final TCPChannel<ByteBuffer> channel = new TCPChannel<>(executor, processor);
			channel.open();
			return channel;
		}
	}

	/**
	 * @author Ricardo Padilha
	 */
	@ParametersAreNonnullByDefault
	public static final class UDPChannelBuilder {

		private final CommonBuilderData<ByteBuffer> base;
		private final ClientBuilderData client;

		UDPChannelBuilder() {
			this.base = new CommonBuilderData<>();
			this.client = new ClientBuilderData();
		}

		@Nonnull
		@Mandatory(restrictions = "pool != null")
		public UDPChannelBuilder setPool(final SelectorPool pool) {
			base.setPool(pool);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "256", restrictions = "capacity > 0")
		public UDPChannelBuilder setBufferCapacity(@Nonnegative final int capacity) {
			base.setBufferCapacity(capacity);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "0xFFFF", restrictions = "sendBufferSize > 0")
		public UDPChannelBuilder setSendBufferSize(@Nonnegative final int sendBufferSize) {
			base.setSendBufferSize(sendBufferSize);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "0xFFFF", restrictions = "receiveBufferSize > 0")
		public UDPChannelBuilder setReceiveBufferSize(@Nonnegative final int receiveBufferSize) {
			base.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useHeapBuffer()")
		public UDPChannelBuilder useDirectBuffer() {
			base.useDirectBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useDirectBuffer()")
		public UDPChannelBuilder useHeapBuffer() {
			base.useHeapBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useBlockingQueue()", restrictions = "requires disruptor library")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useBlockingQueue()")
		public UDPChannelBuilder useRingBuffer() {
			base.useRingBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useBlockingQueue()")
		@OptionGroup(name = "bufferImplementation", seeAlso = "useRingBuffer()")
		public UDPChannelBuilder useBlockingQueue() {
			base.useBlockingQueue();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(consumer), useMultipleInputBuffers()")
		public UDPChannelBuilder useSingleInputBuffer() {
			base.useSingleInputBuffer();
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useMultipleInputBuffers()", restrictions = "consumer != null")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useMultipleInputBuffers()")
		public UDPChannelBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			base.useSingleInputBuffer(consumer);
			return this;
		}

		@Nonnull
		@Optional(defaultValue = "useMultipleInputBuffers()")
		@OptionGroup(name = "inputBuffer", seeAlso = "useSingleInputBuffer(), useSingleInputBuffer(consumer)")
		public UDPChannelBuilder useMultipleInputBuffers() {
			base.useMultipleInputBuffers();
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "codec != null")
		@OptionGroup(name = "codec", seeAlso = "setMessageLength(length)")
		public UDPChannelBuilder setMessageCodec(final MessageCodec codec) {
			client.setMessageCodec(codec);
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "length > 0")
		@OptionGroup(name = "codec", seeAlso = "setMessageCodec(codec)")
		public UDPChannelBuilder setMessageLength(@Nonnegative final int length) {
			client.setMessageLength(length);
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "limiter != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimit(value, unit)")
		public UDPChannelBuilder setRateLimiter(final RateLimiter limiter) {
			client.setRateLimiter(limiter);
			return this;
		}

		@Nonnull
		@Mandatory(restrictions = "value >= 1 && unit != null")
		@OptionGroup(name = "limiter", seeAlso = "setRateLimiter(limiter)")
		public UDPChannelBuilder setRateLimit(@Nonnegative final long value, final BinaryUnit unit) {
			client.setRateLimit(value, unit);
			return this;
		}

		@Nonnull
		public UDPChannel<ByteBuffer> open() throws IOException {
			final MessageCodec codec = client.getMessageCodec();
			final RateLimiter limiter = client.getRateLimiter();
			final Factory<ByteBuffer> factory = base.getFactory(codec.getBodyLength());
			final MessageBufferProvider<ByteBuffer> provider = base.getProvider(factory);
			final KeyProcessor<ByteBuffer> processor = new UDPProcessor(codec, limiter, provider);
			final SelectorPool pool = base.getPool();
			final UDPChannel<ByteBuffer> channel = new UDPChannel<>(pool, processor);
			channel.open();
			return channel;
		}
	}
}
