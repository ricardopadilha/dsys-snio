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
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import net.dsys.commons.api.lang.BinaryUnit;
import net.dsys.commons.api.lang.Factory;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.channel.AcceptListener;
import net.dsys.snio.api.channel.CloseListener;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.limit.RateLimiter;
import net.dsys.snio.api.pool.KeyProcessor;
import net.dsys.snio.api.pool.SelectorExecutor;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.impl.channel.builder.ClientConfig;
import net.dsys.snio.impl.channel.builder.ChannelConfig;
import net.dsys.snio.impl.channel.builder.SSLConfig;

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
	static <T> CloseListener<T> dummyCloseListener() {
		return new CloseListener<T>() {
			@Override
			public void connectionClosed(final MessageChannel<T> channel) {
				return;
			}
		};
	}

	@Nonnull
	static <T> AcceptListener<T> dummyAcceptListener() {
		return new AcceptListener<T>() {
			@Override
			public void connectionAccepted(final SocketAddress remote, final MessageChannel<T> channel) {
				return;
			}
		};
	}

	@Nonnull
	public static MessageChannel<ByteBuffer> openTCPChannel(
			@Nonnull final ChannelConfig<ByteBuffer> common,
			@Nonnull final ClientConfig client) throws IOException {
		if (common == null) {
			throw new NullPointerException("common == null");
		}
		if (client == null) {
			throw new NullPointerException("client == null");
		}

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

	@Nonnull
	public static MessageChannel<ByteBuffer> openSSLChannel(
			@Nonnull final ChannelConfig<ByteBuffer> common,
			@Nonnull final ClientConfig client,
			@Nonnull final SSLConfig ssl) throws IOException {
		if (common == null) {
			throw new NullPointerException("common == null");
		}
		if (client == null) {
			throw new NullPointerException("client == null");
		}
		if (ssl == null) {
			throw new NullPointerException("ssl == null");
		}

		final SSLEngine engine = ssl.getContext().createSSLEngine();
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

	@Nonnull
	public static MessageChannel<ByteBuffer> openUDPChannel(
			@Nonnull final ChannelConfig<ByteBuffer> common,
			@Nonnull final ClientConfig client) throws IOException {
		if (common == null) {
			throw new NullPointerException("common == null");
		}
		if (client == null) {
			throw new NullPointerException("client == null");
		}

		final MessageCodec codec = client.getMessageCodec();
		final RateLimiter limiter = client.getRateLimiter();
		final Factory<ByteBuffer> factory = common.getFactory(codec.getBodyLength());
		final MessageBufferProvider<ByteBuffer> provider = common.getProvider(factory);
		final KeyProcessor<ByteBuffer> processor = new UDPProcessor(codec, limiter, provider);
		final SelectorPool pool = common.getPool();
		final UDPChannel<ByteBuffer> channel = new UDPChannel<>(pool, processor);
		channel.open();
		return channel;
	}

	public static TCPChannelBuilder newTCPChannel() {
		return new TCPChannelBuilder();
	}

	public static SSLChannelBuilder newSSLChannel() {
		return new SSLChannelBuilder();
	}

	public static UDPChannelBuilder newUDPChannel() {
		return new UDPChannelBuilder();
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class TCPChannelBuilder {

		private final ChannelConfig<ByteBuffer> common;
		private final ClientConfig client;

		TCPChannelBuilder() {
			this.common = new ChannelConfig<>();
			this.client = new ClientConfig();
		}

		/**
		 * @see ChannelConfig#setPool(SelectorPool)
		 */
		public TCPChannelBuilder setPool(final SelectorPool pool) {
			common.setPool(pool);
			return this;
		}

		/**
		 * @see ChannelConfig#setBufferCapacity(int)
		 */
		public TCPChannelBuilder setBufferCapacity(final int capacity) {
			common.setBufferCapacity(capacity);
			return this;
		}

		/**
		 * @see ChannelConfig#setSendBufferSize(int)
		 */
		public TCPChannelBuilder setSendBufferSize(final int sendBufferSize) {
			common.setSendBufferSize(sendBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#setReceiveBufferSize(int)
		 */
		public TCPChannelBuilder setReceiveBufferSize(final int receiveBufferSize) {
			common.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#useDirectBuffer()
		 */
		public TCPChannelBuilder useDirectBuffer() {
			common.useDirectBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useHeapBuffer()
		 */
		public TCPChannelBuilder useHeapBuffer() {
			common.useHeapBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useRingBuffer()
		 */
		public TCPChannelBuilder useRingBuffer() {
			common.useRingBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useBlockingQueue()
		 */
		public TCPChannelBuilder useBlockingQueue() {
			common.useBlockingQueue();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer()
		 */
		public TCPChannelBuilder useSingleInputBuffer() {
			common.useSingleInputBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer(net.dsys.snio.api.buffer.MessageBufferConsumer)
		 */
		public TCPChannelBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			common.useSingleInputBuffer(consumer);
			return this;
		}

		/**
		 * @see ChannelConfig#useMultipleInputBuffers()
		 */
		public TCPChannelBuilder useMultipleInputBuffers() {
			common.useMultipleInputBuffers();
			return this;
		}

		/**
		 * @see ClientConfig#setMessageCodec(MessageCodec)
		 */
		public TCPChannelBuilder setMessageCodec(final MessageCodec codec) {
			client.setMessageCodec(codec);
			return this;
		}

		/**
		 * @see ClientConfig#setMessageLength(int)
		 */
		public TCPChannelBuilder setMessageLength(final int length) {
			client.setMessageLength(length);
			return this;
		}

		/**
		 * @see ClientConfig#setRateLimiter(RateLimiter)
		 */
		public TCPChannelBuilder setRateLimiter(final RateLimiter limiter) {
			client.setRateLimiter(limiter);
			return this;
		}

		/**
		 * @see ClientConfig#setRateLimit(long, BinaryUnit)
		 */
		public TCPChannelBuilder setRateLimit(final long value, final BinaryUnit unit) {
			client.setRateLimit(value, unit);
			return this;
		}

		public MessageChannel<ByteBuffer> open() throws IOException {
			return openTCPChannel(common, client);
		}
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class SSLChannelBuilder {

		private final ChannelConfig<ByteBuffer> common;
		private final ClientConfig client;
		private final SSLConfig ssl;

		SSLChannelBuilder() {
			this.common = new ChannelConfig<>();
			this.client = new ClientConfig();
			this.ssl = new SSLConfig();
		}

		/**
		 * @see ChannelConfig#setPool(SelectorPool)
		 */
		public SSLChannelBuilder setPool(final SelectorPool pool) {
			common.setPool(pool);
			return this;
		}

		/**
		 * @see ChannelConfig#setBufferCapacity(int)
		 */
		public SSLChannelBuilder setBufferCapacity(final int capacity) {
			common.setBufferCapacity(capacity);
			return this;
		}

		/**
		 * @see ChannelConfig#setSendBufferSize(int)
		 */
		public SSLChannelBuilder setSendBufferSize(final int sendBufferSize) {
			common.setSendBufferSize(sendBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#setReceiveBufferSize(int)
		 */
		public SSLChannelBuilder setReceiveBufferSize(final int receiveBufferSize) {
			common.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#useDirectBuffer()
		 */
		public SSLChannelBuilder useDirectBuffer() {
			common.useDirectBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useHeapBuffer()
		 */
		public SSLChannelBuilder useHeapBuffer() {
			common.useHeapBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useRingBuffer()
		 */
		public SSLChannelBuilder useRingBuffer() {
			common.useRingBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useBlockingQueue()
		 */
		public SSLChannelBuilder useBlockingQueue() {
			common.useBlockingQueue();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer()
		 */
		public SSLChannelBuilder useSingleInputBuffer() {
			common.useSingleInputBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer(net.dsys.snio.api.buffer.MessageBufferConsumer)
		 */
		public SSLChannelBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			common.useSingleInputBuffer(consumer);
			return this;
		}

		/**
		 * @see ChannelConfig#useMultipleInputBuffers()
		 */
		public SSLChannelBuilder useMultipleInputBuffers() {
			common.useMultipleInputBuffers();
			return this;
		}

		/**
		 * @see ClientConfig#setMessageCodec(MessageCodec)
		 */
		public SSLChannelBuilder setMessageCodec(final MessageCodec codec) {
			client.setMessageCodec(codec);
			return this;
		}

		/**
		 * @see ClientConfig#setMessageLength(int)
		 */
		public SSLChannelBuilder setMessageLength(final int length) {
			client.setMessageLength(length);
			return this;
		}

		/**
		 * @see ClientConfig#setRateLimiter(RateLimiter)
		 */
		public SSLChannelBuilder setRateLimiter(final RateLimiter limiter) {
			client.setRateLimiter(limiter);
			return this;
		}

		/**
		 * @see ClientConfig#setRateLimit(long, BinaryUnit)
		 */
		public SSLChannelBuilder setRateLimit(final long value, final BinaryUnit unit) {
			client.setRateLimit(value, unit);
			return this;
		}

		/**
		 * @see SSLConfig#setContext(SSLContext)
		 */
		public SSLChannelBuilder setContext(final SSLContext context) {
			ssl.setContext(context);
			return this;
		}

		public MessageChannel<ByteBuffer> open() throws IOException {
			return openSSLChannel(common, client, ssl);
		}
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class UDPChannelBuilder {

		private final ChannelConfig<ByteBuffer> common;
		private final ClientConfig client;

		UDPChannelBuilder() {
			this.common = new ChannelConfig<>();
			this.client = new ClientConfig();
		}

		/**
		 * @see ChannelConfig#setPool(SelectorPool)
		 */
		public UDPChannelBuilder setPool(final SelectorPool pool) {
			common.setPool(pool);
			return this;
		}

		/**
		 * @see ChannelConfig#setBufferCapacity(int)
		 */
		public UDPChannelBuilder setBufferCapacity(final int capacity) {
			common.setBufferCapacity(capacity);
			return this;
		}

		/**
		 * @see ChannelConfig#setSendBufferSize(int)
		 */
		public UDPChannelBuilder setSendBufferSize(final int sendBufferSize) {
			common.setSendBufferSize(sendBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#setReceiveBufferSize(int)
		 */
		public UDPChannelBuilder setReceiveBufferSize(final int receiveBufferSize) {
			common.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#useDirectBuffer()
		 */
		public UDPChannelBuilder useDirectBuffer() {
			common.useDirectBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useHeapBuffer()
		 */
		public UDPChannelBuilder useHeapBuffer() {
			common.useHeapBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useRingBuffer()
		 */
		public UDPChannelBuilder useRingBuffer() {
			common.useRingBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useBlockingQueue()
		 */
		public UDPChannelBuilder useBlockingQueue() {
			common.useBlockingQueue();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer()
		 */
		public UDPChannelBuilder useSingleInputBuffer() {
			common.useSingleInputBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer(net.dsys.snio.api.buffer.MessageBufferConsumer)
		 */
		public UDPChannelBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			common.useSingleInputBuffer(consumer);
			return this;
		}

		/**
		 * @see ChannelConfig#useMultipleInputBuffers()
		 */
		public UDPChannelBuilder useMultipleInputBuffers() {
			common.useMultipleInputBuffers();
			return this;
		}

		/**
		 * @see ClientConfig#setMessageCodec(MessageCodec)
		 */
		public UDPChannelBuilder setMessageCodec(final MessageCodec codec) {
			client.setMessageCodec(codec);
			return this;
		}

		/**
		 * @see ClientConfig#setMessageLength(int)
		 */
		public UDPChannelBuilder setMessageLength(final int length) {
			client.setMessageLength(length);
			return this;
		}

		/**
		 * @see ClientConfig#setRateLimiter(RateLimiter)
		 */
		public UDPChannelBuilder setRateLimiter(final RateLimiter limiter) {
			client.setRateLimiter(limiter);
			return this;
		}

		/**
		 * @see ClientConfig#setRateLimit(long, BinaryUnit)
		 */
		public UDPChannelBuilder setRateLimit(final long value, final BinaryUnit unit) {
			client.setRateLimit(value, unit);
			return this;
		}

		public MessageChannel<ByteBuffer> open() throws IOException {
			return openUDPChannel(common, client);
		}
	}
}
