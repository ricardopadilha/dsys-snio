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

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;

import net.dsys.commons.api.lang.BinaryUnit;
import net.dsys.commons.api.lang.Factory;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.channel.MessageServerChannel;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.limit.RateLimiter;
import net.dsys.snio.api.pool.KeyAcceptor;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.impl.channel.builder.ChannelConfig;
import net.dsys.snio.impl.channel.builder.SSLConfig;
import net.dsys.snio.impl.channel.builder.ServerConfig;

/**
 * Helper class to create {@link MessageServerChannel} instances.
 * 
 * @author Ricardo Padilha
 */
public final class MessageServerChannels {

	private MessageServerChannels() {
		// no instantiation
	}

	@Nonnull
	public static MessageServerChannel<ByteBuffer> openTCPServerChannel(
			@Nonnull final ChannelConfig<ByteBuffer> common,
			@Nonnull final ServerConfig server) throws IOException {
		if (common == null) {
			throw new NullPointerException("common == null");
		}
		if (server == null) {
			throw new NullPointerException("server == null");
		}

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

	@Nonnull
	public static MessageServerChannel<ByteBuffer> openSSLServerChannel(
			@Nonnull final ChannelConfig<ByteBuffer> common,
			@Nonnull final ServerConfig server,
			@Nonnull final SSLConfig ssl) throws IOException {
		if (common == null) {
			throw new NullPointerException("common == null");
		}
		if (server == null) {
			throw new NullPointerException("server == null");
		}
		if (ssl == null) {
			throw new NullPointerException("ssl == null");
		}

		final Factory<MessageCodec> codecs = server.getMessageCodecs();
		final Factory<RateLimiter> limiters = server.getRateLimiters();
		final Factory<ByteBuffer> factory = common.getFactory(codecs.newInstance().getBodyLength());
		final Factory<MessageBufferProvider<ByteBuffer>> provider = common.getProviderFactory(factory);
		final SelectorPool pool = common.getPool();
		final KeyAcceptor<ByteBuffer> acceptor = new SSLAcceptor(pool, codecs, limiters, provider,
				common.getSendBufferSize(), common.getReceiveBufferSize(), ssl.getContext());
		final TCPServerChannel<ByteBuffer> channel = new TCPServerChannel<>(pool, acceptor);
		channel.open();
		return channel;
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

		private final ChannelConfig<ByteBuffer> common;
		private final ServerConfig server;

		TCPServerChannelBuilder() {
			this.common = new ChannelConfig<>();
			this.server = new ServerConfig();
		}

		/**
		 * @see ChannelConfig#setPool(SelectorPool)
		 */
		public TCPServerChannelBuilder setPool(final SelectorPool pool) {
			common.setPool(pool);
			return this;
		}

		/**
		 * @see ChannelConfig#setBufferCapacity(int)
		 */
		public TCPServerChannelBuilder setBufferCapacity(final int capacity) {
			common.setBufferCapacity(capacity);
			return this;
		}

		/**
		 * @see ChannelConfig#setSendBufferSize(int)
		 */
		public TCPServerChannelBuilder setSendBufferSize(final int sendBufferSize) {
			common.setSendBufferSize(sendBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#setReceiveBufferSize(int)
		 */
		public TCPServerChannelBuilder setReceiveBufferSize(final int receiveBufferSize) {
			common.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#useDirectBuffer()
		 */
		public TCPServerChannelBuilder useDirectBuffer() {
			common.useDirectBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useHeapBuffer()
		 */
		public TCPServerChannelBuilder useHeapBuffer() {
			common.useHeapBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useRingBuffer()
		 */
		public TCPServerChannelBuilder useRingBuffer() {
			common.useRingBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useBlockingQueue()
		 */
		public TCPServerChannelBuilder useBlockingQueue() {
			common.useBlockingQueue();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer()
		 */
		public TCPServerChannelBuilder useSingleInputBuffer() {
			common.useSingleInputBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer(net.dsys.snio.api.buffer.MessageBufferConsumer)
		 */
		public TCPServerChannelBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			common.useSingleInputBuffer(consumer);
			return this;
		}

		/**
		 * @see ChannelConfig#useMultipleInputBuffers()
		 */
		public TCPServerChannelBuilder useMultipleInputBuffers() {
			common.useMultipleInputBuffers();
			return this;
		}

		/**
		 * @see ServerConfig#setMessageCodec(Factory)
		 */
		public TCPServerChannelBuilder setMessageCodec(final Factory<MessageCodec> codecs) {
			server.setMessageCodec(codecs);
			return this;
		}

		/**
		 * @see ServerConfig#setMessageLength(int)
		 */
		public TCPServerChannelBuilder setMessageLength(final int length) {
			server.setMessageLength(length);
			return this;
		}

		/**
		 * @see ServerConfig#setRateLimiter(Factory)
		 */
		public TCPServerChannelBuilder setRateLimiter(final Factory<RateLimiter> limiters) {
			server.setRateLimiter(limiters);
			return this;
		}

		/**
		 * @see ServerConfig#setRateLimit(long, BinaryUnit)
		 */
		public TCPServerChannelBuilder setRateLimit(final long value, final BinaryUnit unit) {
			server.setRateLimit(value, unit);
			return this;
		}

		public MessageServerChannel<ByteBuffer> open() throws IOException {
			return openTCPServerChannel(common, server);
		}
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class SSLServerChannelBuilder {

		private final ChannelConfig<ByteBuffer> common;
		private final ServerConfig server;
		private final SSLConfig ssl;

		SSLServerChannelBuilder() {
			this.common = new ChannelConfig<>();
			this.server = new ServerConfig();
			this.ssl = new SSLConfig();
		}

		/**
		 * @see ChannelConfig#setPool(SelectorPool)
		 */
		public SSLServerChannelBuilder setPool(final SelectorPool pool) {
			common.setPool(pool);
			return this;
		}

		/**
		 * @see ChannelConfig#setBufferCapacity(int)
		 */
		public SSLServerChannelBuilder setBufferCapacity(final int capacity) {
			common.setBufferCapacity(capacity);
			return this;
		}

		/**
		 * @see ChannelConfig#setSendBufferSize(int)
		 */
		public SSLServerChannelBuilder setSendBufferSize(final int sendBufferSize) {
			common.setSendBufferSize(sendBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#setReceiveBufferSize(int)
		 */
		public SSLServerChannelBuilder setReceiveBufferSize(final int receiveBufferSize) {
			common.setReceiveBufferSize(receiveBufferSize);
			return this;
		}

		/**
		 * @see ChannelConfig#useDirectBuffer()
		 */
		public SSLServerChannelBuilder useDirectBuffer() {
			common.useDirectBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useHeapBuffer()
		 */
		public SSLServerChannelBuilder useHeapBuffer() {
			common.useHeapBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useRingBuffer()
		 */
		public SSLServerChannelBuilder useRingBuffer() {
			common.useRingBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useBlockingQueue()
		 */
		public SSLServerChannelBuilder useBlockingQueue() {
			common.useBlockingQueue();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer()
		 */
		public SSLServerChannelBuilder useSingleInputBuffer() {
			common.useSingleInputBuffer();
			return this;
		}

		/**
		 * @see ChannelConfig#useSingleInputBuffer(net.dsys.snio.api.buffer.MessageBufferConsumer)
		 */
		public SSLServerChannelBuilder useSingleInputBuffer(final MessageBufferConsumer<ByteBuffer> consumer) {
			common.useSingleInputBuffer(consumer);
			return this;
		}

		/**
		 * @see ChannelConfig#useMultipleInputBuffers()
		 */
		public SSLServerChannelBuilder useMultipleInputBuffers() {
			common.useMultipleInputBuffers();
			return this;
		}

		/**
		 * @see ServerConfig#setMessageCodec(Factory)
		 */
		public SSLServerChannelBuilder setMessageCodec(final Factory<MessageCodec> codecs) {
			server.setMessageCodec(codecs);
			return this;
		}

		/**
		 * @see ServerConfig#setMessageLength(int)
		 */
		public SSLServerChannelBuilder setMessageLength(final int length) {
			server.setMessageLength(length);
			return this;
		}

		/**
		 * @see ServerConfig#setRateLimiter(Factory)
		 */
		public SSLServerChannelBuilder setRateLimiter(final Factory<RateLimiter> limiters) {
			server.setRateLimiter(limiters);
			return this;
		}

		/**
		 * @see ServerConfig#setRateLimit(long, BinaryUnit)
		 */
		public SSLServerChannelBuilder setRateLimit(final long value, final BinaryUnit unit) {
			server.setRateLimit(value, unit);
			return this;
		}

		/**
		 * @see SSLConfig#setContext(SSLContext)
		 */
		public SSLServerChannelBuilder setContext(final SSLContext context) {
			ssl.setContext(context);
			return this;
		}

		public MessageServerChannel<ByteBuffer> open() throws IOException {
			return openSSLServerChannel(common, server, ssl);
		}
	}
}
