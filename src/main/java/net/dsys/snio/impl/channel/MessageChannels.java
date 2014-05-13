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
import javax.net.ssl.SSLEngine;

import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.builder.Mandatory;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.pool.KeyProcessor;

/**
 * Helper class to create {@link MessageChannel} instances.
 * 
 * @author Ricardo Padilha
 */
public final class MessageChannels {

	private MessageChannels() {
		// no instantiation
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
	public static final class TCPChannelBuilder extends AbstractBuilder<MessageChannel<ByteBuffer>, ByteBuffer> {

		TCPChannelBuilder() {
			super();
		}

		@Override
		public MessageChannel<ByteBuffer> open() throws IOException {
			final MessageCodec codec = getMessageCodec();
			final Factory<ByteBuffer> factory = getFactory(codec.getBodyLength());
			final MessageBufferProvider<ByteBuffer> provider = getProvider(factory);
			final KeyProcessor<ByteBuffer> processor = new TCPProcessor(codec, provider, getSendBufferSize(),
					getReceiveBufferSize());
			final TCPChannel<ByteBuffer> channel = new TCPChannel<>(getPool().next(), processor, null, null);
			channel.open();
			return channel;
		}
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class SSLChannelBuilder extends AbstractBuilder<MessageChannel<ByteBuffer>, ByteBuffer> {

		private SSLContext context;

		SSLChannelBuilder() {
			super();
		}

		@Mandatory(restrictions = "context != null")
		public SSLChannelBuilder setContext(final SSLContext context) {
			if (context == null) {
				throw new NullPointerException("context == null");
			}
			this.context = context;
			return this;
		}

		@Override
		public MessageChannel<ByteBuffer> open() throws IOException {
			final SSLEngine engine = context.createSSLEngine();
			engine.setUseClientMode(true);

			final MessageCodec codec = getMessageCodec();
			final Factory<ByteBuffer> factory = getFactory(codec.getBodyLength());
			final MessageBufferProvider<ByteBuffer> provider = getProvider(factory);
			final KeyProcessor<ByteBuffer> processor = new SSLProcessor(codec, provider, getSendBufferSize(),
					getReceiveBufferSize(), engine);
			final TCPChannel<ByteBuffer> channel = new TCPChannel<>(getPool().next(), processor, null, null);
			channel.open();
			return channel;
		}
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class UDPChannelBuilder extends AbstractBuilder<MessageChannel<ByteBuffer>, ByteBuffer> {

		UDPChannelBuilder() {
			super();
		}

		@Override
		public UDPChannel<ByteBuffer> open() throws IOException {
			final MessageCodec codec = getMessageCodec();
			final Factory<ByteBuffer> factory = getFactory(codec.getBodyLength());
			final MessageBufferProvider<ByteBuffer> provider = getProvider(factory);
			final KeyProcessor<ByteBuffer> processor = new UDPProcessor(codec, provider);
			final UDPChannel<ByteBuffer> channel = new UDPChannel<>(getPool(), processor);
			channel.open();
			channel.register();
			return channel;
		}
	}
}
