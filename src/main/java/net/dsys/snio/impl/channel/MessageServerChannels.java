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

import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.builder.Mandatory;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.channel.MessageServerChannel;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.pool.KeyAcceptor;

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
	public static final class TCPServerChannelBuilder extends
			AbstractBuilder<MessageServerChannel<ByteBuffer>, ByteBuffer> {

		TCPServerChannelBuilder() {
			super();
		}

		@Override
		public MessageServerChannel<ByteBuffer> open() throws IOException {
			final MessageCodec codec = getMessageCodec();
			final Factory<ByteBuffer> factory = getFactory(codec.getBodyLength());
			final Factory<MessageBufferProvider<ByteBuffer>> provider = getProviderFactory(factory);
			final KeyAcceptor<ByteBuffer> acceptor = new TCPAcceptor(getPool(), codec, provider, getSendBufferSize(),
					getReceiveBufferSize());
			final TCPServerChannel<ByteBuffer> channel = new TCPServerChannel<>(getPool(), acceptor);
			channel.open();
			return channel;
		}
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class SSLServerChannelBuilder extends
			AbstractBuilder<MessageServerChannel<ByteBuffer>, ByteBuffer> {

		private SSLContext context;

		SSLServerChannelBuilder() {
			super();
		}

		@Mandatory(restrictions = "context != null")
		public SSLServerChannelBuilder setContext(final SSLContext context) {
			if (context == null) {
				throw new NullPointerException("context == null");
			}
			this.context = context;
			return this;
		}

		@Override
		public MessageServerChannel<ByteBuffer> open() throws IOException {
			final MessageCodec codec = getMessageCodec();
			final Factory<ByteBuffer> factory = getFactory(codec.getBodyLength());
			final Factory<MessageBufferProvider<ByteBuffer>> provider = getProviderFactory(factory);
			final KeyAcceptor<ByteBuffer> acceptor = new SSLAcceptor(getPool(), codec, provider, getSendBufferSize(),
					getReceiveBufferSize(), context);
			final TCPServerChannel<ByteBuffer> channel = new TCPServerChannel<>(getPool(), acceptor);
			channel.open();
			return channel;
		}
	}
}
