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

package net.dsys.snio.demo;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import net.dsys.snio.api.channel.MessageServerChannel;
import net.dsys.snio.api.handler.MessageHandler;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.impl.channel.MessageServerChannels;
import net.dsys.snio.impl.channel.builder.ChannelConfig;
import net.dsys.snio.impl.channel.builder.SSLConfig;
import net.dsys.snio.impl.channel.builder.ServerConfig;
import net.dsys.snio.impl.codec.Codecs;
import net.dsys.snio.impl.handler.MessageHandlers;
import net.dsys.snio.impl.pool.SelectorPools;

/**
 * Echo server using SSL.
 * From a performance PoV it is better to pass messages through
 * a compressor before encryption because compression uses less
 * CPU cycles per byte than encryption.
 * If heap buffers are used in the channel (the default), one less
 * memory copy is needed to encrypt.
 * 
 * @author Ricardo Padilha
 */
public final class SSLEchoServer {

	private SSLEchoServer() {
		return;
	}

	public static void main(final String[] args) throws Exception {
		final int threads = Integer.parseInt(getArg("threads", "1", args));
		final int length = Integer.parseInt(getArg("length", "1024", args));
		final int port = Integer.parseInt(getArg("port", "12345", args));

		final SelectorPool pool = SelectorPools.open("server", threads);

		final ChannelConfig<ByteBuffer> common = new ChannelConfig<ByteBuffer>()
				.setPool(pool)
				.useRingBuffer();
		final ServerConfig server = new ServerConfig()
				.setMessageCodec(Codecs.getLZ4Factory(length));
		final SSLConfig ssl = new SSLConfig()
				.setContext(getContext());
		final MessageServerChannel<ByteBuffer> channel =
				MessageServerChannels.openSSLServerChannel(common, server, ssl);

		// one thread per client
		final MessageHandler<ByteBuffer> handler = MessageHandlers.buildHandler()
				.useDecoupledProcessing(length)
				.useManyConsumers(EchoServer.createFactory())
				.useHeapBuffer()
				.build();

		channel.onAccept(handler.getAcceptListener());
		channel.bind(new InetSocketAddress(port));
		channel.getBindFuture().get();

		pool.getCloseFuture().get();
	}

	private static SSLContext getContext() throws Exception {
		final char[] password = "password".toCharArray();

		// First initialize the key and trust material.
		final KeyStore ksKeys = KeyStore.getInstance("JKS");
		try (final InputStream in = SSLEchoClient.class.getResourceAsStream("nodes.jks")) {
			ksKeys.load(in, password);
		}

		final KeyStore ksTrust = KeyStore.getInstance("JKS");
		try (final InputStream in = SSLEchoClient.class.getResourceAsStream("nodes.jks")) {
			ksTrust.load(in, password);
		}

		// KeyManager's decide which key material to use.
		final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
		kmf.init(ksKeys, password);

		// TrustManager's decide whether to allow connections.
		final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
		tmf.init(ksTrust);

		final SSLContext context = SSLContext.getInstance("TLS");
		context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
		return context;
	}

	private static String getArg(final String name, final String defaultValue, final String[] args) {
		if (args == null || name == null) {
			return defaultValue;
		}
		final String key = "--" + name;
		final int k = args.length - 1;
		for (int i = 0; i < k; i++) {
			if (key.equals(args[i])) {
				return args[i + 1];
			}
		}
		return defaultValue;
	}

}
