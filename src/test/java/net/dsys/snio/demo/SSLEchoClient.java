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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.impl.channel.MessageChannels;
import net.dsys.snio.impl.codec.LZ4CompressionCodec;
import net.dsys.snio.impl.handler.MessageHandlers;
import net.dsys.snio.impl.pool.SelectorPools;

/**
 * Echo client using SSL.
 * From a performance PoV it is better to pass messages through
 * a compressor before encryption because compression uses less
 * CPU cycles per byte than encryption.
 * If heap buffers are used in the channel (the default), one less
 * memory copy is needed to encrypt.
 * 
 * @author Ricardo Padilha
 */
public final class SSLEchoClient {

	private SSLEchoClient() {
		return;
	}

	public static void main(final String[] args) throws Exception {
		final int threads = Integer.parseInt(getArg("threads", "1", args));
		final int length = Integer.parseInt(getArg("length", "1024", args));
		final String host = getArg("host", "localhost", args);
		final int port = Integer.parseInt(getArg("port", "12345", args));

		final SelectorPool pool = SelectorPools.open("client", threads);
		final MessageChannel<ByteBuffer> client = MessageChannels.newSSLChannel()
				.setContext(getContext())
				.setPool(pool)
				.setMessageCodec(new LZ4CompressionCodec(length))
				.useRingBuffer()
				.open();

		client.connect(new InetSocketAddress(host, port));
		client.getConnectFuture().get();

		final MessageBufferConsumer<ByteBuffer> in = client.getInputBuffer();
		final MessageBufferProducer<ByteBuffer> out = client.getOutputBuffer();

		final ExecutorService executor = Executors.newCachedThreadPool();
		executor.execute(MessageHandlers.syncConsumer(in, new EchoConsumer()));
		executor.execute(MessageHandlers.syncProducer(out, new EchoProducer()));

		pool.getCloseFuture().get();
		executor.shutdown();
	}

	private static SSLContext getContext() throws Exception {
		final char[] password = "password".toCharArray();

		InputStream in;
		// First initialize the key and trust material.
		final KeyStore ksKeys = KeyStore.getInstance("JKS");
		in = SSLEchoClient.class.getResourceAsStream("nodes.jks");
		ksKeys.load(in, password);
		in.close();

		final KeyStore ksTrust = KeyStore.getInstance("JKS");
		in = SSLEchoClient.class.getResourceAsStream("nodes.jks");
		ksTrust.load(in, password);
		in.close();

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
		for (int i = 0, k = args.length - 1; i < k; i++) {
			if (key.equals(args[i])) {
				return args[i + 1];
			}
		}
		return defaultValue;
	}

}
