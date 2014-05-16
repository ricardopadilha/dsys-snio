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

package net.dsys.snio.test;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import net.dsys.commons.api.lang.Interruptible;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.channel.AcceptListener;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.channel.MessageServerChannel;
import net.dsys.snio.api.handler.MessageHandler;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.demo.EchoConsumer;
import net.dsys.snio.demo.EchoProducer;
import net.dsys.snio.demo.EchoServer;
import net.dsys.snio.demo.SSLEchoClient;
import net.dsys.snio.impl.channel.MessageChannels;
import net.dsys.snio.impl.channel.MessageServerChannels;
import net.dsys.snio.impl.channel.MessageChannels.TCPChannelBuilder;
import net.dsys.snio.impl.channel.MessageServerChannels.TCPServerChannelBuilder;
import net.dsys.snio.impl.handler.MessageHandlers;
import net.dsys.snio.impl.handler.MessageHandlers.HandlerBuilder;
import net.dsys.snio.impl.pool.SelectorPools;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class TransferTest {

	private static final int SEC = 1_000_000_000;
	private static final int CAPACITY = 256;
	private static final int LENGTH = 1024;
	private static final int PORT = 65535;

	private SelectorPool pool;
	private SSLContext context;
	private AtomicInteger atomicPort = new AtomicInteger(PORT);
	private SocketAddress local;
	private SocketAddress remote;

	public TransferTest() {
		super();
	}

	@Before
	public void setUp() throws Exception {
		pool = SelectorPools.open("test", 1);
		context = getContext();
	}

	@After
	public void tearDown() throws Exception {
		if (pool.isOpen()) {
			pool.close();
			pool.getCloseFuture().get();
		}
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

	private void testTransfer(final MessageServerChannel<ByteBuffer> server, final MessageHandler<ByteBuffer> handler,
			final MessageChannel<ByteBuffer> client) throws Exception {
		server.onAccept(handler.getAcceptListener());
		server.bind(local);
		server.getBindFuture().get();

		client.connect(remote);
		client.getConnectFuture().get();
		
		final MessageBufferConsumer<ByteBuffer> in = client.getInputBuffer();
		final Interruptible consumer = MessageHandlers.syncConsumer(in, new EchoConsumer(2 * CAPACITY));
		final Thread c = new Thread(consumer);
		c.start();

		final MessageBufferProducer<ByteBuffer> out = client.getOutputBuffer();
		final Interruptible producer = MessageHandlers.syncProducer(out, new EchoProducer(2 * CAPACITY));
		final Thread p = new Thread(producer);
		p.start();

		p.join();
		c.join();

		client.close();
		client.getCloseFuture().get();
		server.close();
		server.getCloseFuture().get();
		handler.close();
	}

	//@Test
	public void testTCP() throws Exception {
		for (int i = 0; i <= 0b0011111; i++) {
			System.out.println("Test combination: " + Integer.toBinaryString(i));
			final InetAddress addr = InetAddress.getLocalHost();
			final int port = atomicPort.getAndDecrement();
			local = new InetSocketAddress(port);
			remote = new InetSocketAddress(addr, port);

			final TCPServerChannelBuilder serverBuilder = MessageServerChannels.newTCPServerChannel();
			serverBuilder.setPool(pool).setMessageLength(LENGTH);
			final TCPChannelBuilder clientBuilder = MessageChannels.newTCPChannel();
			clientBuilder.setPool(pool).setMessageLength(LENGTH);
			final HandlerBuilder handlerBuilder = MessageHandlers.buildHandler();

			final CountDownLatch latch = new CountDownLatch(1);
			if ((i & 0b0000001) == 0b0000001) {
				serverBuilder.useDirectBuffer();
				clientBuilder.useDirectBuffer();
				handlerBuilder.useDirectBuffer();
			}
			if ((i & 0b0000010) == 0b0000010) {
				serverBuilder.useRingBuffer();
				clientBuilder.useRingBuffer();
			}
			if ((i & 0b0000100) == 0b0000100) {
				serverBuilder.useSingleInputBuffer();
				clientBuilder.useSingleInputBuffer();
				handlerBuilder.useSingleConsumer(new EchoServer());
			} else if ((i | ~0b0000100) == ~0b0000100) {
				handlerBuilder.useManyConsumers(EchoConsumer.createFactory());
			}
			if ((i & 0b0001000) == 0b0001000) {
				handlerBuilder.useDecoupledProcessing(LENGTH);
			}
			if ((i & 0b0010000) == 0b0010000) {
				handlerBuilder.setDelegate(new AcceptListener<ByteBuffer>() {
					@Override
					public void connectionAccepted(final SocketAddress remote, 
							final MessageChannel<ByteBuffer> channel) {
						latch.countDown();
					}
				});
			} else {
				latch.countDown();
			}

			final MessageServerChannel<ByteBuffer> server = serverBuilder.open();
			final MessageChannel<ByteBuffer> client = clientBuilder.open();
			final MessageHandler<ByteBuffer> handler = handlerBuilder.build();
			testTransfer(server, handler, client);
			assertEquals(0, latch.getCount());
		}
	}

}
