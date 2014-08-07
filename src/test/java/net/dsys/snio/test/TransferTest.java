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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

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
import net.dsys.snio.impl.channel.MessageChannels;
import net.dsys.snio.impl.channel.MessageServerChannels;
import net.dsys.snio.impl.channel.builder.ChannelConfig;
import net.dsys.snio.impl.channel.builder.ClientConfig;
import net.dsys.snio.impl.channel.builder.ServerConfig;
import net.dsys.snio.impl.handler.MessageHandlers;
import net.dsys.snio.impl.handler.MessageHandlers.HandlerBuilder;
import net.dsys.snio.impl.pool.SelectorPools;

import org.junit.After;
import org.junit.Before;

public final class TransferTest {

	//private static final int SEC = 1_000_000_000;
	private static final int CAPACITY = 256;
	private static final int LENGTH = 1024;
	private static final int PORT = 65535;

	private SelectorPool pool;
	//private SSLContext context;
	private AtomicInteger atomicPort = new AtomicInteger(PORT);
	private SocketAddress local;
	private SocketAddress remote;

	public TransferTest() {
		super();
	}

	@Before
	public void setUp() throws Exception {
		pool = SelectorPools.open("test", 1);
		//context = getContext();
	}

	@After
	public void tearDown() throws Exception {
		if (pool.isOpen()) {
			pool.close();
			pool.getCloseFuture().get();
		}
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
		final ChannelConfig<ByteBuffer> common = new ChannelConfig<ByteBuffer>().setPool(pool);
		final ServerConfig server = new ServerConfig().setMessageLength(LENGTH);
		final ClientConfig client = new ClientConfig().setMessageLength(LENGTH);

		for (int i = 0; i <= 0b0011111; i++) {
			System.out.println("Test combination: " + Integer.toBinaryString(i));
			final InetAddress addr = InetAddress.getLocalHost();
			final int port = atomicPort.getAndDecrement();
			local = new InetSocketAddress(port);
			remote = new InetSocketAddress(addr, port);

			final HandlerBuilder handler = MessageHandlers.buildHandler();

			final CountDownLatch latch = new CountDownLatch(1);
			if ((i & 0b0000001) == 0b0000001) {
				common.useDirectBuffer();
				handler.useDirectBuffer();
			}
			if ((i & 0b0000010) == 0b0000010) {
				common.useRingBuffer();
			}
			if ((i & 0b0000100) == 0b0000100) {
				common.useSingleInputBuffer();
				handler.useSingleConsumer(new EchoServer());
			} else if ((i | ~0b0000100) == ~0b0000100) {
				handler.useManyConsumers(EchoConsumer.createFactory());
			}
			if ((i & 0b0001000) == 0b0001000) {
				handler.useDecoupledProcessing(LENGTH);
			}
			if ((i & 0b0010000) == 0b0010000) {
				handler.setDelegate(new AcceptListener<ByteBuffer>() {
					@Override
					public void connectionAccepted(final SocketAddress remote, 
							final MessageChannel<ByteBuffer> channel) {
						latch.countDown();
					}
				});
			} else {
				latch.countDown();
			}

			final MessageServerChannel<ByteBuffer> serverChannel =
					MessageServerChannels.openTCPServerChannel(common, server);
			final MessageChannel<ByteBuffer> clientChannel =
					MessageChannels.openTCPChannel(common, client);
			final MessageHandler<ByteBuffer> messageHandler = handler.build();
			testTransfer(serverChannel, messageHandler, clientChannel);
			assertEquals(0, latch.getCount());
		}
	}

}
