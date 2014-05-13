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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import net.dsys.commons.impl.future.SettableFuture;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.channel.MessageServerChannel;
import net.dsys.snio.api.pool.SelectorPool;
import net.dsys.snio.demo.SSLEchoClient;
import net.dsys.snio.impl.channel.MessageChannels;
import net.dsys.snio.impl.channel.MessageServerChannels;
import net.dsys.snio.impl.pool.SelectorPools;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class CloseTest {

	private static final int SEC = 1_000_000_000;
	private static final int CAPACITY = 1;
	private static final int LENGTH = 8;
	private static final int PORT = 65535;

	private AtomicInteger atomicPort = new AtomicInteger(PORT);
	private SelectorPool pool;
	private SSLContext context;

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

	@Test
	public void testClosePool() throws InterruptedException, ExecutionException, IOException {
		assertTrue(pool.isOpen());
		pool.close();
		pool.getCloseFuture().get();
		assertFalse(pool.isOpen());
	}

	private static void testClosePoolThenChannel(final SelectorPool pool, final MessageChannel<?> channel)
			throws Exception {
		assertTrue(pool.isOpen());
		assertTrue(channel.isOpen());
		pool.close();
		pool.getCloseFuture().get();
		assertFalse(pool.isOpen());
		assertTrue(channel.isOpen());
		channel.close();
		channel.getCloseFuture().get();
		assertFalse(pool.isOpen());
		assertFalse(channel.isOpen());
	}

	private static void testCloseChannelThenPool(final SelectorPool pool, final MessageChannel<?> channel)
			throws Exception {
		assertTrue(pool.isOpen());
		assertTrue(channel.isOpen());
		channel.close();
		channel.getCloseFuture().get();
		assertTrue(pool.isOpen());
		assertFalse(channel.isOpen());
		pool.close();
		pool.getCloseFuture().get();
		assertFalse(pool.isOpen());
		assertFalse(channel.isOpen());
	}

	@Test
	public void testClosePoolThenChannelTCP() throws Exception {
		final MessageChannel<?> channel = MessageChannels.newTCPChannel()
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		testClosePoolThenChannel(pool, channel);
	}

	@Test
	public void testCloseChannelThenPoolTCP() throws Exception {
		final MessageChannel<?> channel = MessageChannels.newTCPChannel()
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		testCloseChannelThenPool(pool, channel);
	}

	@Test
	public void testClosePoolThenChannelSSL() throws Exception {
		final MessageChannel<?> channel = MessageChannels.newSSLChannel()
				.setContext(context)
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		testClosePoolThenChannel(pool, channel);
	}

	@Test
	public void testCloseChannelThenPoolSSL() throws Exception {
		final MessageChannel<?> channel = MessageChannels.newSSLChannel()
				.setContext(context)
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		testCloseChannelThenPool(pool, channel);
	}

	private static void testClosePoolThenChannel(final SelectorPool pool, final MessageServerChannel<?> channel)
			throws Exception {
		assertTrue(pool.isOpen());
		assertTrue(channel.isOpen());
		pool.close();
		pool.getCloseFuture().get();
		assertFalse(pool.isOpen());
		assertTrue(channel.isOpen());
		channel.close();
		channel.getCloseFuture().get();
		assertFalse(pool.isOpen());
		assertFalse(channel.isOpen());
	}

	private static void testCloseChannelThenPool(final SelectorPool pool, final MessageServerChannel<?> channel)
			throws Exception {
		assertTrue(pool.isOpen());
		assertTrue(channel.isOpen());
		channel.close();
		channel.getCloseFuture().get();
		assertTrue(pool.isOpen());
		assertFalse(channel.isOpen());
		pool.close();
		pool.getCloseFuture().get();
		assertFalse(pool.isOpen());
		assertFalse(channel.isOpen());
	}

	@Test
	public void testClosePoolThenServerChannelTCP() throws Exception {
		final MessageServerChannel<?> channel = MessageServerChannels.newTCPServerChannel()
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		testClosePoolThenChannel(pool, channel);
	}

	@Test
	public void testCloseServerChannelThenPoolTCP() throws Exception {
		final MessageServerChannel<?> channel = MessageServerChannels.newTCPServerChannel()
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		testCloseChannelThenPool(pool, channel);
	}

	@Test
	public void testBindTCP() throws Exception {
		final int port = atomicPort.getAndDecrement();
		final InetSocketAddress local = new InetSocketAddress(port);

		final MessageServerChannel<?> channel = MessageServerChannels.newTCPServerChannel()
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		assertTrue(channel.isOpen());
		try {
			channel.bind(local);
			channel.getBindFuture().get();
		} catch (final BindException e) {
			fail("test failed: test port is already occupied -- make sure that no other process is using that port");
			return;
		} finally {
			assertTrue(channel.isOpen());
			channel.close();
			channel.getCloseFuture().get();
			assertFalse(channel.isOpen());
		}
	}

	@Test
	public void testBindSSL() throws Exception {
		final int port = atomicPort.getAndDecrement();
		final InetSocketAddress local = new InetSocketAddress(port);

		final MessageServerChannel<?> channel = MessageServerChannels.newSSLServerChannel()
				.setContext(context)
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		assertTrue(channel.isOpen());
		try {
			channel.bind(local);
			channel.getBindFuture().get();
		} catch (final BindException e) {
			fail("test failed: test port is already occupied -- make sure that no other process is using that port");
			return;
		} finally {
			assertTrue(channel.isOpen());
			channel.close();
			channel.getCloseFuture().get();
			assertFalse(channel.isOpen());
		}
	}

	@Test
	public void testFailedBindTCP() throws Exception {
		final int port = atomicPort.getAndDecrement();
		final InetSocketAddress local = new InetSocketAddress(port);

		final ServerSocketChannel socket = ServerSocketChannel.open();
		try {
			socket.bind(local);
		} catch (final BindException e) {
			fail("test failed: test port is already occupied -- make sure that no other process is using that port");
			socket.close();
			return;
		}

		final MessageChannel<?> channel = MessageChannels.newTCPChannel()
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		try {
			channel.bind(local);
			channel.getBindFuture().get();
		} catch (final BindException e) {
			assertNotNull(e);
			return;
		} finally {
			socket.close();
			channel.close();
			channel.getCloseFuture().get();
		}
		fail("should have got a BindException");
	}

	@Test
	public void testFailedBindSSL() throws Exception {
		final int port = atomicPort.getAndDecrement();
		final InetSocketAddress local = new InetSocketAddress(port);

		final ServerSocketChannel socket = ServerSocketChannel.open();
		try {
			socket.bind(local);
		} catch (final BindException e) {
			fail("test failed: test port is already occupied -- make sure that no other process is using that port");
			socket.close();
			return;
		}

		final MessageChannel<?> channel = MessageChannels.newSSLChannel()
				.setContext(context)
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		try {
			channel.bind(local);
			channel.getBindFuture().get();
		} catch (final BindException e) {
			assertNotNull(e);
			return;
		} finally {
			socket.close();
			channel.close();
			channel.getCloseFuture().get();
		}
		fail("should have got a BindException");
	}

	@Test
	public void testConnectionTCP() throws Exception {
		final InetAddress addr = InetAddress.getLocalHost();
		final int port = atomicPort.getAndDecrement();
		final InetSocketAddress local = new InetSocketAddress(port);
		final InetSocketAddress remote = new InetSocketAddress(addr, port);

		final ServerSocketChannel server = ServerSocketChannel.open();
		server.configureBlocking(true);
		try {
			server.bind(local);
		} catch (final BindException e) {
			fail("test failed: test port is already occupied -- make sure that no other process is using that port");
			server.close();
			return;
		}

		final MessageChannel<?> channel = MessageChannels.newTCPChannel()
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		assertTrue(channel.isOpen());
		channel.connect(remote);

		final SocketChannel endpoint = server.accept();
		assertNotNull(endpoint);

		channel.getConnectFuture().get();

		channel.close();
		channel.getCloseFuture().get();
		assertFalse(channel.isOpen());

		endpoint.close();
		server.close();
	}

	@Test
	public void testConnectionSSL() throws Exception {
		final InetAddress addr = InetAddress.getLocalHost();
		final int port = atomicPort.getAndDecrement();
		final InetSocketAddress local = new InetSocketAddress(port);
		final InetSocketAddress remote = new InetSocketAddress(addr, port);

		final MessageServerChannel<?> server = MessageServerChannels.newSSLServerChannel()
				.setContext(context)
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		try {
			server.bind(local);
			server.getBindFuture().get();
		} catch (final BindException e) {
			fail("test failed: test port is already occupied -- make sure that no other process is using that port");
			server.close();
			return;
		}

		final MessageChannel<?> client = MessageChannels.newSSLChannel()
				.setContext(context)
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		assertTrue(client.isOpen());
		client.connect(remote);
		client.getConnectFuture().get();

		//LockSupport.parkNanos(SEC);

		client.close();
		client.getCloseFuture().get();
		assertFalse(client.isOpen());

		server.close();
		server.getCloseFuture().get();
	}

	@Test
	public void testFailedConnectionTCP() throws Exception {
		final InetAddress addr = InetAddress.getLocalHost();
		final int port = atomicPort.getAndDecrement();
		final InetSocketAddress remote = new InetSocketAddress(addr, port);

		final MessageChannel<?> channel = MessageChannels.newTCPChannel()
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		assertTrue(channel.isOpen());
		channel.connect(remote);
		try {
			channel.getConnectFuture().get();
		} catch (final ExecutionException e) {
			assertTrue(e.getCause() instanceof ConnectException);
			return;
		} finally {
			channel.close();
			channel.getCloseFuture().get();
			assertFalse(channel.isOpen());
		}
		fail("should have got a ConnectException");
	}

	@Test
	public void testServerClosedConnectionTCP() throws Exception {
		final InetAddress addr = InetAddress.getLocalHost();
		final int port = atomicPort.getAndDecrement();
		final InetSocketAddress local = new InetSocketAddress(port);
		final InetSocketAddress remote = new InetSocketAddress(addr, port);

		final ServerSocketChannel server = ServerSocketChannel.open();
		server.configureBlocking(true);
		try {
			server.bind(local);
		} catch (final BindException e) {
			fail("test failed: test port is already occupied -- make sure that no other process is using that port");
			server.close();
			return;
		}

		final MessageChannel<?> channel = MessageChannels.newTCPChannel()
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		assertTrue(channel.isOpen());
		channel.connect(remote);

		final SocketChannel endpoint = server.accept();
		assertNotNull(endpoint);

		channel.getConnectFuture().get();

		endpoint.close();
		server.close();

		channel.close();
		channel.getCloseFuture().get();
		assertFalse(channel.isOpen());

	}

	@Test
	public void testClientInterruptWriteTCP() throws Exception {
		final InetAddress addr = InetAddress.getLocalHost();
		final int port = atomicPort.getAndDecrement();
		final InetSocketAddress local = new InetSocketAddress(port);
		final InetSocketAddress remote = new InetSocketAddress(addr, port);

		final ServerSocketChannel server = ServerSocketChannel.open();
		server.configureBlocking(true);
		try {
			server.bind(local);
		} catch (final BindException e) {
			fail("test failed: test port is already occupied -- make sure that no other process is using that port");
			server.close();
			return;
		}

		final MessageChannel<ByteBuffer> channel = MessageChannels.newTCPChannel()
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		assertTrue(channel.isOpen());
		channel.connect(remote);

		final SocketChannel endpoint = server.accept();
		assertNotNull(endpoint);

		channel.getConnectFuture().get();

		final CountDownLatch latch = new CountDownLatch(1);
		final SettableFuture<Void> future = new SettableFuture<>();
		final Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					final MessageBufferProducer<ByteBuffer> out = channel.getOutputBuffer();
					latch.await();
					out.acquire();
					future.fail(new AssertionError("able to acquire despite remote endpoint being closed"));
				} catch (Exception e) {
					future.success(null);
				}
			}
		});
		t.start();

		endpoint.close();
		server.close();
		// wait for a while so that the close propagates through the TCP stack
		LockSupport.parkNanos(SEC);

		// tells the other thread to write, and wait for outcome
		latch.countDown();
		try {
			future.get();
		} finally {
			channel.close();
			channel.getCloseFuture().get();
			assertFalse(channel.isOpen());
		}
	}

	@Test
	public void testClientInterruptReadTCP() throws Exception {
		final InetAddress addr = InetAddress.getLocalHost();
		final int port = atomicPort.getAndDecrement();
		final InetSocketAddress local = new InetSocketAddress(port);
		final InetSocketAddress remote = new InetSocketAddress(addr, port);

		final ServerSocketChannel server = ServerSocketChannel.open();
		server.configureBlocking(true);
		try {
			server.bind(local);
		} catch (final BindException e) {
			fail("test failed: test port is already occupied -- make sure that no other process is using that port");
			server.close();
			return;
		}

		final MessageChannel<ByteBuffer> channel = MessageChannels.newTCPChannel()
				.setPool(pool)
				.setBufferCapacity(CAPACITY)
				.setMessageLength(LENGTH)
				.open();
		assertTrue(channel.isOpen());
		channel.connect(remote);

		final SocketChannel endpoint = server.accept();
		assertNotNull(endpoint);

		channel.getConnectFuture().get();

		final SettableFuture<Void> future = new SettableFuture<>();
		final Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					final MessageBufferConsumer<ByteBuffer> in = channel.getInputBuffer();
					in.acquire();
					future.fail(new AssertionError("able to acquire despite remote endpoint being closed"));
				} catch (Exception e) {
					future.success(null);
				}
			}
		});
		t.start();

		endpoint.close();
		server.close();
		// wait for a while so that the close propagates through the TCP stack
		LockSupport.parkNanos(SEC);

		// tells the other thread to write, and wait for outcome
		future.get();

		channel.close();
		channel.getCloseFuture().get();
		assertFalse(channel.isOpen());
	}
}
