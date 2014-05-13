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

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.future.CountDownFuture;
import net.dsys.commons.impl.lang.ByteBufferFactory;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.impl.buffer.BlockingQueueProvider;
import net.dsys.snio.impl.buffer.RingBufferProvider;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Ricardo Padilha
 */
public final class ProviderTest {

	private static final int REPS = 100_000;

	private Factory<ByteBuffer> factory;
	private MessageBufferProvider<ByteBuffer> provider;
	private MessageBufferProducer<ByteBuffer> out;
	private MessageBufferConsumer<ByteBuffer> in;
	private ExecutorService executor;

	@Before
	public void setUp() throws Exception {
		factory = new ByteBufferFactory(Integer.SIZE / Byte.SIZE);
		executor = Executors.newFixedThreadPool(2);
	}

	@After
	public void tearDown() throws Exception {
		factory = null;
		provider = null;
		out = null;
		in = null;
		if (executor != null) {
			executor.shutdownNow();
		}
		executor = null;
	}

	private void test(final MessageBufferProducer<ByteBuffer> out, final MessageBufferConsumer<ByteBuffer> in)
			throws InterruptedException, ExecutionException {
		final CountDownLatch latch = new CountDownLatch(2);
		final CountDownFuture<Void> future = new CountDownFuture<>(latch, null);

		Runnable producer = new Runnable() {
			@Override
			public void run() {
				try {
					for (int i = 0; i < REPS; i++) {
						final long seq = out.acquire();
						try {
							ByteBuffer bb = out.get(seq);
							bb.clear();
							bb.putInt(i);
							bb.flip();
							out.attach(seq, Integer.valueOf(i));
						} finally {
							out.release(seq);
						}
					}
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (final Throwable t) {
					future.fail(t);
				} finally {
					latch.countDown();
				}
			}
		};

		Runnable consumer = new Runnable() {
			@Override
			public void run() {
				try {
					for (int i = 0; i < REPS; i++) {
						final long seq = in.acquire();
						try {
							ByteBuffer bb = in.get(seq);
							final int pos = bb.position();
							if (pos != 0) {
								System.out.println(i + " " + pos);
							}
							int n = bb.getInt();
							assertEquals(i, n);
							final Integer a = (Integer) in.attachment(seq);
							assertEquals(i, a.intValue());
						} finally {
							in.release(seq);
						}
					}
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (final Throwable t) {
					future.fail(t);
				} finally {
					latch.countDown();
				}
			}
		};

		executor.execute(consumer);
		executor.execute(producer);
		future.get();
	}

	@Test
	public void testRingBuffer() throws InterruptedException, ExecutionException {
		provider = RingBufferProvider.createFactory(1, factory).newInstance();
		out = provider.getChannelOut();
		in = provider.getAppIn();
		test(out, in);
	}

	@Test
	public void testBlockingQueue() throws InterruptedException, ExecutionException {
		provider = BlockingQueueProvider.createFactory(1, factory).newInstance();
		out = provider.getChannelOut();
		in = provider.getAppIn();
		test(out, in);
	}

}
