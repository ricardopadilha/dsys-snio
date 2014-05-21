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
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

import net.dsys.commons.impl.future.CountDownFuture;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.impl.codec.Codecs;

import org.junit.Test;

/**
 * @author Ricardo Padilha
 */
public final class CodecTest {

	public CodecTest() {
		super();
	}

	static void edgeTest(final MessageCodec codec, final int min, final int max) {
		final ByteBuffer zero = ByteBuffer.allocate(min);
		assertFalse(codec.isValid(zero));
		final ByteBuffer full = ByteBuffer.allocate(max);
		assertFalse(codec.isValid(full));
	}

	static void testCodec(final MessageCodec codec, final int length, final Random rnd) throws Exception {
		final int len = 2 * length + codec.getFrameLength();
		final ByteBuffer heap = ByteBuffer.allocate(len);
		final ByteBuffer hin = ((ByteBuffer) heap.limit(length)).slice();
		final ByteBuffer hout = ((ByteBuffer) heap.position(length).limit(2 * length)).slice();
		final ByteBuffer htemp = ((ByteBuffer) heap.position(2 * length).limit(heap.capacity())).slice();
		while (hin.remaining() > 0) {
			hin.put((byte) rnd.nextInt());
		}
		hin.flip();
		codec.put(hin, htemp);
		hin.flip();
		htemp.flip();
		codec.get(htemp, hout);
		htemp.clear();
		hout.flip();
		assertEquals(hin, hout);

		final ByteBuffer direct = ByteBuffer.allocateDirect(len);
		final ByteBuffer din = ((ByteBuffer) direct.limit(length)).slice();
		final ByteBuffer dout = ((ByteBuffer) direct.position(length).limit(2 * length)).slice();
		final ByteBuffer dtemp = ((ByteBuffer) direct.position(2 * length).limit(direct.capacity())).slice();
		while (din.remaining() > 0) {
			din.put((byte) rnd.nextInt());
		}
		din.flip();
		codec.put(din, dtemp);
		din.flip();
		dtemp.flip();
		codec.get(dtemp, dout);
		dtemp.clear();
		dout.flip();
		assertEquals(din, dout);
	}

	private static void testCodec(final int maxLength, final CodecFactory factory) throws Exception {
		final int reps = 2000;
		final Random rnd = ThreadLocalRandom.current();
		final CountDownLatch latch = new CountDownLatch(reps);
		final CountDownFuture<Void> future = new CountDownFuture<>(latch, null);
		final Semaphore semaphore = new Semaphore(Runtime.getRuntime().availableProcessors());
		final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		final int k = reps / 2;
		for (int i = 1; i <= k; i++) {
			semaphore.acquire();
			final int length = i;
			final MessageCodec codec = factory.newInstance(length);
			executor.execute(getCodecRunnable(rnd, latch, future, semaphore, length, codec));
		}
		for (int i = maxLength - (reps / 2); i <= maxLength; i++) {
			semaphore.acquire();
			final int length = i;
			final MessageCodec codec = factory.newInstance(length);
			executor.execute(getCodecRunnable(rnd, latch, future, semaphore, length, codec));
		}
		try {
			future.get();
		} finally {
			executor.shutdown();
		}
	}

	private static Runnable getCodecRunnable(final Random rnd, final CountDownLatch latch,
			final CountDownFuture<Void> future, final Semaphore semaphore, final int length, final MessageCodec codec) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					if (!future.isDone()) {
						testCodec(codec, length, rnd);
						latch.countDown();
					}
				} catch (final Exception e) {
					future.fail(e);
				} finally {
					codec.close();
					semaphore.release();
				}
			}
		};
	}

	@Test
	@SuppressWarnings("static-method")
	public void testLengthHeader() throws Exception {
		final MessageCodec codec = Codecs.getDefault();
		edgeTest(codec, 0, codec.getBodyLength() + 1);
		testCodec(codec.getBodyLength(), new CodecFactory() {
			@Override
			public MessageCodec newInstance(final int length) {
				return Codecs.getDefault(length);
			}
		});
	}

	@Test
	@SuppressWarnings("static-method")
	public void testAdler32() throws Exception {
		final MessageCodec codec = Codecs.getAdler32Checksum();
		edgeTest(codec, 0, codec.getBodyLength() + 1);
		testCodec(codec.getBodyLength(), new CodecFactory() {
			@Override
			public MessageCodec newInstance(final int length) {
				return Codecs.getAdler32Checksum(length);
			}
		});
	}

	@Test
	@SuppressWarnings("static-method")
	public void testCRC32() throws Exception {
		final MessageCodec codec = Codecs.getCRC32Checksum();
		edgeTest(codec, 0, codec.getBodyLength() + 1);
		testCodec(codec.getBodyLength(), new CodecFactory() {
			@Override
			public MessageCodec newInstance(final int length) {
				return Codecs.getCRC32Checksum(length);
			}
		});
	}

	@Test
	@SuppressWarnings("static-method")
	public void testXXHash() throws Exception {
		final MessageCodec codec = Codecs.getXXHashChecksum();
		edgeTest(codec, 0, codec.getBodyLength() + 1);
		testCodec(codec.getBodyLength(), new CodecFactory() {
			@Override
			public MessageCodec newInstance(final int length) {
				return Codecs.getXXHashChecksum(length);
			}
		});
	}

	@Test
	@SuppressWarnings("static-method")
	public void testDeflate() throws Exception {
		final MessageCodec codec = Codecs.getDeflateCompression();
		edgeTest(codec, 0, codec.getBodyLength() + 1);
		testCodec(codec.getBodyLength(), new CodecFactory() {
			@Override
			public MessageCodec newInstance(final int length) {
				return Codecs.getDeflateCompression(length);
			}
		});
	}

	@Test
	@SuppressWarnings("static-method")
	public void testLZ4() throws Exception {
		final MessageCodec codec = Codecs.getLZ4Compression();
		edgeTest(codec, 0, codec.getBodyLength() + 1);
		testCodec(codec.getBodyLength(), new CodecFactory() {
			@Override
			public MessageCodec newInstance(final int length) {
				return Codecs.getLZ4Compression(length);
			}
		});
	}

	private interface CodecFactory {
		MessageCodec newInstance(int length);
	}

}
