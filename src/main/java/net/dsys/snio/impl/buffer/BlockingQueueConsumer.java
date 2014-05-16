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

package net.dsys.snio.impl.buffer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import net.dsys.commons.api.lang.Factory;
import net.dsys.snio.api.buffer.InterruptedByClose;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.impl.buffer.BlockingBuffer.Tuple;

/**
 * @author Ricardo Padilha
 */
final class BlockingQueueConsumer<T> implements MessageBufferConsumer<T> {

	private final BlockingBuffer<T> buffer;
	private final Factory<T> factory;
	private final Map<Long, Tuple<T>> temp;
	private AtomicLong last;
	private AtomicLong cursor;
	private boolean closed;

	BlockingQueueConsumer(final BlockingBuffer<T> buffer, final Factory<T> factory) {
		if (buffer == null) {
			throw new NullPointerException("queue == null");
		}
		if (factory == null) {
			throw new NullPointerException("factory == null");
		}
		this.buffer = buffer;
		this.factory = factory;
		this.temp = new HashMap<>();
		this.last = new AtomicLong(BlockingQueueProvider.INITIAL_SEQUENCE_VALUE);
		this.cursor = new AtomicLong(BlockingQueueProvider.INITIAL_SEQUENCE_VALUE);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BlockingQueueProducer<T> createProducer() {
		return new BlockingQueueProducer<>(buffer, factory);
	}

	void close() {
		closed = true;
		buffer.interruptTake(new InterruptedByClose());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long acquire() throws InterruptedException {
		return acquire(1);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long acquire(final int n) throws InterruptedException {
		if (n < 1) {
			throw new IllegalArgumentException("n < 1");
		}
		if (closed) {
			throw new InterruptedByClose();
		}
		int k = Math.min(n, Math.max(buffer.size(), 1));
		long c = cursor.get();
		while (--k >= 0) {
			final Tuple<T> tuple = buffer.take();
			c = cursor.incrementAndGet();
			temp.put(Long.valueOf(c), tuple);
		}
		return c;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int remaining() {
		return temp.size() + buffer.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T get(final long sequence) {
		return temp.get(Long.valueOf(sequence)).getValue();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object attachment(final long sequence) {
		return temp.get(Long.valueOf(sequence)).getAttachment();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void release(final long sequence) throws InterruptedException {
		if (closed) {
			throw new InterruptedByClose();
		}
		final long c = cursor.get();
		final long l = last.get();
		if (sequence > c) {
			throw new IllegalArgumentException("sequence > cursor");
		}
		final long k = Math.min(sequence, c);
		for (long i = l + 1; i <= k; i++) {
			final Long key = Long.valueOf(i);
			temp.remove(key);
		}
		last.set(k);
	}
}
