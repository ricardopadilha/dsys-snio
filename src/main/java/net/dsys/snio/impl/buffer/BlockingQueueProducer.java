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

import net.dsys.commons.api.lang.Factory;
import net.dsys.snio.api.buffer.InterruptedByClose;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.pool.KeyProcessor;
import net.dsys.snio.impl.buffer.BlockingBuffer.Tuple;

/**
 * @author Ricardo Padilha
 */
final class BlockingQueueProducer<T> implements MessageBufferProducer<T> {

	private final BlockingBuffer<T> buffer;
	private final Factory<T> factory;
	private final Map<Long, Tuple<T>> temp;
	private KeyProcessor<T> processor;
	private long last;
	private long cursor;
	private boolean closed;

	BlockingQueueProducer(final BlockingBuffer<T> buffer, final Factory<T> factory) {
		if (buffer == null) {
			throw new NullPointerException("buffer == null");
		}
		if (factory == null) {
			throw new NullPointerException("factory == null");
		}
		this.buffer = buffer;
		this.factory = factory;
		this.temp = new HashMap<>();
		this.last = BlockingQueueProvider.INITIAL_SEQUENCE_VALUE;
		this.cursor = BlockingQueueProvider.INITIAL_SEQUENCE_VALUE;
	}

	public void setProcessor(final KeyProcessor<T> processor) {
		this.processor = processor;
		if (!buffer.isEmpty()) {
			processor.wakeupWriter();
		}
	}

	void close() {
		closed = true;
		buffer.interruptPut(new InterruptedByClose());
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
		if (n <= 0) {
			throw new IllegalArgumentException("n <= 0");
		}
		if (closed) {
			throw new InterruptedByClose();
		}
		int k = Math.min(n, buffer.capacity());
		while (--k >= 0) {
			final T value = factory.newInstance();
			final Tuple<T> tuple = new Tuple<>(value, null);
			final Long key = Long.valueOf(++cursor);
			temp.put(key, tuple);
		}
		return cursor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int remaining() {
		return buffer.remainingCapacity();
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
	public void attach(final long sequence, final Object attachment) {
		temp.get(Long.valueOf(sequence)).setAttachment(attachment);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void release(final long sequence) throws InterruptedException {
		if (closed) {
			throw new InterruptedByClose();
		}
		if (sequence > cursor) {
			throw new IllegalArgumentException("sequence > cursor");
		}
		final long k = Math.min(sequence, cursor);
		for (long i = last + 1; i <= k; i++) {
			final Long key = Long.valueOf(i);
			final Tuple<T> tuple = temp.remove(key);
			if (tuple != null) {
				buffer.put(tuple);
			}
		}
		last = k;
		if (processor != null) {
			processor.wakeupWriter();
		}
	}
}