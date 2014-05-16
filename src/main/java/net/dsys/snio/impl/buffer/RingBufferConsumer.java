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

import net.dsys.snio.api.buffer.InterruptedByClose;
import net.dsys.snio.api.buffer.MessageBufferConsumer;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;

/**
 * @author Ricardo Padilha
 */
final class RingBufferConsumer<T> implements MessageBufferConsumer<T> {

	private final RingBuffer<T> buffer;
	private final Object[] attachments;
	private final SequenceBarrier barrier;
	private final Sequence sequence;
	private long cursor;
	private long available;
	private boolean closed;

	RingBufferConsumer(final RingBuffer<T> buffer, final Object[] attachments) {
		if (buffer == null) {
			throw new NullPointerException("buffer == null");
		}
		if (attachments == null) {
			throw new NullPointerException("attachments == null");
		}
		if (buffer.getBufferSize() != attachments.length) {
			throw new IllegalArgumentException("buffer.getBufferSize() != attachments.length");
		}
		this.buffer = buffer;
		this.attachments = attachments;
		this.barrier = buffer.newBarrier();
		this.sequence = new Sequence();
		buffer.addGatingSequences(sequence);
		this.cursor = sequence.get();
		this.available = sequence.get();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RingBufferProducer<T> createProducer() {
		return new RingBufferProducer<>(buffer, attachments);
	}

	void close() {
		closed = true;
		buffer.removeGatingSequence(sequence);
		barrier.alert();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long acquire() throws InterruptedException {
		if (closed) {
			throw new InterruptedByClose();
		}
		final long newCursor = cursor + 1;
		if (newCursor <= available) {
			return newCursor;
		}
		try {
			do {
				available = barrier.waitFor(newCursor);
			} while (available < newCursor);
		} catch (final AlertException e) {
			throw new InterruptedByClose();
		} catch (final TimeoutException e) {
			throw new InterruptedException(e.getLocalizedMessage());
		}
		return newCursor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long acquire(final int n) throws InterruptedException {
		if (closed) {
			throw new InterruptedByClose();
		}
		final long newCursor = cursor + n;
		if (newCursor <= available) {
			return newCursor;
		}
		try {
			final long minCursor = cursor + 1;
			do {
				available = barrier.waitFor(newCursor);
			} while (available < minCursor);
		} catch (final AlertException e) {
			throw new InterruptedByClose();
		} catch (final TimeoutException e) {
			throw new InterruptedException(e.getLocalizedMessage());
		}
		return Math.min(newCursor, available);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int remaining() {
		long c = buffer.getCursor();
		while (!buffer.isPublished(c)) {
			c--;
		}
		available = c;
		return (int) (available - cursor);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T get(final long sequence) {
		return buffer.get(sequence);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object attachment(final long sequence) {
		return attachments[(int) (sequence % attachments.length)];
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void release(final long seq) throws InterruptedException {
		if (closed) {
			throw new InterruptedByClose();
		}
		if (seq > available) {
			throw new IllegalArgumentException("seq > available");
		}
		cursor = seq;
		if (cursor == available) {
			sequence.set(cursor);
		}
	}
}
