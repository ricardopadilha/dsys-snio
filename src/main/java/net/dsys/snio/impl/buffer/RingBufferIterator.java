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


import javax.annotation.Nonnull;
import javax.annotation.meta.When;

import net.dsys.snio.api.buffer.InterruptedByClose;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;

/**
 * An iterator for {@link RingBuffer}.
 * <br>
 * Typical usage:
 * <pre>
 * RingBuffer rb = ...
 * RingBufferIterator it = new RingBufferIterator(rb);
 * it.open();
 * while (true) {
 *   it.await();
 *   while (it.hasNext()) {
 *     Object o = it.get();
 *     ... processing ...
 *     it.advance();
 *   }
 * }
 * </pre>
 * 
 * Non-blocking usage:
 * <pre>
 * RingBuffer rb = ...
 * RingBufferIterator it = new RingBufferIterator(rb);
 * it.open();
 * while (true) {
 *   it.update();
 *   while (it.hasNext()) {
 *     Object o = it.get();
 *     ... processing ...
 *     it.advance();
 *   }
 * }
 * </pre>
 * 
 * @author Ricardo Padilha
 */
final class RingBufferIterator<E> {

	private final RingBuffer<E> buffer;
	private final Sequence[] leading;

	private SequenceBarrier barrier;
	private Sequence sequence;
	private long available;
	private long cursor;
	private volatile boolean closed;

	/**
	 * Creates an iterator that will follow the leading sequences.
	 * 
	 * @param buffer
	 * @param leading
	 */
	private RingBufferIterator(@Nonnull final RingBuffer<E> buffer,
			@Nonnull(when = When.MAYBE) final Sequence... leading) {
		if (buffer == null) {
			throw new NullPointerException("buffer == null");
		}
		this.buffer = buffer;
		this.leading = leading;
		this.closed = false;
	}

	/**
	 * Creates an iterator for a given RingBuffer.
	 * 
	 * @param buffer
	 */
	RingBufferIterator(@Nonnull final RingBuffer<E> buffer) {
		this(buffer, (Sequence[]) null);
	}

	/**
	 * Creates an iterator that will follow all the leading iterators.
	 * This iterator will only process events once all leading iterators advanced.
	 * 
	 * @param leaders
	 */
	@SafeVarargs
	RingBufferIterator(@Nonnull final RingBufferIterator<E>... leaders) {
		if (leaders == null) {
			throw new NullPointerException("leaders == null");
		}
		final int k = leaders.length;
		if (k == 0) {
			throw new IllegalArgumentException("leaders.length == 0");
		}
		this.leading = new Sequence[k];
		RingBuffer<E> buffer = null;
		for (int i = 0; i < k; i++) {
			final RingBufferIterator<E> it = leaders[i];
			if (it == null) {
				throw new NullPointerException("leaders[" + i + "] == null");
			}
			if (!it.isOpen()) {
				throw new IllegalArgumentException("leaders[" + i + "] is not open");
			}
			if (buffer == null) {
				buffer = leaders[i].buffer;
			} else if (!buffer.equals(leaders[i].buffer)) {
				throw new IllegalArgumentException("leaders[" + i + "] has a different RingBuffer");
			}
			leading[i] = it.sequence;
		}
		this.buffer = buffer;
	}

	/**
	 * An iterator does not affect the RingBuffer until it is opened.
	 */
	public void open() {
		this.barrier = buffer.newBarrier(leading);
		final long start = buffer.getCursor();
		this.sequence = new Sequence(start);
		this.available = start;
		this.cursor = start;
		buffer.addGatingSequences(sequence);
	}

	/**
	 * @return <code>true</code> if this iterator is opened.
	 */
	public boolean isOpen() {
		return closed;
	}

	/**
	 * Closes this iterator. Removes it from the RingBuffer.
	 */
	public void close() {
		buffer.removeGatingSequence(sequence);
		barrier.alert();
		closed = true;
	}

	/**
	 * Creates a new iterator that will follow the same barrier as this one.
	 * The new iterator will be able to process the same events as this one, in parallel.
	 */
	@Nonnull
	public RingBufferIterator<E> createPeer() {
		if (!isOpen()) {
			throw new IllegalStateException("iterator must be opened before creating a peer");
		}
		return new RingBufferIterator<>(buffer, leading);
	}

	/**
	 * Creates a new iterator that will follow this iterator.
	 * The new iterator will only be able to process new events once this one advances.
	 */
	@Nonnull
	public RingBufferIterator<E> createFollower() {
		if (!isOpen()) {
			throw new IllegalStateException("iterator must be opened before creating a follower");
		}
		return new RingBufferIterator<>(buffer, sequence);
	}

	/**
	 * Updates the available sequence by waiting on the barrier.
	 * 
	 * @throws InterruptedException
	 * @see SequenceBarrier#waitFor(long)
	 */
	public void await() throws InterruptedException {
		if (!isOpen()) {
			throw new InterruptedByClose();
		}
		try {
			available = barrier.waitFor(cursor);
		} catch (final AlertException e) {
			throw new InterruptedByClose();
		} catch (final TimeoutException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Non-blocking update of the available sequence on the barrier.
	 * 
	 * @see SequenceBarrier#getCursor()
	 */
	public void update() {
		long n = buffer.getCursor();
		while (!buffer.isPublished(n)) {
			n--;
		}
		available = n;
	}

	/**
	 * @return <code>true</code> if this iterator's sequence is lower than the
	 *         (cached) available
	 */
	public boolean hasNext() {
		return cursor <= available;
	}

	/**
	 * @return the element at this iterator's current sequence.
	 */
	@Nonnull
	public E get() {
		return buffer.get(cursor);
	}

	/**
	 * @return this iterator's sequence.
	 */
	public long cursor() {
		return cursor;
	}

	/**
	 * Increments this iterator's sequence.
	 */
	public void advance() {
		if (cursor == available) {
			sequence.set(cursor);
		}
		cursor++;
	}
}
