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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Behaves like an ArrayBlockingQueue, but both {@link #put(Object)} and
 * {@link #take()} can be interrupted. Loosely follows the contract of
 * {@link BlockingQueue}.
 * 
 * @author Ricardo Padilha
 */
final class BlockingBuffer<T> {

	private final int mask;
	private final Tuple<T>[] values;
	private final Lock lock;
	private final Condition notEmpty;
	private final Condition notFull;

	private long putIndex;
	private long takeIndex;
	private int count;
	private InterruptedException interruptPut;
	private InterruptedException interruptTake;

	/**
	 * Creates a buffer with the given (fixed) capacity and default access
	 * policy.
	 * 
	 * @param capacity
	 *            the capacity of this queue
	 * @throws IllegalArgumentException
	 *             if {@code capacity < 1}
	 */
	BlockingBuffer(@Nonnegative final int capacity) {
		this(capacity, false);
	}

	/**
	 * Creates a buffer with the given (fixed) capacity and the specified access
	 * policy.
	 * 
	 * @param capacity
	 *            the capacity of this buffer
	 * @param fair
	 *            if {@code true} then queue accesses for threads blocked on
	 *            insertion or removal, are processed in FIFO order; if
	 *            {@code false} the access order is unspecified.
	 * @throws IllegalArgumentException
	 *             if {@code capacity < 1}
	 */
	BlockingBuffer(@Nonnegative final int capacity, final boolean fair) {
		if (capacity < 1) {
			throw new IllegalArgumentException("capacity < 1");
		}
		if (Integer.bitCount(capacity) > 1) {
			// not a power of two
			throw new IllegalArgumentException("capacity must be a power of two");
		}
		this.mask = capacity - 1;
		@SuppressWarnings("unchecked")
		final Tuple<T>[] values = new Tuple[capacity];
		this.values = values;
		this.lock = new ReentrantLock(fair);
		this.notEmpty = lock.newCondition();
		this.notFull = lock.newCondition();
	}

	/**
	 * @see BlockingQueue#put(Object)
	 */
	void put(@Nonnull final Tuple<T> value) throws InterruptedException {
		if (value == null || value.getValue() == null) {
			throw new NullPointerException("value == null");
		}
		lock.lockInterruptibly();
		try {
			while (count == values.length && interruptPut == null) {
				notFull.await();
			}
			if (interruptPut != null) {
				final InterruptedException ex = interruptPut;
				interruptPut = null;
				Thread.currentThread().interrupt();
				throw ex;
			}
			final int index = (int) (putIndex & mask);
			values[index] = value;
			++putIndex;
			++count;
			notEmpty.signal();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Interrupt threads blocked in {@link #put(Object)} or
	 * {@link #put(Object, long, TimeUnit)} and throws the given exception.
	 */
	void interruptPut(@Nonnull final InterruptedException e) {
		if (e == null) {
			throw new NullPointerException("e == null");
		}
		lock.lock();
		try {
			interruptPut = e;
			notFull.signalAll();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * @see BlockingQueue#take()
	 */
	@Nonnull
	Tuple<T> take() throws InterruptedException {
		lock.lockInterruptibly();
		try {
			while (count == 0 && interruptTake == null) {
				notEmpty.await();
			}
			if (interruptTake != null) {
				final InterruptedException ex = interruptTake;
				interruptTake = null;
				Thread.currentThread().interrupt();
				throw ex;
			}
			final int index = (int) (takeIndex & mask);
			final Tuple<T> value = values[index];
			values[index] = null;
			++takeIndex;
			--count;
			notFull.signal();
			return value;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Interrupt threads blocked in {@link #take()} or
	 * {@link #poll(long, TimeUnit)} and throws the given exception.
	 */
	void interruptTake(@Nonnull final InterruptedException e) {
		if (e == null) {
			throw new NullPointerException("e == null");
		}
		lock.lock();
		try {
			interruptTake = e;
			notEmpty.signalAll();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * @return the capacity of this buffer
	 */
	@Nonnegative
	int capacity() {
		return values.length;
	}

	/**
	 * @return the number of elements in this buffer
	 */
	@Nonnegative
	int size() {
		lock.lock();
		try {
			return count;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * @return {@link #size()} == 0
	 * @see java.util.Collection#isEmpty()
	 */
	boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * @see BlockingQueue#remainingCapacity()
	 */
	@Nonnegative
	int remainingCapacity() {
		lock.lock();
		try {
			return values.length - count;
		} finally {
			lock.unlock();
		}
	}

	static final class Tuple<T> {
		private final T value;
		private Object attachment;

		Tuple(@Nonnull final T value) {
			this.value = value;
		}

		Tuple(@Nonnull final T value, @Nonnull final Object attachment) {
			this.value = value;
			this.attachment = attachment;
		}

		@Nonnull T getValue() {
			return value;
		}

		@Nonnull Object getAttachment() {
			return attachment;
		}

		void setAttachment(@Nonnull final Object attachment) {
			this.attachment = attachment;
		}
	}
}
