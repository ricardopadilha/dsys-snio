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
import net.dsys.snio.api.buffer.MessageBufferProducer;

import com.lmax.disruptor.RingBuffer;

/**
 * {@link MessageBuffer} implementation based on {@link RingBuffer}.
 * 
 * @author Ricardo Padilha
 */
final class RingBufferProducer<T> implements MessageBufferProducer<T> {

	private final RingBuffer<T> buffer;
	private final Object[] attachments;
	private boolean closed;

	public RingBufferProducer(final RingBuffer<T> buffer, final Object[] attachments) {
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
	}

	void close() {
		closed = true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long acquire() throws InterruptedException {
		if (closed) {
			throw new InterruptedByClose();
		}
		return buffer.next();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long acquire(final int n) throws InterruptedException {
		if (closed) {
			throw new InterruptedByClose();
		}
		return buffer.next(n);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int remaining() {
		// XXX: funky code, never tested
		return (int) (buffer.getMinimumGatingSequence() + buffer.getBufferSize() - buffer.getCursor());
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
	public void attach(final long sequence, final Object attachment) {
		attachments[(int) (sequence % attachments.length)] = attachment;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void release(final long sequence) throws InterruptedException {
		if (closed) {
			throw new InterruptedByClose();
		}
		buffer.publish(sequence);
	}
}