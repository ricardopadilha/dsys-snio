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

package net.dsys.snio.impl.handler;

import static net.dsys.snio.impl.handler.ExecutionType.DECOUPLED;
import static net.dsys.snio.impl.handler.ExecutionType.ZERO_COPY;

import java.util.concurrent.atomic.AtomicBoolean;

import net.dsys.commons.api.lang.Cleaner;
import net.dsys.commons.api.lang.Copier;
import net.dsys.commons.api.lang.Interruptible;
import net.dsys.snio.api.buffer.InterruptedByClose;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.handler.MessageProducer;

/**
 * This implementation copies the information produced locally before acquiring
 * a new position in the output buffer. This allows a measure of decoupling. The
 * down-side is that it requires an additional copy of the data.
 * 
 * @author Ricardo Padilha
 */
final class ProducerThread<T> implements Interruptible {

	private final ExecutionType type;
	private final MessageBufferProducer<T> out;
	private final MessageProducer<T> producer;
	private final T holder;
	private final Copier<T> copier;
	private final Cleaner<T> cleaner;
	private final AtomicBoolean interrupted;

	ProducerThread(final MessageBufferProducer<T> out, final MessageProducer<T> producer, final T holder,
			final Copier<T> copier, final Cleaner<T> cleaner) {
		if (out == null) {
			throw new NullPointerException("in == null");
		}
		if (producer == null) {
			throw new NullPointerException("producer == null");
		}
		if (holder == null) {
			throw new NullPointerException("holder == null");
		}
		if (copier == null) {
			throw new NullPointerException("copier == null");
		}
		if (cleaner == null) {
			throw new NullPointerException("cleaner == null");
		}
		this.type = DECOUPLED;
		this.out = out;
		this.producer = producer;
		this.holder = holder;
		this.copier = copier;
		this.cleaner = cleaner;
		this.interrupted = new AtomicBoolean();
	}

	ProducerThread(final MessageBufferProducer<T> out, final MessageProducer<T> producer) {
		if (out == null) {
			throw new NullPointerException("in == null");
		}
		if (producer == null) {
			throw new NullPointerException("producer == null");
		}
		this.type = ZERO_COPY;
		this.out = out;
		this.producer = producer;
		this.holder = null;
		this.copier = null;
		this.cleaner = null;
		this.interrupted = new AtomicBoolean();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void interrupt() {
		interrupted.lazySet(true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {
		switch (type) {
		case DECOUPLED: {
			runDecoupled();
			break;
		}
		case ZERO_COPY: {
			runZeroCopy();
			break;
		}
		default: {
			throw new AssertionError("Unsuppported ThreadType: " + type);
		}
	}
}

	private void runDecoupled() {
		final T holder = this.holder;
		Object attachment;
		while (!Thread.interrupted() && !interrupted.get()) {
			try {
				cleaner.clear(holder);
				attachment = producer.produce(holder);
				final long sequence = out.acquire();
				try {
					final T value = out.get(sequence);
					copier.copy(holder, value);
					out.attach(sequence, attachment);
				} finally {
					out.release(sequence);
				}
			} catch (final InterruptedByClose e) {
				return;
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			} catch (final Throwable t) {
				System.err.println("Uncaught MessageHandler exception: " + t.getLocalizedMessage());
				t.printStackTrace();
			}
		}
	}

	private void runZeroCopy() {
		while (!Thread.interrupted() && !interrupted.get()) {
			try {
				final long sequence = out.acquire();
				try {
					final T value = out.get(sequence);
					final Object attachment = producer.produce(value);
					out.attach(sequence, attachment);
				} finally {
					out.release(sequence);
				}
			} catch (final InterruptedByClose e) {
				return;
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			} catch (final Throwable t) {
				System.err.println("Uncaught MessageHandler exception: " + t.getLocalizedMessage());
				t.printStackTrace();
			}
		}
	}
}