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
import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.api.lang.Interruptible;
import net.dsys.snio.api.buffer.InterruptedByClose;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.handler.MessageConsumer;

/**
 * This implementation copies the information to be processed from the input
 * buffer locally and releases it before processing it. This allows a measure of
 * decoupling between input and output buffers. The down-side is that it requires
 * an additional copy of the data to be processed.
 * 
 * @author Ricardo Padilha
 */
final class ConsumerThread<T> implements Interruptible {

	private final ExecutionType type;
	private final MessageBufferConsumer<T> in;
	private final MessageConsumer<T> consumer;
	private final T holder;
	private final Copier<T> copier;
	private final Cleaner<T> cleaner;
	private final AtomicBoolean interrupted;

	ConsumerThread(final MessageBufferConsumer<T> in, final MessageConsumer<T> consumer, final T holder,
			final Copier<T> copier, final Cleaner<T> cleaner) {
		if (in == null) {
			throw new NullPointerException("in == null");
		}
		if (consumer == null) {
			throw new NullPointerException("consumer == null");
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
		this.in = in;
		this.consumer = consumer;
		this.holder = holder;
		this.copier = copier;
		this.cleaner = cleaner;
		this.interrupted = new AtomicBoolean();
	}

	ConsumerThread(final MessageBufferConsumer<T> in, final MessageConsumer<T> consumer) {
		if (in == null) {
			throw new NullPointerException("in == null");
		}
		if (consumer == null) {
			throw new NullPointerException("consumer == null");
		}
		this.type = ZERO_COPY;
		this.in = in;
		this.consumer = consumer;
		this.holder = null;
		this.copier = null;
		this.cleaner = null;
		this.interrupted = new AtomicBoolean();
	}

	static <T> ConsumerThreadFactory<T> createAsyncFactory(final Factory<T> factory, final Copier<T> copier,
			final Cleaner<T> cleaner) {
		return new ConsumerThreadFactory<T>() {
			@Override
			public Runnable newInstance(final MessageBufferConsumer<T> in, final MessageConsumer<T> consumer) {
				return new ConsumerThread<>(in, consumer, factory.newInstance(), copier, cleaner);
			}
		};
	}

	static <T> ConsumerThreadFactory<T> createSyncFactory() {
		return new ConsumerThreadFactory<T>() {
			@Override
			public Runnable newInstance(final MessageBufferConsumer<T> in, final MessageConsumer<T> consumer) {
				return new ConsumerThread<>(in, consumer);
			}
		};
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
				asyncRun();
				break;
			}
			case ZERO_COPY: {
				syncRun();
				break;
			}
			default: {
				throw new AssertionError("Unsuppported ThreadType: " + type);
			}
		}
	}

	private void asyncRun() {
		final T holder = this.holder;
		Object attachment;
		while (!Thread.interrupted() && !interrupted.get()) {
			try {
				final long sequence = in.acquire();
				try {
					final T value = in.get(sequence);
					copier.copy(value, holder);
					attachment = in.attachment(sequence);
				} finally {
					in.release(sequence);
				}
				consumer.consume(holder, attachment);
				cleaner.clear(holder);
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

	private void syncRun() {
		while (!Thread.interrupted() && !interrupted.get()) {
			try {
				final long sequence = in.acquire();
				try {
					final T value = in.get(sequence);
					final Object attachment = in.attachment(sequence);
					consumer.consume(value, attachment);
				} finally {
					in.release(sequence);
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
