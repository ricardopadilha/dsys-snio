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

package net.dsys.snio.impl.group;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;

import net.dsys.commons.api.lang.Copier;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.group.GroupData;

/**
 * @author Ricardo Padilha
 */
final class GroupMessageBufferProducer<T> implements MessageBufferProducer<T> {

	private static final int INITIAL_SEQUENCE_VALUE = -1;

	private final Copier<T> copier;
	private final MessageBufferProducer<T>[] buffers;
	private long last;

	GroupMessageBufferProducer(@Nonnull final Copier<T> copier,
			@Nonnull final Collection<MessageBufferProducer<T>> buffers) {
		if (copier == null) {
			throw new NullPointerException("copier == null");
		}
		if (buffers == null) {
			throw new NullPointerException("buffers == null");
		}
		if (buffers.isEmpty()) {
			throw new IllegalArgumentException("buffers.isEmpty()");
		}
		this.copier = copier;
		@SuppressWarnings("unchecked")
		final MessageBufferProducer<T>[] bs = buffers.toArray(new MessageBufferProducer[0]);
		this.buffers = bs;
		this.last = INITIAL_SEQUENCE_VALUE;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long acquire() throws InterruptedException {
		long sequence = INITIAL_SEQUENCE_VALUE;
		for (final MessageBufferProducer<T> buffer : buffers) {
			final long seq = buffer.acquire();
			if (sequence == INITIAL_SEQUENCE_VALUE) {
				sequence = seq;
			} else if (sequence != seq) {
				throw new IllegalStateException(
						String.format("sequence numbers for MessageBuffers are not matching: %d != %d",
								Long.valueOf(seq), Long.valueOf(sequence)));
			}
		}
		return sequence;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long acquire(final int n) throws InterruptedException {
		long sequence = INITIAL_SEQUENCE_VALUE;
		for (final MessageBufferProducer<T> buffer : buffers) {
			final long seq = buffer.acquire();
			if (sequence == INITIAL_SEQUENCE_VALUE) {
				sequence = seq;
			} else {
				sequence = Math.min(sequence, seq);
			}
		}
		return sequence;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int remaining() {
		int rem = Integer.MAX_VALUE;
		for (final MessageBufferProducer<T> buffer : buffers) {
			rem = Math.min(rem, buffer.remaining());
		}
		return rem;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T get(final long sequence) {
		return buffers[0].get(sequence);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void attach(final long sequence, final Object attachment) {
		if (attachment instanceof GroupData) {
			final GroupData<?> data = (GroupData<?>) attachment;
			if (buffers.length != data.size()) {
				throw new IllegalArgumentException("buffers.length != data.size()");
			}
			final int k = buffers.length;
			for (int i = 0; i < k; i++) {
				buffers[i].attach(sequence, data.get(i));
			}
		} else {
			for (final MessageBufferProducer<T> buffer : buffers) {
				buffer.attach(sequence, attachment);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void release(final long sequence) throws InterruptedException {
		final int k = buffers.length;
		for (long s = last + 1; s <= sequence; s++) {
			final T in = buffers[0].get(s);
			for (int i = 1; i < k; i++) {
				final T out = buffers[i].get(s);
				copier.copy(in, out);
			}
		}
		for (final MessageBufferProducer<T> buffer : buffers) {
			buffer.release(sequence);
		}
		last = sequence;
	}

	@Nonnull
	static <T> Builder<T> build() {
		return new Builder<>();
	}

	/**
	 * @author Ricardo Padilha
	 */
	static final class Builder<T> {

		private final List<MessageBufferProducer<T>> list;
		private Copier<T> copier;

		Builder() {
			this.list = new ArrayList<>();
			this.copier = null;
		}

		public void setCopier(@Nonnull final Copier<T> copier) {
			if (copier == null) {
				throw new NullPointerException("copier == null");
			}
			this.copier = copier;
		}

		public Builder<T> add(@Nonnull final MessageBufferProducer<T> producer) {
			if (producer == null) {
				throw new NullPointerException("producer == null");
			}
			list.add(producer);
			return this;
		}

		@Nonnull
		public GroupMessageBufferProducer<T> build() {
			return new GroupMessageBufferProducer<>(copier, list);
		}
	}
}
