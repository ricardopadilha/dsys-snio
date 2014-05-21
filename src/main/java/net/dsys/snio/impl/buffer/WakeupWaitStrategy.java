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

import net.dsys.commons.api.exception.Bug;
import net.dsys.snio.api.pool.KeyProcessor;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;

/**
 * Optimized implementation of {@link WaitStrategy}, which directly
 * wakes up a selector instead of waking up a thread.
 *  
 * @author Ricardo Padilha
 */
public final class WakeupWaitStrategy implements WaitStrategy {

	private KeyProcessor<?> processor;

	public WakeupWaitStrategy() {
		super();
	}

	/**
	 * Set the channel to wake up when {@link #signalAllWhenBlocking()} is called.
	 */
	public void setProcessor(final KeyProcessor<?> processor) {
		if (processor == null) {
			throw new NullPointerException("processor == null");
		}
		this.processor = processor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long waitFor(final long sequence, final Sequence cursor,
			final Sequence dependentSequence, final SequenceBarrier barrier)
			throws AlertException, InterruptedException, TimeoutException {
		return cursor.get();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void signalAllWhenBlocking() {
		if (processor == null) {
			throw new Bug("processor == null");
		}
		processor.wakeupWriter();
	}

}
