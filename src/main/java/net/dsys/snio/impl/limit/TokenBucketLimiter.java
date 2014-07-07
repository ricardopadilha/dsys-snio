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

package net.dsys.snio.impl.limit;

import java.util.concurrent.TimeUnit;

import net.dsys.commons.api.lang.BinaryUnit;
import net.dsys.snio.api.limit.RateLimiter;

import org.isomorphism.util.TokenBucket;
import org.isomorphism.util.TokenBuckets;

/**
 * @author Ricardo Padilha
 */
final class TokenBucketLimiter implements RateLimiter {

	/**
	 * Refill rate constants.
	 * These translate to "refill each 100ms with a 10th of the refill rate." 
	 */
	private static final int REFILL_FACTOR = 10;
	private static final int REFILL_INTERVAL = 100;
	private static final TimeUnit REFILL_UNIT = TimeUnit.MILLISECONDS;
	private static final int BASE_INTERVAL = 1;
	private static final TimeUnit BASE_UNIT = TimeUnit.SECONDS;

	private TokenBucket send;
	private TokenBucket recv;

	TokenBucketLimiter(final long value, final BinaryUnit unit) {
		setRate(value, unit);
	}

	private static TokenBucket createBucket(final long bits) {
		// Instead of refilling once per second, we refill 10 times per second.
		// It makes for a smoother bandwidth curve under very low rates,
		// e.g., less than 100 kbps.
		final long bytes = bits / Byte.SIZE;
		final long refillRate = bytes / REFILL_FACTOR;
		// if the division caused an underflow, revert to base units.
		if (refillRate < 1) {
			return TokenBuckets.builder()
				.withCapacity(bytes)
				.withYieldingSleepStrategy()
				.withFixedIntervalRefillStrategy(bytes, BASE_INTERVAL, BASE_UNIT)
				.build();
		}
		return TokenBuckets.builder()
			.withCapacity(bytes)
			.withYieldingSleepStrategy()
			.withFixedIntervalRefillStrategy(refillRate, REFILL_INTERVAL, REFILL_UNIT)
			.build();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setRate(final long value, final BinaryUnit unit) {
		if (value < 1) {
			throw new IllegalArgumentException("value < 1");
		}
		final long bits = unit.toBits(value);
		this.send = createBucket(bits);
		this.recv = createBucket(bits);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void send(final long bytes) {
		if (bytes > 0) {
			send.consume(bytes);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void receive(final long bytes) {
		if (bytes > 0) {
			recv.consume(bytes);
		}
	}
}
