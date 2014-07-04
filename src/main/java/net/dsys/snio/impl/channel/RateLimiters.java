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

package net.dsys.snio.impl.channel;

import net.dsys.commons.api.lang.BinaryUnit;
import net.dsys.commons.api.lang.Factory;
import net.dsys.snio.api.channel.RateLimiter;

/**
 * @author Ricardo Padilha
 */
public final class RateLimiters {

	/**
	 * Rate limiter that does not limit anything.
	 */
	private static final RateLimiter NO_LIMIT = new NullLimiter();

	private RateLimiters() {
		// no instantiation
	}

	public static RateLimiter noRateLimit() {
		return NO_LIMIT;
	}

	public static Factory<RateLimiter> noLimitFactory() {
		return new Factory<RateLimiter>() {
			@Override
			@SuppressWarnings("synthetic-access")
			public RateLimiter newInstance() {
				return NO_LIMIT;
			}
		};
	}

	public static RateLimiter limit(final long value, final BinaryUnit unit) {
		return new TokenBucketLimiter(value, unit);
	}

	public static Factory<RateLimiter> limitFactory(final long value, final BinaryUnit unit) {
		return new Factory<RateLimiter>() {
			@Override
			public RateLimiter newInstance() {
				return new TokenBucketLimiter(value, unit);
			}
		};
	}
}
