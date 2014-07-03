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

import net.dsys.snio.api.channel.RateLimiter;
import net.dsys.snio.api.io.BinaryUnit;

/**
 * @author Ricardo Padilha
 */
final class NullLimiter implements RateLimiter {

	NullLimiter() {
		super();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setRate(final long value, final BinaryUnit unit) {
		return;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void send(final long bytes) {
		return;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void receive(final long bytes) {
		return;
	}
}
