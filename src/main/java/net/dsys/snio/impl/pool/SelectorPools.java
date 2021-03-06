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

package net.dsys.snio.impl.pool;

import java.io.IOException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.snio.api.pool.SelectorPolicy;
import net.dsys.snio.api.pool.SelectorPool;

/**
 * @author Ricardo Padilha
 */
public final class SelectorPools {

	private SelectorPools() {
		// no instantiation
		return;
	}

	@Nonnull
	public static SelectorPool open(@Nonnull final String name) throws IOException {
		final int size = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
		return open(name, size, new RoundRobinPolicy());
	}

	@Nonnull
	public static SelectorPool open(@Nonnull final String name, @Nonnegative final int size) throws IOException {
		return open(name, size, new RoundRobinPolicy());
	}

	@Nonnull
	public static SelectorPool open(@Nonnull final String name, @Nonnegative final int size,
			@Nonnull final SelectorPolicy policy) throws IOException {
		final SelectorPoolImpl pool = new SelectorPoolImpl(name, size, policy);
		pool.open();
		return pool;
	}

}
