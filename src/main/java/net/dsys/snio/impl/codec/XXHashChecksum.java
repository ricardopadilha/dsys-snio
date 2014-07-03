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

package net.dsys.snio.impl.codec;

import java.util.zip.Checksum;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Simple wrapper for xxHash. Only implements {@link #update(byte[], int, int)},
 * and can only be updated once before a reset.
 * 
 * @author Ricardo Padilha
 */
final class XXHashChecksum implements Checksum {

	private final XXHash32 xxhash;
	private boolean done;
	private int value;

	XXHashChecksum() {
		this.xxhash = XXHashFactory.fastestInstance().hash32();
		this.done = false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void update(final int b) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void update(final byte[] b, final int off, final int len) {
		if (done) {
			throw new IllegalStateException("reset before updating again");
		}
		this.value = xxhash.hash(b, off, len, 0);
		this.done = true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getValue() {
		if (!done) {
			throw new IllegalStateException("update before getting value");
		}
		return value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reset() {
		done = false;
		value = 0;
	}
}
