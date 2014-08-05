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

package net.dsys.snio.api.group;

import java.util.Iterator;

import javax.annotation.Nonnull;

/**
 * @author Ricardo Padilha
 */
public final class GroupDataIterator<T> implements Iterator<T> {

	private final GroupData<T> data;
	private final int k;
	private int i;

	public GroupDataIterator(@Nonnull final GroupData<T> data) {
		if (data == null) {
			throw new NullPointerException("data == null");
		}
		this.data = data;
		this.k = data.size() - 1;
		this.i = -1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNext() {
		return i < k;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	@Nonnull
	public T next() {
		return data.get(++i);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
