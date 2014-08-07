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

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;

/**
 * @author Ricardo Padilha
 */
public final class GroupSocketAddress extends SocketAddress implements GroupData<SocketAddress> {

	private static final long serialVersionUID = 1L;

	@Nonnull
	private final List<SocketAddress> addresses;

	GroupSocketAddress(@Nonnull final List<SocketAddress> addresses) {
		if (addresses == null) {
			throw new NullPointerException("addresses == null");
		}
		if (addresses.isEmpty()) {
			throw new IllegalArgumentException("addresses.isEmpty()");
		}
		final int k = addresses.size();
		for (int i = 0; i < k; i++) {
			if (addresses.get(i) == null) {
				throw new NullPointerException("addresses.get(" + i + ") == null");
			}
		}
		this.addresses = new ArrayList<>(addresses);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {
		return addresses.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SocketAddress get(final int index) {
		return addresses.get(index);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterator<SocketAddress> iterator() {
		return new GroupDataIterator<>(this);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (obj instanceof GroupSocketAddress) {
			return addresses.equals(((GroupSocketAddress) obj).addresses);
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return addresses.hashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return addresses.toString();
	}

	@Nonnull
	public static Builder build() {
		return new Builder();
	}

	/**
	 * @author Ricardo Padilha
	 */
	public static final class Builder {

		private final List<SocketAddress> list;

		Builder() {
			this.list = new ArrayList<>();
		}

		@Nonnull
		public Builder add(@Nonnull final SocketAddress address) {
			if (address == null) {
				throw new NullPointerException("address == null");
			}
			list.add(address);
			return this;
		}

		@Nonnull
		public GroupSocketAddress build() {
			return new GroupSocketAddress(list);
		}

	}

}
