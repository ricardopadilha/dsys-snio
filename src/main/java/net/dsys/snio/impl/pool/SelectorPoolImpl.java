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

import net.dsys.commons.api.future.CallbackFuture;
import net.dsys.commons.impl.future.MergingCallbackFuture;
import net.dsys.snio.api.pool.SelectorExecutor;
import net.dsys.snio.api.pool.SelectorPolicy;
import net.dsys.snio.api.pool.SelectorPool;

/**
 * @author Ricardo Padilha
 */
final class SelectorPoolImpl implements SelectorPool {

	private final SelectorPolicy policy;
	private final SelectorExecutorImpl[] selectors;
	private CallbackFuture<Void> closeFuture;

	SelectorPoolImpl(final String name, final int size, final SelectorPolicy policy) {
		if (size < 1) {
			throw new IllegalArgumentException("size < 1: " + size);
		}
		if (policy == null) {
			throw new NullPointerException("policy == null");
		}
		this.policy = policy;
		this.selectors = new SelectorExecutorImpl[size];
		for (int i = 0; i < size; i++) {
			selectors[i] = new SelectorExecutorImpl(name + "-" + i);
		}
	}

	void open() throws IOException {
		for (final SelectorExecutorImpl selector : selectors) {
			selector.open();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isOpen() {
		return selectors[0].isOpen();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {
		return selectors.length;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SelectorExecutorImpl get(final int index) {
		return selectors[index];
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SelectorExecutor next() {
		return policy.allocate(this);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		for (final SelectorExecutorImpl selector : selectors) {
			selector.close();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CallbackFuture<Void> getCloseFuture() {
		if (closeFuture == null) {
			final MergingCallbackFuture.Builder<Void> builder = MergingCallbackFuture.builder();
			for (final SelectorExecutorImpl selector : selectors) {
				builder.add(selector.getCloseFuture());
			}
			this.closeFuture = builder.build();
		}
		return closeFuture;
	}

}
