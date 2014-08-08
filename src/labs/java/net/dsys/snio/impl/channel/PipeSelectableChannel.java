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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.Pipe;
import java.nio.channels.Pipe.SinkChannel;
import java.nio.channels.Pipe.SourceChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;

/**
 * This class encapsulates opposite endpoints of two different
 * {@link Pipe}s, to simulate a {@link SelectableChannel} without
 * network overhead.
 * 
 * @author Ricardo Padilha
 */
final class PipeSelectableChannel extends SelectableChannel implements ScatteringByteChannel, GatheringByteChannel {

	// These two belong to symmetrical pipes
	private final SinkChannel out;
	private final SourceChannel in;

	PipeSelectableChannel(final SinkChannel out, final SourceChannel in) {
		if (out == null) {
			throw new NullPointerException("out == null");
		}
		if (in == null) {
			throw new NullPointerException("in == null");
		}
		if ((in.validOps() & out.validOps()) != 0) {
			throw new IllegalArgumentException("sink and source have overlapping validOps");
		}
		this.out = out;
		this.in = in;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SelectorProvider provider() {
		return out.provider();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int validOps() {
		return in.validOps() | out.validOps();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isRegistered() {
		return in.isRegistered() || out.isRegistered();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SelectionKey keyFor(final Selector sel) {
		final SelectionKey kout = out.keyFor(sel);
		final SelectionKey kin = in.keyFor(sel);
		if (kout != null && kin != null) {
			throw new IllegalStateException("sink and source registered with the same selector");
		}
		if (kout != null) {
			return kout;
		}
		return kin;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SelectionKey register(final Selector sel, final int ops,
			final Object att) throws ClosedChannelException {
		if ((in.validOps() & ops) != 0) {
			if (out.keyFor(sel) != null) {
				throw new IllegalArgumentException("cannot register with selector - sink is already registered");
			}
			return in.register(sel, ops, att);
		}
		if ((out.validOps() & ops) != 0) {
			if (in.keyFor(sel) != null) {
				throw new IllegalArgumentException("cannot register with selector - source is already registered");
			}
			return out.register(sel, ops, att);
		}
		throw new IllegalArgumentException("unsupported ops: " + ops);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SelectableChannel configureBlocking(final boolean block) throws IOException {
		in.configureBlocking(block);
		out.configureBlocking(block);
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isBlocking() {
		return in.isBlocking() && out.isBlocking();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object blockingLock() {
		// XXX: what about in's blockingLock() ?
		return out.blockingLock();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void implCloseChannel() throws IOException {
		in.close();
		out.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long read(final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
		return in.read(dsts, offset, length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long read(final ByteBuffer[] dsts) throws IOException {
		return in.read(dsts);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(final ByteBuffer dst) throws IOException {
		return in.read(dst);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long write(final ByteBuffer[] srcs, final int offset, final int length) throws IOException {
		return out.write(srcs, offset, length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long write(final ByteBuffer[] srcs) throws IOException {
		return out.write(srcs);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int write(final ByteBuffer src) throws IOException {
		return out.write(src);
	}
}
