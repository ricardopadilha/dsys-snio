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
import java.nio.channels.NetworkChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.dsys.commons.impl.future.MergingCallbackFuture;
import net.dsys.commons.impl.future.SettableCallbackFuture;
import net.dsys.commons.impl.lang.DaemonThreadFactory;
import net.dsys.snio.api.pool.Acceptor;
import net.dsys.snio.api.pool.Processor;
import net.dsys.snio.api.pool.SelectionType;
import net.dsys.snio.api.pool.SelectorExecutor;

/**
 * @author Ricardo Padilha
 */
final class SelectorExecutorImpl implements SelectorExecutor {

	private static final int THREAD_COUNT = 3;

	private final ExecutorService executor;
	private final SelectorThreadImpl accepter;
	private final SelectorThreadImpl reader;
	private final SelectorThreadImpl writer;
	private volatile boolean accepting;
	private MergingCallbackFuture<Void> closeFuture;

	SelectorExecutorImpl(final String name) {
		this.executor = Executors.newFixedThreadPool(THREAD_COUNT, new DaemonThreadFactory(name));
		this.accepter = new SelectorThreadImpl(SelectionType.OP_ACCEPT);
		this.reader = new SelectorThreadImpl(SelectionType.OP_READ);
		this.writer = new SelectorThreadImpl(SelectionType.OP_WRITE);
		this.accepting = false;
	}

	/**
	 * Open this executor.
	 */
	void open() throws IOException {
		accepter.open();
		reader.open();
		writer.open();
		executor.execute(reader.getRunnable());
		executor.execute(writer.getRunnable());
	}

	/**
	 * @return <code>true</code> if this executor is open.
	 */
	boolean isOpen() {
		return accepter.isOpen() && reader.isOpen() && writer.isOpen();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <S extends SelectableChannel & NetworkChannel> void bind(final S channel, final Acceptor acceptor) {
		if ((channel.validOps() & SelectionKey.OP_ACCEPT) == 0) {
			throw new IllegalArgumentException("channel does not support SelectionKey.OP_ACCEPT");
		}
		/** XXX: see #getCloseFuture(). */
		if (!accepting) {
			executor.execute(accepter.getRunnable());
			accepting = true;
		}
		accepter.bind(channel, acceptor);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <S extends SelectableChannel & NetworkChannel> void connect(final S channel, final Processor processor) {
		reader.connect(channel, processor);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <S extends SelectableChannel & NetworkChannel> void register(final S channel, final Processor processor) {
		reader.register(channel, processor);
		writer.register(channel, processor);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void cancelBind(final SelectionKey key, final SettableCallbackFuture<Void> future,
			final Callable<Void> task) {
		if (key != null) {
			accepter.cancel(key, future, task);
		} else {
			try {
				task.call();
				future.success(null);
			} catch (final Throwable t) {
				future.fail(t);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void cancelConnect(final SelectionKey readKey, final SettableCallbackFuture<Void> readFuture,
			final SelectionKey writeKey, final SettableCallbackFuture<Void> writeFuture) {
		if (readKey != null) {
			reader.cancel(readKey, readFuture);
		} else {
			readFuture.success(null);
		}
		if (writeKey != null) {
			writer.cancel(writeKey, writeFuture);
		} else {
			writeFuture.success(null);
		}
	}

	/**
	 * Close this executor.
	 */
	void close() {
		writer.close();
		reader.close();
		accepter.close();
		executor.shutdown();
	}

	MergingCallbackFuture<Void> getCloseFuture() {
		if (closeFuture == null) {
			final MergingCallbackFuture.Builder<Void> builder = MergingCallbackFuture.builder();
			builder.add(writer.getCloseFuture());
			builder.add(reader.getCloseFuture());
			/**
			 * XXX: there is a racing condition between this method and bind().
			 * 
			 * It shouldn't be a problem unless someone closes a channel at the
			 * same time they are trying to bind it.
			 * <p>
			 * What happens when someone gets the future and then the channel is
			 * bound?
			 * <p>
			 * How do we include the future from the accepter thread in the
			 * reference that was given out?
			 * <p>
			 * How do we make sure that the reference that was given out is
			 * properly set as done, if the accepter thread is never started?
			 */
			if (accepting) {
				builder.add(accepter.getCloseFuture());
			}
			closeFuture = builder.build();
		}
		return closeFuture;
	}
}
