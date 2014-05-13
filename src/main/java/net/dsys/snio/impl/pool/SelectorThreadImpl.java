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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import net.dsys.commons.impl.future.SettableFuture;
import net.dsys.snio.api.pool.Acceptor;
import net.dsys.snio.api.pool.KeyAcceptor;
import net.dsys.snio.api.pool.KeyProcessor;
import net.dsys.snio.api.pool.Processor;
import net.dsys.snio.api.pool.SelectionType;
import net.dsys.snio.api.pool.SelectorThread;

/**
 * @author Ricardo Padilha
 */
final class SelectorThreadImpl implements SelectorThread {

	private final SelectionType type;
	private final AtomicBoolean newOps;
	private final Queue<IOOperation> ops;
	private final AtomicBoolean newKeys;
	private final NavigableSet<SelectionKey> keys;
	private final SettableFuture<Void> closeFuture;
	private Selector selector;
	private Loop loop;

	SelectorThreadImpl(final SelectionType type) {
		if (type == null) {
			throw new NullPointerException("type == null");
		}
		if (type != SelectionType.OP_READ && type != SelectionType.OP_WRITE && type != SelectionType.OP_ACCEPT) {
			throw new IllegalArgumentException("invalid type");
		}
		this.type = type;
		this.newOps = new AtomicBoolean();
		this.ops = new ConcurrentLinkedQueue<>();
		this.newKeys = new AtomicBoolean();
		this.keys = new ConcurrentSkipListSet<>(new KeyComparator());
		this.closeFuture = new SettableFuture<>();
	}

	private void queueOp(final IOOperation op) {
		ops.offer(op);
		if (newOps.compareAndSet(false, true)) {
			selector.wakeup();
		}
	}

	void open() throws IOException {
		if (selector != null) {
			return;
		}
		selector = Selector.open();
	}

	boolean isOpen() {
		return selector != null && selector.isOpen();
	}

	Future<Void> close() {
		final IOOperation close = new IOOperation() {
			@Override
			public void run() throws IOException {
				doClose();
			}
		};
		queueOp(close);
		return closeFuture;
	}

	/**
	 * Only called from within an {@link IOOperation} submitted by {@link #close()}.
	 * 
	 * @throws IOException
	 */
	void doClose() {
		if (selector == null) {
			return;
		}
		IOException ioex = null;
		for (final SelectionKey key : selector.keys()) {
			if (!key.isValid()) {
				continue;
			}
			try {
				doCloseAttachment(key);
			} catch (final IOException e) {
				if (ioex == null) {
					ioex = new IOException();
				}
				ioex.addSuppressed(e);
			}
		}
		try {
			selector.close();
		} catch (final IOException e) {
			if (ioex == null) {
				ioex = new IOException();
			}
			ioex.addSuppressed(e);
		}
		if (ioex == null) {
			closeFuture.success(null);
		} else {
			closeFuture.fail(ioex);
		}
	}

	static void doCloseAttachment(final SelectionKey key) throws IOException {
		final Object attach = key.attachment();
		if (attach instanceof Processor) {
			((Processor) attach).close();
		} else if (attach instanceof Acceptor) {
			((Acceptor) attach).close();
		} else {
			throw new AssertionError("Unknown attachment type: " + attach);
		}
	}

	SettableFuture<Void> getCloseFuture() {
		return closeFuture;
	}

	Runnable getRunnable() {
		if (loop != null) {
			return loop;
		}
		switch (type) {
		case OP_ACCEPT:
			loop = new AcceptLoop(selector, newOps, ops);
			break;
		case OP_READ:
			loop = new ReadLoop(selector, newOps, ops);
			break;
		case OP_WRITE:
			loop = new WriteLoop(selector, newOps, ops, newKeys, keys, SelectionKey.OP_WRITE);
			break;
		default:
			throw new AssertionError("Unsupported selection type: " + type);
		}
		return loop;
	}

	<S extends SelectableChannel & NetworkChannel> void bind(final S channel, final Acceptor acceptor) {
		final IOOperation bind = new IOOperation() {
			@Override
			public void run() throws IOException {
				doBind(channel, acceptor);
			}
		};
		queueOp(bind);
	}

	/**
	 * Only called from within an IOOperation.
	 * 
	 * @throws ClosedChannelException
	 */
	<S extends SelectableChannel & NetworkChannel> void doBind(final S channel, final Acceptor acceptor)
			throws IOException {
		final SelectionKey key = channel.register(selector, SelectionKey.OP_ACCEPT, acceptor);
		acceptor.getAcceptor().registered(this, key);
	}

	<S extends SelectableChannel & NetworkChannel> void connect(final S channel, final Processor processor) {
		final IOOperation bind = new IOOperation() {
			@Override
			public void run() throws IOException {
				doConnect(channel, processor);
			}
		};
		queueOp(bind);
	}

	/**
	 * Only called from within an IOOperation.
	 * 
	 * @throws ClosedChannelException
	 */
	<S extends SelectableChannel & NetworkChannel> void doConnect(final S channel, final Processor processor)
			throws IOException {
		if ((channel.validOps() & SelectionKey.OP_CONNECT) != 0) {
			final SelectionKey key = channel.register(selector, SelectionKey.OP_CONNECT, processor);
			processor.getProcessor().registered(this, key, SelectionType.OP_CONNECT);
		}
	}

	<S extends SelectableChannel & NetworkChannel> void register(final S channel, final Processor processor) {
		final IOOperation register = new IOOperation() {
			@Override
			public void run() throws IOException {
				doRegister(channel, processor);
			}
		};
		queueOp(register);
	}

	/**
	 * Only called from within an IOOperation.
	 * 
	 * @throws ClosedChannelException
	 */
	<S extends SelectableChannel & NetworkChannel> void doRegister(final S channel, final Processor processor) {
		try {
			final SelectionKey key = channel.register(selector, type.getOp(), processor);
			processor.getProcessor().registered(this, key, type);
		} catch (final ClosedChannelException e) {
			// channel was already closed, notify the processor all the same;
			processor.getProcessor().registered(this, null, type);
		}
	}

	void cancel(final SelectionKey key, final SettableFuture<Void> future) {
		final IOOperation cancel = new IOOperation() {
			@Override
			public void run() {
				try {
					key.cancel();
					future.success(null);
				} catch (final Throwable t) {
					future.fail(t);
				}
			}
		};
		queueOp(cancel);
	}

	void cancel(final SelectionKey key, final SettableFuture<Void> future, final Callable<Void> task) {
		final IOOperation cancel = new IOOperation() {
			@Override
			public void run() {
				try {
					key.cancel();
					task.call();
					future.success(null);
				} catch (final Throwable t) {
					future.fail(t);
				}
			}
		};
		queueOp(cancel);
	}

	/**
	 * {@inheritDoc}
	 * @see net.dsys.snio.api.pool.SelectorThread#enableKey(java.nio.channels.SelectionKey)
	 */
	@Override
	public void enableKey(final SelectionKey key) {
		if (keys.add(key) && newKeys.compareAndSet(false, true)) {
			selector.wakeup();
		}
	}

	/**
	 * Base class for all threads.
	 * 
	 * @author Ricardo Padilha
	 */
	private abstract static class Loop implements Runnable {

		private final Selector selector;
		private final AtomicBoolean newOps;
		private final Queue<IOOperation> ops;

		public Loop(final Selector selector, final AtomicBoolean newOps, final Queue<IOOperation> ops) {
			if (selector == null) {
				throw new NullPointerException("selector == null");
			}
			if (newOps == null) {
				throw new NullPointerException("selector == null");
			}
			if (ops == null) {
				throw new NullPointerException("ops == null");
			}
			this.selector = selector;
			this.newOps = newOps;
			this.ops = ops;
		}

		@Override
		public void run() {
			while (selector.isOpen()) {
				try {
					final int n = selector.select();
					runOps();
					updateKeys();
					if (n == 0) {
						continue;
					}
					final Set<SelectionKey> ks = selector.selectedKeys();
					if (ks.isEmpty()) {
						continue;
					}
					for (final Iterator<SelectionKey> it = ks.iterator(); it.hasNext();) {
						final SelectionKey k = it.next();
						it.remove();
						runKey(k);
					}
				} catch (final ClosedSelectorException e) {
					// this is an expected exception when the channel is closed.
					break;
				} catch (final IOException e) {
					// wtf? log and continue
					e.printStackTrace();
					continue;
				}
			}
		}

		/**
		 * Subclasses can override as needed.
		 */
		protected void updateKeys() {
			return;
		}

		/**
		 * Process a single SelectionKey.
		 */
		protected abstract void runKey(SelectionKey k);

		private void runOps() throws IOException {
			if (newOps.compareAndSet(true, false)) {
				while (!ops.isEmpty()) {
					final IOOperation op = ops.poll();
					op.run();
				}
			}
		}

	}

	/**
	 * @author Ricardo Padilha
	 */
	private static final class AcceptLoop extends Loop {

		public AcceptLoop(final Selector selector, final AtomicBoolean newOps, final Queue<IOOperation> ops) {
			super(selector, newOps, ops);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		protected void runKey(final SelectionKey k) {
			try {
				if (k.isAcceptable()) {
					final Acceptor accp = (Acceptor) k.attachment();
					final KeyAcceptor<?> keyaccp = accp.getAcceptor();
					keyaccp.accept(k);
				}
			} catch (final CancelledKeyException e) {
				// another thread cancelled the key
				return;
			} catch (final IOException e) {
				// wtf?
				e.printStackTrace();
				return;
			}
		}
	}

	/**
	 * @author Ricardo Padilha
	 */
	private static final class ReadLoop extends Loop {

		public ReadLoop(final Selector selector, final AtomicBoolean newOps, final Queue<IOOperation> ops) {
			super(selector, newOps, ops);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		protected void runKey(final SelectionKey k) {
			try {
				if (k.isReadable()) {
					final Processor proc = (Processor) k.attachment();
					final KeyProcessor<?> keyproc = proc.getProcessor();
					try {
						if (keyproc.read(k) < 0) {
							proc.close();
						}
					} catch (final IOException e) {
						proc.close();
					} catch (final NotYetConnectedException e) {
						// wtf?
						e.printStackTrace();
						proc.close();
					}
				} else if (k.isConnectable()) {
					final Processor proc = (Processor) k.attachment();
					final KeyProcessor<?> processor = proc.getProcessor();
					processor.connect(k);
				}
			} catch (final CancelledKeyException e) {
				// another thread cancelled the key
				return;
			} catch (final IOException e) {
				// wtf?
				e.printStackTrace();
				return;
			}
		}

	}

	/**
	 * @author Ricardo Padilha
	 */
	private static final class WriteLoop extends Loop {

		private final AtomicBoolean newKeys;
		private final NavigableSet<SelectionKey> keys;
		private final int op;

		public WriteLoop(final Selector selector, final AtomicBoolean newOps, final Queue<IOOperation> ops,
				final AtomicBoolean newKeys, final NavigableSet<SelectionKey> keys, final int op) {
			super(selector, newOps, ops);
			if (newKeys == null) {
				throw new NullPointerException("newKeys == null");
			}
			if (keys == null) {
				throw new NullPointerException("newKeys == null");
			}
			if (op != SelectionKey.OP_WRITE) {
				throw new IllegalArgumentException("op != SelectionKey.OP_WRITE");
			}
			this.newKeys = newKeys;
			this.keys = keys;
			this.op = op;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		protected void runKey(final SelectionKey k) {
			try {
				if (k.isWritable()) {
					final Processor proc = (Processor) k.attachment();
					final KeyProcessor<?> keyproc = proc.getProcessor();
					try {
						if (keyproc.write(k) < 0) {
							proc.close();
						}
					} catch (final IOException e) {
						proc.close();
					} catch (final NotYetConnectedException e) {
						e.printStackTrace();
						proc.close();
					}
				}
			} catch (final CancelledKeyException e) {
				// another thread cancelled the key
				return;
			} catch (final IOException e) {
				// wtf?
				e.printStackTrace();
				return;
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		protected void updateKeys() {
			if (newKeys.compareAndSet(true, false)) {
				SelectionKey key = null;
				while ((key = keys.pollFirst()) != null) {
					try {
						final int iops = key.interestOps();
						if ((iops & op) == 0) {
							key.interestOps(iops | op);
						}
					} catch (final CancelledKeyException e) {
						// another thread cancelled the key
						continue;
					}
				}
			}
		}

	}

	/**
	 * Single command to be executed within the selector thread.
	 * 
	 * @author Ricardo Padilha
	 */
	private interface IOOperation {
		void run() throws IOException;
	}

	/**
	 * Comparator for SelectionKeys. Makes sure that identical keys returns
	 * zero, non-identical keys are sorted by hash code.
	 * 
	 * @author Ricardo Padilha
	 */
	private static final class KeyComparator implements Comparator<SelectionKey> {
		KeyComparator() {
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int compare(final SelectionKey o1, final SelectionKey o2) {
			if ((o1 == o2) || (o1 == null && o2 == null)) {
				return 0;
			}
			if (o1 == null) {
				return -1;
			}
			if (o2 == null) {
				return 1;
			}
			return Integer.signum(o2.hashCode() - o1.hashCode());
		}
	}

}
