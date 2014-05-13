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
import java.nio.channels.NoConnectionPendingException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;

import net.dsys.commons.impl.future.SettableFuture;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.codec.MessageCodec;

/**
 * @author Ricardo Padilha
 */
final class TCPProcessor extends AbstractProcessor<ByteBuffer> {

	private static final int NO_SEQUENCE = -1;

	private final MessageCodec codec;
	private final int sendSize;
	private final int receiveSize;
	private ByteBuffer receiveBuffer;
	private ByteBuffer sendBuffer;
	private long writeSequence;

	TCPProcessor(final MessageCodec codec, final MessageBufferProvider<ByteBuffer> provider, final int sendSize,
			final int receiveSize) {
		super(provider);
		if (codec == null) {
			throw new NullPointerException("codec == null");
		}
		if (sendSize < 1) {
			throw new IllegalArgumentException("sendSize < 1");
		}
		if (receiveSize < 1) {
			throw new IllegalArgumentException("receiveSize < 1");
		}
		this.codec = codec.clone();
		this.sendSize = sendSize;
		this.receiveSize = receiveSize;
		this.writeSequence = NO_SEQUENCE;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void connect(final SelectionKey key) {
		final SocketChannel client = (SocketChannel) key.channel();
		try {
			if (client.finishConnect()) {
				key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
				if (!(key.attachment() instanceof TCPChannel)) {
					throw new AssertionError("Unsupported key attachment: " + key.attachment());
				}
				((TCPChannel<?>) key.attachment()).register();
			}
		} catch (final IOException | NoConnectionPendingException e) {
			getConnectReadFuture().fail(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void readRegistered(final SelectionKey key) {
		this.receiveBuffer = ByteBuffer.allocateDirect(sendSize);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void writeRegistered(final SelectionKey key) {
		this.sendBuffer = ByteBuffer.allocateDirect(receiveSize);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long read(final SelectionKey key) throws IOException {
		final SocketChannel channel = (SocketChannel) key.channel();
		final MessageBufferProducer<ByteBuffer> chnOut = getChannelOutput();
		final MessageBufferProducer<ByteBuffer> appOut = getOutputBuffer();
		final long n = channel.read(receiveBuffer);
		if (n <= 0) {
			// (n < 0) means channel closed from the other side
			return n;
		}
		receiveBuffer.flip();
		while (codec.hasNext(receiveBuffer)) {
			try {
				final long sequence = chnOut.acquire();
				try {
					final ByteBuffer msg = chnOut.get(sequence);
					msg.clear();
					codec.get(receiveBuffer, msg);
					msg.flip();
					chnOut.attach(sequence, appOut);
				} finally {
					chnOut.release(sequence);
				}
			} catch (final InterruptedException e) {
				throw new IOException(e);
			}
		}
		if (receiveBuffer.remaining() > 0) {
			receiveBuffer.compact();
		} else {
			receiveBuffer.clear();
		}
		return n;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long write(final SelectionKey key) throws IOException {
		final SocketChannel channel = (SocketChannel) key.channel();
		final MessageBufferConsumer<ByteBuffer> chnIn = getChannelInput();
		try {
			int k = chnIn.remaining();
			while (--k >= 0) {
				if (writeSequence == NO_SEQUENCE) {
					writeSequence = chnIn.acquire();
				}
				final ByteBuffer msg = chnIn.get(writeSequence);
				if (codec.length(msg) > sendBuffer.remaining()) {
					break;
				}
				codec.put(msg, sendBuffer);
				msg.clear();
				chnIn.release(writeSequence);
				writeSequence = NO_SEQUENCE;
			}
		} catch (final InterruptedException e) {
			throw new IOException(e);
		}
		sendBuffer.flip();
		final int n = channel.write(sendBuffer);
		if (sendBuffer.remaining() > 0) {
			sendBuffer.compact();
			return n;
		}
		sendBuffer.clear();
		if (chnIn.remaining() == 0) {
			disableWriter();
		}
		return n;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void shutdown(final SettableFuture<Void> future, final Callable<Void> task) {
		try {
			codec.close();
			task.call();
			future.success(null);
		} catch (final Throwable t) {
			future.fail(t);
		}
	}
}
