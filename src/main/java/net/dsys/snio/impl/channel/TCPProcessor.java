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

import net.dsys.commons.api.exception.Bug;
import net.dsys.commons.impl.future.SettableCallbackFuture;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.limit.RateLimiter;

/**
 * @author Ricardo Padilha
 */
final class TCPProcessor extends AbstractProcessor<ByteBuffer> {

	private static final int NO_SEQUENCE = -1;

	private final MessageCodec codec;
	private final RateLimiter limiter;
	private final int sendSize;
	private final int receiveSize;
	private ByteBuffer receiveBuffer;
	private ByteBuffer sendBuffer;
	private long writeSequence;

	TCPProcessor(final MessageCodec codec, final RateLimiter limiter,
			final MessageBufferProvider<ByteBuffer> provider,
			final int sendBufferSize, final int receiveBufferSize) {
		super(provider);
		if (codec == null) {
			throw new NullPointerException("codec == null");
		}
		if (limiter == null) {
			throw new NullPointerException("limiter == null");
		}

		final int sendSize = nearestPowerOfTwo(Math.max(sendBufferSize, codec.getFrameLength()));
		final int receiveSize = nearestPowerOfTwo(Math.max(receiveBufferSize, codec.getFrameLength()));
		if (sendSize < 1) {
			throw new IllegalArgumentException("sendSize < 1");
		}
		if (receiveSize < 1) {
			throw new IllegalArgumentException("receiveSize < 1");
		}

		this.codec = codec;
		this.limiter = limiter;
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
					throw new Bug("Unsupported key attachment: " + key.attachment());
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
		this.receiveBuffer = ByteBuffer.allocateDirect(receiveSize);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void writeRegistered(final SelectionKey key) {
		this.sendBuffer = ByteBuffer.allocateDirect(sendSize);
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

		limiter.receive(n);

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
				final int msglen = codec.getEncodedLength(msg);
				if (msglen > sendBuffer.capacity()) {
					// this message is too big for the current buffer
					throw new IOException("codec.length(msg) > sendBuffer.capacity()");
				}
				if (msglen > sendBuffer.remaining()) {
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

		limiter.send(sendBuffer.remaining());

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
	protected void shutdown(final SettableCallbackFuture<Void> future, final Callable<Void> task) {
		try {
			codec.close();
			task.call();
			future.success(null);
		} catch (final Throwable t) {
			future.fail(t);
		}
	}

	/**
	 * @return nearest larger or equal power of two
	 */
	private static int nearestPowerOfTwo(final int num) {
	    int n = 0;
	    if (num > 0) {
			n = num - 1;
		}
	    n |= n >> 1;
	    n |= n >> 2;
	    n |= n >> 4;
	    n |= n >> 8;
	    n |= n >> 16;
	    n++;
	    return n;
	}
}
