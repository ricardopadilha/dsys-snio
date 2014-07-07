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

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_TASK;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_WRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_OVERFLOW;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NoConnectionPendingException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSession;

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
final class SSLProcessor extends AbstractProcessor<ByteBuffer> {

	private static final int NO_SEQUENCE = -1;

	private final MessageCodec codec;
	private final RateLimiter limiter;
	private final SSLEngine engine;
	private final int sendSize;
	private final int receiveSize;
	private ByteBuffer receiveBuffer;
	private ByteBuffer sendBuffer;
	private ByteBuffer preSendBuffer;
	private ByteBuffer postReceiveBuffer;
	private long writeSequence;
	private volatile SettableCallbackFuture<Void> closeFuture;
	private volatile Callable<Void> closeTask;
	private volatile boolean closedInternally;
	private volatile boolean closed;

	SSLProcessor(final MessageCodec codec, final RateLimiter limiter,
			final MessageBufferProvider<ByteBuffer> provider,
			final int sendBufferSize, final int receiveBufferSize, final SSLEngine engine) {
		super(provider);
		if (codec == null) {
			throw new NullPointerException("codec == null");
		}
		if (limiter == null) {
			throw new NullPointerException("limiter == null");
		}
		if (engine == null) {
			throw new NullPointerException("engine == null");
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
		this.engine = engine;
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
				assert key.attachment() instanceof TCPChannel;
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
		final SSLSession session = engine.getSession();
		final int delta = session.getPacketBufferSize() - session.getApplicationBufferSize();
		final int outSize = Math.max(receiveSize, session.getPacketBufferSize());
		final int inSize = Math.max(receiveSize - delta, session.getApplicationBufferSize());
		this.receiveBuffer = ByteBuffer.allocateDirect(outSize);
		this.postReceiveBuffer = ByteBuffer.allocate(inSize);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void writeRegistered(final SelectionKey key) {
		final SSLSession session = engine.getSession();
		final int delta = session.getPacketBufferSize() - session.getApplicationBufferSize();
		final int inSize = Math.max(sendSize - delta, session.getApplicationBufferSize());
		final int outSize = Math.max(sendSize, session.getPacketBufferSize());
		this.preSendBuffer = ByteBuffer.allocate(inSize);
		this.sendBuffer = ByteBuffer.allocateDirect(outSize);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long read(final SelectionKey key) throws IOException {
		if (closed) {
			return 0;
		}
		final SocketChannel channel = (SocketChannel) key.channel();
		final MessageBufferProducer<ByteBuffer> chnOut = getChannelOutput();
		final MessageBufferProducer<ByteBuffer> appOut = getOutputBuffer();
		final long n = channel.read(receiveBuffer);
		if (n <= 0) {
			// (n < 0) means channel closed from the other side
			closedInternally = true;
			return n;
		}

		limiter.receive(n);

		receiveBuffer.flip();

		// SSL handling
		boolean closed = false;
		SSLEngineResult result = engine.unwrap(receiveBuffer, postReceiveBuffer);
		// state machine
		if (result.getHandshakeStatus() != NOT_HANDSHAKING) {
			// handshaking
			SSLEngineResult.Status status = result.getStatus();
			SSLEngineResult.HandshakeStatus hstatus = result.getHandshakeStatus();
			while (status != BUFFER_OVERFLOW && hstatus != NEED_WRAP && hstatus != FINISHED) {
				assert hstatus == NEED_UNWRAP || hstatus == NEED_TASK;
				if (hstatus == NEED_TASK) {
					Runnable task;
					while ((task = engine.getDelegatedTask()) != null) {
						task.run();
					}
				}
				result = engine.unwrap(receiveBuffer, postReceiveBuffer);
				status = result.getStatus();
				hstatus = result.getHandshakeStatus();
				assert result.bytesProduced() == 0;
			}
			if (hstatus == NEED_WRAP) {
				wakeupWriter();
			}
			if (receiveBuffer.remaining() > 0) {
				receiveBuffer.compact();
			} else {
				receiveBuffer.clear();
			}
		} else {
			switch (result.getStatus()) {
				case OK: {
					assert result.bytesConsumed() > 0 && result.bytesProduced() > 0;
					// normal encryption and send
					if (receiveBuffer.remaining() > 0) {
						receiveBuffer.compact();
					} else {
						receiveBuffer.clear();
					}
					break;
				}
				case BUFFER_UNDERFLOW: {
					assert result.bytesConsumed() == 0 && result.bytesProduced() == 0;
					// We can't decrypt more until some bytes are received.
					// We have to "unflip" receiveBuffer, otherwise the flip above
					// will "zero" the buffer the next time read() is called.
					receiveBuffer.position(receiveBuffer.limit());
					receiveBuffer.limit(receiveBuffer.capacity());
					break;
				}
				case BUFFER_OVERFLOW: {
					assert result.bytesConsumed() == 0 && result.bytesProduced() == 0;
					// We can't decrypt more until some bytes are delivered.
					// We have to "unflip" receiveBuffer, otherwise the flip above
					// will "zero" the buffer the next time read() is called.
					receiveBuffer.position(receiveBuffer.limit());
					receiveBuffer.limit(receiveBuffer.capacity());
					break;
				}
				case CLOSED: {
					//assert result.bytesConsumed() > 0 && result.bytesProduced() == 0;
					// SSLEngine close handshake was completed
					closedInternally = true;
					closed = true;
					break;
				}
				default: {
					// some status code that is not known
					throw new Bug("Unsupported SSLEngineResult.Status: " + result.getStatus());
				}
			}
		}
		postReceiveBuffer.flip();

		while (codec.hasNext(postReceiveBuffer)) {
			try {
				final long sequence = chnOut.acquire();
				try {
					final ByteBuffer msg = chnOut.get(sequence);
					msg.clear();
					codec.get(postReceiveBuffer, msg);
					msg.flip();
					chnOut.attach(sequence, appOut);
				} finally {
					chnOut.release(sequence);
				}
			} catch (final InterruptedException e) {
				throw new IOException(e);
			}
		}
		if (postReceiveBuffer.remaining() > 0) {
			postReceiveBuffer.compact();
		} else {
			postReceiveBuffer.clear();
		}
		if (closed) {
			return -1;
		}
		return n;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long write(final SelectionKey key) throws IOException {
		if (closed) {
			return 0;
		}
		final SocketChannel channel = (SocketChannel) key.channel();

		final MessageBufferConsumer<ByteBuffer> chnIn = getChannelInput();
		try {
			long k = chnIn.remaining();
			while (--k >= 0) {
				if (writeSequence == NO_SEQUENCE) {
					writeSequence = chnIn.acquire();
				}
				final ByteBuffer msg = chnIn.get(writeSequence);
				final int msglen = codec.getEncodedLength(msg);
				if (msglen > preSendBuffer.capacity()) {
					// this message is too big for the current buffer
					throw new IOException("codec.length(msg) > preSendBuffer.capacity()");
				}
				if (msglen > preSendBuffer.remaining()) {
					break;
				}
				codec.put(msg, preSendBuffer);
				msg.clear();
				chnIn.release(writeSequence);
				writeSequence = NO_SEQUENCE;
			}
		} catch (final InterruptedException e) {
			throw new IOException(e);
		}
		preSendBuffer.flip(); // ready to send

		// SSL handling
		boolean closed = false;
		SSLEngineResult result = engine.wrap(preSendBuffer, sendBuffer);
		// state machine
		if (result.getHandshakeStatus() != NOT_HANDSHAKING) {
			// handshaking
			SSLEngineResult.Status status = result.getStatus();
			SSLEngineResult.HandshakeStatus hstatus = result.getHandshakeStatus();
			while (status != BUFFER_OVERFLOW && hstatus != NEED_UNWRAP && hstatus != FINISHED) {
				assert hstatus == NEED_WRAP || hstatus == NEED_TASK;
				if (hstatus == NEED_TASK) {
					Runnable task;
					while ((task = engine.getDelegatedTask()) != null) {
						task.run();
					}
				}
				result = engine.wrap(preSendBuffer, sendBuffer);
				status = result.getStatus();
				hstatus = result.getHandshakeStatus();
				assert result.bytesConsumed() == 0;
			}
			if (preSendBuffer.remaining() > 0) {
				preSendBuffer.compact();
			} else {
				preSendBuffer.clear();
			}
		} else {
			switch (result.getStatus()) {
				case OK: {
					//assert result.bytesConsumed() > 0 && result.bytesProduced() > 0;
					// normal encryption and send
					if (preSendBuffer.remaining() > 0) {
						preSendBuffer.compact();
					} else {
						preSendBuffer.clear();
					}
					break;
				}
				case BUFFER_OVERFLOW: {
					assert result.bytesConsumed() == 0 && result.bytesProduced() == 0;
					// We can't encrypt more until some bytes are sent.
					// We have to "unflip" preSendBuffer, otherwise the flip above
					// will "zero" the buffer the next time write() is called.
					preSendBuffer.position(preSendBuffer.limit());
					preSendBuffer.limit(preSendBuffer.capacity());
					break;
				}
				case CLOSED: {
					//assert result.bytesConsumed() == 0 && result.bytesProduced() > 0;
					// SSLEngine close handshake was completed
					closedInternally = true;
					closed = true;
					break;
				}
				case BUFFER_UNDERFLOW:
				default: {
					// both cases are illegal here
					throw new Bug("Unsupported SSLEngineResult.Status: " + result.getStatus());
				}
			}
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
		if (closed) {
			return -1;
		}
		return n;
	}

	private void shutdown() {
		try {
			codec.close();
			closeTask.call();
			closeFuture.success(null);
			closed = true;
		} catch (final Throwable t) {
			closeFuture.fail(t);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void shutdown(final SettableCallbackFuture<Void> future, final Callable<Void> task) {
		this.closeFuture = future;
		this.closeTask = task;
		engine.closeOutbound();
		if (closedInternally || (receiveBuffer == null && sendBuffer == null)) {
			// not yet connected or disconnected remotely
			shutdown();
		} else {
			wakeupWriter();
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
