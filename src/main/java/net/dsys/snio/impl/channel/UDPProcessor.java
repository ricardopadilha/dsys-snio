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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Callable;

import net.dsys.commons.impl.future.SettableFuture;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.buffer.MessageBufferProvider;
import net.dsys.snio.api.codec.MessageCodec;

/**
 * @author Ricardo Padilha
 */
final class UDPProcessor extends AbstractProcessor<ByteBuffer> {

	private static final int MAX_FRAME_LENGTH = 0xFFFF;

	private final MessageCodec codec;
	private ByteBuffer receiveBuffer;
	private ByteBuffer sendBuffer;

	UDPProcessor(final MessageCodec codec, final MessageBufferProvider<ByteBuffer> provider) {
		super(provider);
		if (codec == null) {
			throw new NullPointerException("codec == null");
		}
		if (codec.getFrameLength() > MAX_FRAME_LENGTH) {
			throw new IllegalArgumentException("codec.getFrameLength() > MAX_FRAME_LENGTH");
		}
		this.codec = codec.clone();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void connect(final SelectionKey key) {
		throw new UnsupportedOperationException("void connect(final SelectionKey key)");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void readRegistered(final SelectionKey key) {
		this.receiveBuffer = ByteBuffer.allocateDirect(MAX_FRAME_LENGTH);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void writeRegistered(final SelectionKey key) {
		this.sendBuffer = ByteBuffer.allocateDirect(MAX_FRAME_LENGTH);
		// start the sendBuffer as empty to ensure the writerKey is disabled
		sendBuffer.flip();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long read(final SelectionKey key) throws IOException {
		final DatagramChannel channel = (DatagramChannel) key.channel();
		final MessageBufferProducer<ByteBuffer> chnOut = getChannelOutput();
		final int start = receiveBuffer.position();
		final SocketAddress source = channel.receive(receiveBuffer);
		if (source == null) {
			return 0;
		}
		final int n = receiveBuffer.position() - start;
		receiveBuffer.flip();
		while (codec.hasNext(receiveBuffer)) {
			try {
				final long sequence = chnOut.acquire();
				try {
					final ByteBuffer buffer = chnOut.get(sequence);
					buffer.clear();
					codec.get(receiveBuffer, buffer);
					buffer.flip();
					chnOut.attach(sequence, source);
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
		final DatagramChannel channel = (DatagramChannel) key.channel();
		final MessageBufferConsumer<ByteBuffer> chnIn = getChannelInput();
		long n = 0;
		long k = chnIn.remaining();
		for (; k >= 0; k--) {
			final SocketAddress address;
			try {
				final long sequence = chnIn.acquire();
				try {
					final ByteBuffer msg = chnIn.get(sequence);
					sendBuffer.clear();
					codec.put(msg, sendBuffer);
					msg.clear();
					sendBuffer.flip();
					address = (SocketAddress) chnIn.attachment(sequence);
				} finally {
					chnIn.release(sequence);
				}
			} catch (final InterruptedException e) {
				throw new IOException(e);
			}
			do {
				n += channel.send(sendBuffer, address);
			} while (sendBuffer.remaining() > 0);
		}
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
