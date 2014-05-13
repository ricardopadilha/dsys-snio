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

package net.dsys.snio.demo;

import java.net.SocketAddress;
import java.nio.ByteBuffer;

import net.dsys.snio.api.buffer.InterruptedByClose;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.handler.MessageConsumer;
import net.dsys.snio.api.handler.MessageConsumerFactory;

/**
 * @author Ricardo Padilha
 */
public final class EchoServer implements MessageConsumer<ByteBuffer> {

	public static MessageConsumerFactory<ByteBuffer> createFactory() {
		return new MessageConsumerFactory<ByteBuffer>() {
			@Override
			public MessageConsumer<ByteBuffer> newInstance(final SocketAddress remote,
					final MessageChannel<ByteBuffer> channel) {
				return new EchoServer();
			}
		};
	}

	private static final boolean CHECK = false;

	private final MessageBufferProducer<ByteBuffer> out;
	private int n;

	public EchoServer() {
		this.out = null;
		this.n = -1;
	}

	public EchoServer(final MessageBufferProducer<ByteBuffer> out) {
		this.out = out;
		this.n = -1;
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void consume(final ByteBuffer message, final Object attachment) {
		// process request
		final int i = message.getInt(0);
		message.clear();
		// consistency checks
		if (CHECK) {
			assert i == n + 1;
		}
		n++;
		// prepare reply
		final MessageBufferProducer<ByteBuffer> out;
		final Object attach;
		if (this.out == null) {
			out = (MessageBufferProducer<ByteBuffer>) attachment;
			attach = null;
		} else {
			out = this.out;
			attach = attachment;
		}
		// send reply
		try {
			final long seqOut = out.acquire();
			try {
				// consistency checks
				final ByteBuffer msg = out.get(seqOut);
				msg.clear();
				msg.putInt(0, i);
				out.attach(seqOut, attach);
			} finally {
				out.release(seqOut);
			}
		} catch (final InterruptedByClose e) {
			return;
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (final Throwable t) {
			t.printStackTrace();
			return;
		}
	}
}
