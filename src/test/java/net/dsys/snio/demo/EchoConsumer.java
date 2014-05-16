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

import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.handler.MessageConsumer;
import net.dsys.snio.api.handler.MessageConsumerFactory;

/**
 * Consumes echo messages.
 * Supports both TCP and UDP channels.
 * 
 * @author Ricardo Padilha
 */
public final class EchoConsumer implements MessageConsumer<ByteBuffer> {

	private static final boolean CHECK = false;
	private static final long SECOND = 1_000_000_000L;

	private final int limit;
	private long start;
	private int counter;
	private int n;

	public EchoConsumer() {
		this(-1);
	}

	public EchoConsumer(final int limit) {
		this.limit = limit;
		this.start = 0;
		this.counter = 0;
		this.n = -1;
	}

	public static MessageConsumerFactory<ByteBuffer> createFactory() {
		return new MessageConsumerFactory<ByteBuffer>() {
			@Override
			public MessageConsumer<ByteBuffer> newInstance(final SocketAddress remote,
					final MessageChannel<ByteBuffer> channel) {
				return new EchoConsumer();
			}
		};
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void consume(final ByteBuffer message, final Object attachment) {
		final int i = message.getInt();
		message.clear();
		// consistency checks
		if (CHECK) {
			assert i == n + 1;
		}
		n++;
		//System.out.println("< " + i);
		counter++;
		final long delta = System.nanoTime() - start;
		if (delta > SECOND) {
			final double tps = counter / (delta / (double) SECOND);
			System.out.println(tps);
			counter = 0;
			start = System.nanoTime();
		}
		if (limit > 0 && n == limit) {
			Thread.currentThread().interrupt();
		}
	}

}
