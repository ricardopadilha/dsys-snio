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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import net.dsys.snio.api.handler.MessageProducer;

/**
 * Produces echo messages.
 * Supports both TCP and UDP channels.
 * 
 * @author Ricardo Padilha
 */
public final class EchoProducer implements MessageProducer<ByteBuffer> {

//	private static final int INT_LENGTH = 4;
	private final InetSocketAddress address;
	private final int limit;
	private int i = 0;

	public EchoProducer() {
		this(-1, null);
	}

	public EchoProducer(final int limit) {
		this(limit, null);
	}

	public EchoProducer(final InetSocketAddress address) {
		this(-1, address);
	}

	public EchoProducer(final int limit, final InetSocketAddress address) {
		this.limit = limit;
		this.address = address;
	}

	@Override
	public Object produce(final ByteBuffer holder) {
		holder.putInt(0, i);
		i++;
//		int j = 0;
//		if (holder.getInt(holder.limit() - INT_LENGTH) == 0) {
//			holder.position(INT_LENGTH);
//			while (holder.remaining() >= INT_LENGTH) {
//				holder.putInt(j++);
//			}
//			holder.flip();
//		}
		if (limit > 0 && i == limit) {
			Thread.currentThread().interrupt();
		}
		return address;
	}
}
