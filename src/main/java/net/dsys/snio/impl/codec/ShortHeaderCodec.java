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

package net.dsys.snio.impl.codec;

import java.nio.ByteBuffer;

import net.dsys.snio.api.codec.InvalidEncodingException;
import net.dsys.snio.api.codec.InvalidLengthException;
import net.dsys.snio.api.codec.MessageCodec;

/**
 * Simple frame encoding which just adds an unsigned short length field as a
 * header. Messages cannot be longer than 65533 bytes. Thread-safe.
 * 
 * @author Ricardo Padilha
 */
public final class ShortHeaderCodec implements MessageCodec {

	private static final int UNSIGNED_SHORT_MASK = 0xFFFF;

	private static final int HEADER_LENGTH = Short.SIZE / Byte.SIZE;
	private static final int MAX_BODY_LENGTH = 0xFFFD; // 65533

	private final int headerLength;
	private final int bodyLength;
	private final int frameLength;

	public ShortHeaderCodec(final int bodyLength) {
		if (bodyLength < 1 || bodyLength > MAX_BODY_LENGTH) {
			throw new IllegalArgumentException("bodyLength < 1 || bodyLength > 0xFFFD: " + bodyLength);
		}
		this.bodyLength = bodyLength;
		this.headerLength = HEADER_LENGTH;
		this.frameLength = headerLength + bodyLength;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageCodec clone() {
		return new ShortHeaderCodec(bodyLength);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getHeaderLength() {
		return headerLength;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getBodyLength() {
		return bodyLength;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getFrameLength() {
		return frameLength;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int length(final ByteBuffer in) {
		return headerLength + in.remaining();
	}

	/**
	 * {@inheritDoc}
	 * @throws InvalidLengthException 
	 */
	@Override
	public boolean isValid(final ByteBuffer out) {
		final int length = out.remaining();
		return length > 0 && length <= bodyLength;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void put(final ByteBuffer in, final ByteBuffer out) {
		final int length = in.remaining();
		out.putShort((short) length);
		out.put(in);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNext(final ByteBuffer in) throws InvalidEncodingException {
		final int rem = in.remaining();
		if (rem < headerLength) {
			return false;
		}
		final int length = in.getShort(in.position()) & UNSIGNED_SHORT_MASK; // unsigned short
		if (length < 1 || length > bodyLength) {
			throw new InvalidLengthException(length);
		}
		return (rem >= headerLength + length);
	}

	/**
	 * Do not call this method unless {@link #hasNext(ByteBuffer)} has been called before.
	 * 
	 * {@inheritDoc}
	 */
	@Override
	public void get(final ByteBuffer in, final ByteBuffer out) {
		final int pos = in.position();
		final int length = in.getShort() & UNSIGNED_SHORT_MASK; // unsigned short
		final int end = pos + headerLength + length;
		final int lim = in.limit();
		in.limit(end);
		out.put(in);
		in.limit(lim);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		return;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "ShortHeaderCodec(" + headerLength + ":" + bodyLength + ")";
	}

}
