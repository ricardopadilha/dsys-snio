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

import net.dsys.commons.impl.lang.CRC32;
import net.dsys.snio.api.codec.InvalidEncodingException;
import net.dsys.snio.api.codec.InvalidLengthException;
import net.dsys.snio.api.codec.MessageCodec;

/**
 * Frame encoding with a CRC32 checksum at the end.
 * Messages cannot be longer than 65529 bytes. Thread-safe.
 * 
 * @author Ricardo Padilha
 */
public final class ShortCRC32Codec implements MessageCodec {

	private static final int UNSIGNED_SHORT_MASK = 0xFFFF;

	private static final int HEADER_LENGTH = Short.SIZE / Byte.SIZE;
	private static final int FOOTER_LENGTH = Integer.SIZE / Byte.SIZE;
	private static final int MAX_BODY_LENGTH = 0xFFF9; // 65529

	private final int headerLength;
	private final int bodyLength;
	private final int footerLength;
	private final int frameLength;
	private final int tailLength;

	public ShortCRC32Codec(final int bodyLength) {
		if (bodyLength < 1 || bodyLength > MAX_BODY_LENGTH) {
			throw new IllegalArgumentException("bodyLength < 1 || bodyLength > 0xFFF9: " + bodyLength);
		}
		this.bodyLength = bodyLength;
		this.headerLength = HEADER_LENGTH;
		this.footerLength = FOOTER_LENGTH;
		this.frameLength = headerLength + this.bodyLength + footerLength;
		this.tailLength = this.bodyLength + footerLength;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageCodec clone() {
		return new ShortCRC32Codec(bodyLength);
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
	public int getFooterLength() {
		return footerLength;
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
		final int pos = out.position();
		final int length = in.remaining() + FOOTER_LENGTH;
		out.putShort((short) length);
		out.put(in);
		final int crc = CRC32.digest(in, pos, out.position());
		out.putInt(crc);
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
		if (length < 1 || length > tailLength) {
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
	public void get(final ByteBuffer in, final ByteBuffer out) throws InvalidEncodingException {
		final int pos = out.position();
		final int start = in.position();
		final int length = in.getShort() & UNSIGNED_SHORT_MASK; // unsigned short
		final int end = start + headerLength + length - footerLength;
		final int lim = in.limit();
		in.limit(end);
		out.put(in);
		in.limit(lim);
		final int calculated = CRC32.digest(out, pos, out.position());
		final int received = in.getInt();
		if (calculated != received) {
			throw new InvalidEncodingException("mismatching CRC32");
		}
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
