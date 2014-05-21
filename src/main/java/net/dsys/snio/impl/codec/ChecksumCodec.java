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
import java.util.zip.Checksum;

import net.dsys.commons.impl.lang.FastArrays;
import net.dsys.snio.api.codec.InvalidEncodingException;
import net.dsys.snio.api.codec.InvalidLengthException;
import net.dsys.snio.api.codec.MessageCodec;

/**
 * Frame encoding with a 32-bit checksum at the end. Messages cannot be longer
 * than 65521 bytes to make sure that they will fit in an UDP datagram.
 * Thread-safety is guaranteed only between encoding and decoding, i.e., two
 * different threads can encode and decode at the same time, but two threads
 * cannot encode at the same time.
 * 
 * @author Ricardo Padilha
 */
final class ChecksumCodec implements MessageCodec {

	private static final int UNSIGNED_SHORT_MASK = 0xFFFF;

	private static final int HEADER_LENGTH = Short.SIZE / Byte.SIZE;
	private static final int FOOTER_LENGTH = Integer.SIZE / Byte.SIZE;
	private static final int MAX_BODY_LENGTH = Codecs.MAX_DATAGRAM_PAYLOAD - HEADER_LENGTH - FOOTER_LENGTH; // 65521

	private final int headerLength;
	private final int bodyLength;
	private final int footerLength;
	private final int frameLength;
	private final int tailLength;
	private final Checksum encoder;
	private final Checksum decoder;
	private final byte[] encoderArray;
	private final byte[] decoderArray;

	/**
	 * Returns an instance for the maximum body length
	 */
	ChecksumCodec(final Checksum encoder, final Checksum decoder) {
		this(encoder, decoder, MAX_BODY_LENGTH);
	}

	ChecksumCodec(final Checksum encoder, final Checksum decoder, final int bodyLength) {
		if (encoder == null) {
			throw new NullPointerException("encoder == null");
		}
		if (decoder == null) {
			throw new NullPointerException("decoder == null");
		}
		if (bodyLength < 1 || bodyLength > MAX_BODY_LENGTH) {
			throw new IllegalArgumentException("bodyLength < 1 || bodyLength > 65521: " + bodyLength);
		}
		this.bodyLength = bodyLength;
		this.headerLength = HEADER_LENGTH;
		this.footerLength = FOOTER_LENGTH;
		this.frameLength = headerLength + this.bodyLength + footerLength;
		if (frameLength > Codecs.MAX_DATAGRAM_PAYLOAD) {
			throw new IllegalArgumentException("frameLength > 65527: " + frameLength);
		}
		this.tailLength = this.bodyLength + footerLength;
		this.encoder = encoder;
		this.decoder = decoder;
		this.encoderArray = new byte[frameLength];
		this.decoderArray = new byte[frameLength];
	}

	static int getMaxBodyLength() {
		return MAX_BODY_LENGTH;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageCodec clone() {
		try {
			return new ChecksumCodec(encoder.getClass().newInstance(), decoder.getClass().newInstance(), bodyLength);
		} catch (final InstantiationException | IllegalAccessException e) {
			throw new UnsupportedOperationException(e);
		}
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
	 * 
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
		final int rem = in.remaining();
		final int len = rem + headerLength;
		final int length = rem + footerLength;
		final int off;
		final byte[] array;
		if (out.hasArray()) {
			array = out.array();
			off = out.arrayOffset() + out.position();
			out.putShort((short) length);
			out.put(in);
		} else {
			array = encoderArray;
			off = 0;
			FastArrays.putShort(encoderArray, 0, (short) length);
			in.get(array, headerLength, rem);
			out.put(array, 0, len);
		}
		encoder.reset();
		encoder.update(array, off, len);
		final int checksum = (int) encoder.getValue();
		out.putInt(checksum);
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
		final int length = in.getShort(in.position()) & UNSIGNED_SHORT_MASK; // unsigned
																				// short
		if (length < 1 || length > tailLength) {
			throw new InvalidLengthException(length);
		}
		return (rem >= headerLength + length);
	}

	/**
	 * Do not call this method unless {@link #hasNext(ByteBuffer)} has been
	 * called before.
	 * 
	 * {@inheritDoc}
	 */
	@Override
	public void get(final ByteBuffer in, final ByteBuffer out) throws InvalidEncodingException {
		final int start = in.position();
		final int length = (in.getShort() & UNSIGNED_SHORT_MASK) - footerLength;
		final int end = start + headerLength + length;
		final int len = length + headerLength;
		final int off;
		final byte[] array;
		if (in.hasArray()) {
			array = in.array();
			off = in.arrayOffset() + start;
			final int lim = in.limit();
			in.limit(end);
			out.put(in);
			in.limit(lim);
		} else {
			array = decoderArray;
			off = 0;
			in.position(start);
			in.get(array, off, len);
			out.put(array, headerLength, length);
		}
		decoder.reset();
		decoder.update(array, off, len);
		final int calculated = (int) decoder.getValue();
		final int received = in.getInt();
		if (calculated != received) {
			throw new InvalidEncodingException("mismatching xxHash");
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
		return "ChecksumCodec[" + encoder + ":" + decoder + "](" + headerLength + ":" + bodyLength + ")";
	}

}
