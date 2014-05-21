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
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import net.dsys.commons.api.exception.Bug;
import net.dsys.snio.api.codec.InvalidEncodingException;
import net.dsys.snio.api.codec.InvalidLengthException;
import net.dsys.snio.api.codec.MessageCodec;

/**
 * Frame encoding that compresses messages using deflate. Messages cannot be
 * longer than 65499 bytes to make sure that they will fit in an UDP datagram.
 * Thread-safety is guaranteed only between encoding and decoding, i.e., two
 * different threads can encode and decode at the same time, but two threads
 * cannot encode at the same time.
 * 
 * @author Ricardo Padilha
 */
final class DeflateCodec implements MessageCodec {

	private static final int UNSIGNED_SHORT_MASK = 0xFFFF;

	private static final int ZLIB_BLOCK_LENGTH = 0x3FFF; // 16383
	private static final int ZLIB_FIXED_OVERHEAD = 6;
	private static final int ZLIB_BLOCK_OVERHEAD = 5;

	private static final int SHORT_LENGTH = Short.SIZE / Byte.SIZE;
	private static final int HEADER_LENGTH = SHORT_LENGTH;
	private static final int FOOTER_LENGTH = 0;
	/**
	 * Deflate has a 26 byte overhead in 65525 bytes, which translates into an
	 * effective 65499 bytes payload.
	 */
	private static final int MAX_DEFLATE_OVERHEAD = 26;
	private static final int MAX_BODY_LENGTH = Codecs.MAX_DATAGRAM_PAYLOAD - HEADER_LENGTH - MAX_DEFLATE_OVERHEAD;

	private final int headerLength;
	private final int bodyLength;
	private final int compressedLength;
	private final int footerLength;
	private final int frameLength;
	private final Deflater deflater;
	private final Inflater inflater;
	private final byte[] deflaterInput;
	private final byte[] deflaterOutput;
	private final byte[] inflaterInput;
	private final byte[] inflaterOutput;

	/**
	 * Returns an instance for the maximum body length
	 */
	DeflateCodec() {
		this(MAX_BODY_LENGTH);
	}

	DeflateCodec(final int bodyLength) {
		if (bodyLength < 1 || bodyLength > MAX_BODY_LENGTH) {
			throw new IllegalArgumentException("bodyLength < 1 || bodyLength > 65499: " + bodyLength);
		}
		this.deflater = new Deflater(Deflater.BEST_SPEED, false);
		this.inflater = new Inflater(false);

		this.headerLength = HEADER_LENGTH;
		this.bodyLength = bodyLength;
		this.compressedLength = maxCompressedLength(bodyLength);
		this.footerLength = FOOTER_LENGTH;
		this.frameLength = headerLength + compressedLength + footerLength;
		if (frameLength > Codecs.MAX_DATAGRAM_PAYLOAD) {
			throw new IllegalArgumentException("frameLength > 65527: " + frameLength);
		}

		this.deflaterInput = new byte[bodyLength];
		this.deflaterOutput = new byte[compressedLength];
		this.inflaterInput = new byte[compressedLength];
		this.inflaterOutput = new byte[bodyLength];
	}

	static int getMaxBodyLength() {
		return MAX_BODY_LENGTH;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageCodec clone() {
		return new DeflateCodec(bodyLength);
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
	public int getFooterLength() {
		return footerLength;
	}

	/**
	 * The overhead for ZLIB is 6 bytes fixed + 5 bytes / 16KB block.
	 * 
	 * @see http://www.zlib.net/zlib_tech.html
	 */
	private static int maxCompressedLength(final int length) {
		final int n = (length / ZLIB_BLOCK_LENGTH) + 1;
		return length + ZLIB_FIXED_OVERHEAD + n * ZLIB_BLOCK_OVERHEAD;
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
		return headerLength + maxCompressedLength(in.remaining());
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
		final int inflated = in.remaining();

		deflater.reset();
		if (in.hasArray()) {
			final int offset = in.arrayOffset() + in.position();
			deflater.setInput(in.array(), offset, inflated);
			in.position(in.position() + inflated);
		} else {
			in.get(deflaterInput, 0, inflated);
			deflater.setInput(deflaterInput, 0, inflated);
		}
		deflater.finish();

		if (out.hasArray()) {
			final int offset = out.arrayOffset() + out.position() + HEADER_LENGTH;
			final int remaining = out.remaining() - HEADER_LENGTH;
			final int deflated = deflater.deflate(out.array(), offset, remaining);
			if (deflated < 1 || deflated > compressedLength) {
				throw new Bug("Unexpected deflated size: " + deflated);
			}
			out.putShort((short) deflated);
			out.position(out.position() + deflated);
		} else {
			final int deflated = deflater.deflate(deflaterOutput);
			if (deflated < 1 || deflated > compressedLength) {
				throw new Bug("Unexpected deflated size: " + deflated);
			}
			out.putShort((short) deflated);
			out.put(deflaterOutput, 0, deflated);
		}
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
		final int length = in.getShort(in.position()) & UNSIGNED_SHORT_MASK;
		if (length < 1 || length > compressedLength) {
			throw new InvalidLengthException(length);
		}
		return (rem >= headerLength + length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void get(final ByteBuffer in, final ByteBuffer out) throws InvalidEncodingException {
		final int deflated = in.getShort() & UNSIGNED_SHORT_MASK;

		inflater.reset();
		if (in.hasArray()) {
			final int offset = in.arrayOffset() + in.position();
			inflater.setInput(in.array(), offset, deflated);
			in.position(in.position() + deflated);
		} else {
			in.get(inflaterInput, 0, deflated);
			inflater.setInput(inflaterInput, 0, deflated);
		}

		try {
			if (out.hasArray()) {
				final int offset = out.arrayOffset() + out.position();
				final int inflated = inflater.inflate(out.array(), offset, out.remaining());
				if (inflated < 1 || inflated > bodyLength) {
					throw new InvalidLengthException(inflated);
				}
				out.position(out.position() + inflated);
			} else {
				final int inflated = inflater.inflate(inflaterOutput);
				if (inflated < 1 || inflated > bodyLength) {
					throw new InvalidLengthException(inflated);
				}
				out.put(inflaterOutput, 0, inflated);
			}
		} catch (final DataFormatException e) {
			throw new InvalidEncodingException(e);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		deflater.end();
		inflater.end();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "DeflateCodec(" + headerLength + ":" + bodyLength + ")";
	}

}
