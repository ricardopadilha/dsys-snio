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
import net.dsys.snio.api.codec.InvalidMessageException;
import net.dsys.snio.api.codec.MessageCodec;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Frame encoding that compresses messages using LZ4. Messages cannot be longer
 * than 65252 bytes to make sure that they will fit in an UDP datagram.
 * Thread-safety is guaranteed only between encoding and decoding, i.e., two
 * different threads can encode and decode at the same time, but two threads
 * cannot encode at the same time.
 * 
 * @author Ricardo Padilha
 */
final class LZ4CompressionCodec implements MessageCodec {

	private static final int UNSIGNED_SHORT_MASK = 0xFFFF;

	private static final int SHORT_LENGTH = Short.SIZE / Byte.SIZE;
	private static final int HEADER_LENGTH = 2 * SHORT_LENGTH;
	private static final int FOOTER_LENGTH = 0;
	/**
	 * LZ4 has a 271 byte overhead in 65523 bytes, which translates into an
	 * effective 65252 bytes payload.
	 */
	private static final int MAX_LZ4_OVERHEAD = 271;
	private static final int MAX_BODY_LENGTH = Codecs.MAX_DATAGRAM_PAYLOAD - HEADER_LENGTH - MAX_LZ4_OVERHEAD;

	private final int headerLength;
	private final int bodyLength;
	private final int compressedLength;
	private final int footerLength;
	private final int frameLength;
	private final LZ4Compressor compressor;
	private final LZ4FastDecompressor decompressor;
	private final byte[] compressInput;
	private final byte[] compressOutput;
	private final byte[] decompressInput;
	private final byte[] decompressOutput;

	/**
	 * Returns an instance for the maximum body length
	 */
	LZ4CompressionCodec() {
		this(MAX_BODY_LENGTH);
	}

	LZ4CompressionCodec(final int bodyLength) {
		if (bodyLength < 1 || bodyLength > MAX_BODY_LENGTH) {
			throw new IllegalArgumentException("bodyLength < 1 || bodyLength > 0xFEEC: " + bodyLength);
		}
		final LZ4Factory factory = LZ4Factory.fastestInstance();
		this.compressor = factory.fastCompressor();
		this.decompressor = factory.fastDecompressor();

		this.bodyLength = bodyLength;
		this.headerLength = HEADER_LENGTH;
		this.compressedLength = compressor.maxCompressedLength(bodyLength);
		this.footerLength = FOOTER_LENGTH;
		this.frameLength = headerLength + compressedLength + footerLength;
		if (frameLength > Codecs.MAX_DATAGRAM_PAYLOAD) {
			throw new IllegalArgumentException("frameLength > 65527: " + frameLength);
		}

		this.compressInput = new byte[this.bodyLength];
		this.compressOutput = new byte[compressedLength];
		this.decompressInput = new byte[compressedLength];
		this.decompressOutput = new byte[this.bodyLength];
	}

	static int getMaxBodyLength() {
		return MAX_BODY_LENGTH;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageCodec clone() {
		return new LZ4CompressionCodec(bodyLength);
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
		return headerLength + compressor.maxCompressedLength(in.remaining());
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
	public void put(final ByteBuffer in, final ByteBuffer out) throws InvalidMessageException {
		final int decompressed = in.remaining();

		final int offsetIn;
		final byte[] arrayIn;
		if (in.hasArray()) {
			offsetIn = in.arrayOffset() + in.position();
			arrayIn = in.array();
			in.position(in.position() + decompressed);
		} else {
			offsetIn = 0;
			arrayIn = compressInput;
			in.get(compressInput, 0, decompressed);
		}

		final int offsetOut;
		final byte[] arrayOut;
		if (out.hasArray()) {
			offsetOut = out.arrayOffset() + out.position() + HEADER_LENGTH;
			arrayOut = out.array();
		} else {
			offsetOut = 0;
			arrayOut = compressOutput;
		}

		final int compressed;
		try {
			compressed = compressor.compress(arrayIn, offsetIn, decompressed, arrayOut, offsetOut);
			if (compressed < 1 || compressed > compressedLength) {
				throw new AssertionError("Unexpected compressed size: " + compressed);
			}
		} catch (final LZ4Exception e) {
			throw new InvalidMessageException(e);
		}
		out.putShort((short) (compressed + SHORT_LENGTH));
		out.putShort((short) decompressed);
		if (out.hasArray()) {
			out.position(out.position() + compressed);
		} else {
			out.put(compressOutput, 0, compressed);
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
		final int compressed = (in.getShort(in.position()) & UNSIGNED_SHORT_MASK) - SHORT_LENGTH;
		if (compressed < 1 || compressed > compressedLength) {
			throw new InvalidLengthException(compressed);
		}
		final int decompressed = in.getShort(in.position() + SHORT_LENGTH) & UNSIGNED_SHORT_MASK;
		if (decompressed < 1 || decompressed > bodyLength) {
			throw new InvalidLengthException(decompressed);
		}
		return (rem >= headerLength + compressed);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void get(final ByteBuffer in, final ByteBuffer out) throws InvalidEncodingException {
		final int compressed = (in.getShort() & UNSIGNED_SHORT_MASK) - SHORT_LENGTH;
		final int decompressed = in.getShort() & UNSIGNED_SHORT_MASK;

		final byte[] arrayIn;
		final int offsetIn;
		if (in.hasArray()) {
			offsetIn = in.arrayOffset() + in.position();
			arrayIn = in.array();
			in.position(in.position() + compressed);
		} else {
			offsetIn = 0;
			arrayIn = decompressInput;
			in.get(decompressInput, 0, compressed);
		}

		final int offsetOut;
		final byte[] arrayOut;
		if (out.hasArray()) {
			offsetOut = out.arrayOffset() + out.position();
			arrayOut = out.array();
		} else {
			offsetOut = 0;
			arrayOut = decompressOutput;
		}

		final int read;
		try {
			read = decompressor.decompress(arrayIn, offsetIn, arrayOut, offsetOut, decompressed);
		} catch (final LZ4Exception e) {
			throw new InvalidEncodingException(e);
		}
		if (read != compressed) {
			throw new InvalidEncodingException("read != compressed");
		}
		if (out.hasArray()) {
			out.position(out.position() + decompressed);
		} else {
			out.put(decompressOutput, 0, decompressed);
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
		return "LZ4CompressionCodec(" + headerLength + ":" + bodyLength + ")";
	}

}
