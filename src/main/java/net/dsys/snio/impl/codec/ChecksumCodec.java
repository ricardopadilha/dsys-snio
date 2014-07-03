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

import net.dsys.snio.api.codec.InvalidEncodingException;
import net.dsys.snio.api.codec.InvalidMessageException;
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

	private static final int INT_LENGTH = Integer.SIZE / Byte.SIZE;

	private final MessageCodec delegate;
	private final int headerLength;
	private final int bodyLength;
	private final int footerLength;
	private final int frameLength;

	private final Checksum encoder;
	private final Checksum decoder;
	private final byte[] encoderArray;
	private final byte[] decoderArray;

	ChecksumCodec(final MessageCodec codec, final Checksum encoder, final Checksum decoder) {
		if (codec == null) {
			throw new NullPointerException("codec == null");
		}
		if (encoder == null) {
			throw new NullPointerException("encoder == null");
		}
		if (decoder == null) {
			throw new NullPointerException("decoder == null");
		}

		this.delegate = codec;
		this.headerLength = codec.getHeaderLength();
		this.bodyLength = codec.getBodyLength();
		this.footerLength = codec.getFooterLength() + INT_LENGTH;
		this.frameLength = codec.getFrameLength() + INT_LENGTH;

		this.encoder = encoder;
		this.decoder = decoder;
		this.encoderArray = new byte[codec.getFrameLength()];
		this.decoderArray = new byte[codec.getFrameLength()];
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
	public int getEncodedLength(final ByteBuffer in) {
		return delegate.getEncodedLength(in) + INT_LENGTH;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isValid(final ByteBuffer out) {
		final int length = out.remaining();
		return length > 0 && length <= bodyLength;
	}

	/**
	 * {@inheritDoc}
	 * @throws InvalidMessageException 
	 */
	@Override
	public void put(final ByteBuffer in, final ByteBuffer out) throws InvalidMessageException {
		final int start = out.position();
		delegate.put(in, out);
		final int end = out.position();
		final int len = end - start;

		final int off;
		final byte[] array;
		if (out.hasArray()) {
			array = out.array();
			off = out.arrayOffset() + start;
		} else {
			array = encoderArray;
			off = 0;
			// copy to a local array
			final int lim = out.limit();
			out.position(start).limit(end);
			out.get(array, off, len);
			out.limit(lim).position(end);
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
		boolean hasNext = delegate.hasNext(in);
		if (hasNext) {
			final int rem = in.remaining();
			hasNext = (rem >= getDecodedLength(in));
		}
		return hasNext;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getDecodedLength(final ByteBuffer in) {
		return delegate.getDecodedLength(in) + INT_LENGTH;
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
		delegate.get(in, out);
		final int end = in.position();
		final int len = end - start;

		final int off;
		final byte[] array;
		if (in.hasArray()) {
			array = in.array();
			off = in.arrayOffset() + start;
		} else {
			array = decoderArray;
			off = 0;
			final int lim = in.limit();
			in.position(start).limit(end);
			in.get(array, off, len);
			in.limit(lim).position(end);
		}
		decoder.reset();
		decoder.update(array, off, len);
		final int calculated = (int) decoder.getValue();
		final int received = in.getInt();
		if (calculated != received) {
			throw new InvalidEncodingException("mismatching checksum");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		delegate.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "ChecksumCodec[" + encoder + ":" + decoder + "](" + delegate + ")";
	}

}
