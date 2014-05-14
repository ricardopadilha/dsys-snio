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
import net.dsys.snio.api.codec.InvalidMessageException;
import net.dsys.snio.api.codec.MessageCodec;

/**
 * Sends uncompressed messages up to the bandwidth threshold. Then switches to
 * compression. When the bandwidth usage falls below the threshold, switches
 * back to uncompressed. Messages cannot be longer than 65259 bytes. Non
 * thread-safe.
 * 
 * @see ShortHeaderCodec
 * @see LZ4CompressionCodec
 * @author Ricardo Padilha
 */
public final class MaxTputCodec implements MessageCodec {

	private static final long BYTES_TO_BITS = 8;
	private static final long NSEC_TO_SEC = 1_000_000_000;
	private static final long NSEC_TO_MSEC = 1_000_000;

	private static final int BYTE_LENGTH = 1;
	private static final int HEADER_LENGTH = BYTE_LENGTH;
	private static final byte PLAIN = 0x1;
	private static final byte COMPRESSED = 0x2;

	private final MessageCodec plain;
	private final MessageCodec compressed;
	private final int headerLength;
	private final int bodyLength;
	private final int footerLength;
	private final int frameLength;
	private final long bandwidthThreshold; // in bits per second

	// internal accounting
	private long last;
	private long bytes;
	private boolean usePlain;

	public MaxTputCodec(final int bodyLength, final long bandwidthThreshold) {
		if (bandwidthThreshold < 1) {
			throw new IllegalArgumentException("bandwidthInBitsPerSecond < 1");
		}
		this.plain = new ShortHeaderCodec(bodyLength);
		this.compressed = new LZ4CompressionCodec(bodyLength);
		this.bodyLength = Math.min(plain.getBodyLength(), compressed.getBodyLength()) - HEADER_LENGTH;
		this.headerLength = HEADER_LENGTH + Math.max(plain.getHeaderLength(), compressed.getHeaderLength());
		this.footerLength = Math.max(plain.getFooterLength(), compressed.getFooterLength());
		this.frameLength = headerLength + this.bodyLength + footerLength;
		this.bandwidthThreshold = bandwidthThreshold;
		this.last = System.nanoTime();
		this.bytes = 0;
		this.usePlain = true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageCodec clone() {
		return new MaxTputCodec(bodyLength, bandwidthThreshold);
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
		if (usePlain) {
			return HEADER_LENGTH + plain.length(in);
		}
		return HEADER_LENGTH + compressed.length(in);
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
		// are we close to the bandwidth limit?
		final int pos = out.position();
		if (usePlain) {
			out.put(PLAIN);
			plain.put(in, out);
		} else {
			out.put(COMPRESSED);
			compressed.put(in, out);
		}
		bytes += out.position() - pos;
		calculateBandwidth();
	}

	private void calculateBandwidth() {
		final long current = System.nanoTime();
		final long delta = current - last;
		if (delta >= NSEC_TO_MSEC) {
			final long currentBandwidth = (bytes * BYTES_TO_BITS) * NSEC_TO_SEC / delta; // bits/sec
			last = current;
			bytes = 0;
			if (currentBandwidth > bandwidthThreshold) {
				usePlain = false;
				return;
			}
			usePlain = true;
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
		final int pos = in.position();
		try {
			final byte type = in.get();
			switch (type) {
				case PLAIN: {
					return plain.hasNext(in);
				}
				case COMPRESSED: {
					return compressed.hasNext(in);
				}
				default: {
					throw new InvalidEncodingException("Unsupported message type: " + type);
				}
			}
		} finally {
			in.position(pos);
		}
	}

	/**
	 * Do not call this method unless {@link #hasNext(ByteBuffer)} has been
	 * called before.
	 * 
	 * {@inheritDoc}
	 */
	@Override
	public void get(final ByteBuffer in, final ByteBuffer out) throws InvalidEncodingException {
		final byte type = in.get();
		switch (type) {
			case PLAIN: {
				plain.get(in, out);
				return;
			}
			case COMPRESSED: {
				compressed.get(in, out);
				return;
			}
			default: {
				throw new InvalidEncodingException("Unsupported message type: " + type);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		plain.close();
		compressed.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "MaxTputCodec(" + headerLength + ":" + bodyLength + ")";
	}

}
