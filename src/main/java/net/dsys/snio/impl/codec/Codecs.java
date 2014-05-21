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

import java.util.zip.Adler32;
import java.util.zip.CRC32;

import net.dsys.snio.api.codec.MessageCodec;

/**
 * @author Ricardo Padilha
 */
public final class Codecs {

	private static final int MAX_DATAGRAM_LENGTH = 0xFFFF;
	private static final int DATAGRAM_HEADER_LENGTH = 8;
	static final int MAX_DATAGRAM_PAYLOAD = MAX_DATAGRAM_LENGTH - DATAGRAM_HEADER_LENGTH;

	private Codecs() {
		// no instantiation allowed
		return;
	}

	/**
	 * Frame encoding with a simple two-byte length header. Messages cannot be
	 * longer than 65525 bytes to make sure that they will fit in an UDP
	 * datagram. This codec is thread-safe.
	 * 
	 * @return a codec configured for its maximum supported body length
	 */
	public static MessageCodec getDefault() {
		return new ShortHeaderCodec();
	}

	/**
	 * Frame encoding with a simple two-byte length header. Messages cannot be
	 * longer than 65525 bytes to make sure that they will fit in an UDP
	 * datagram. This codec is thread-safe.
	 * 
	 * @param bodyLength
	 *            maximum length of messages
	 * @return a codec configured for the given body length
	 * @throws IllegalArgumentException
	 *             if the body length is too small or too large
	 */
	public static MessageCodec getDefault(final int bodyLength) {
		return new ShortHeaderCodec(bodyLength);
	}

	/**
	 * Frame encoding with a CRC32 checksum at the end. Messages cannot be
	 * longer than 65521 bytes to make sure that they will fit in an UDP
	 * datagram. Thread-safety is guaranteed only between encoding and decoding,
	 * i.e., two different threads can encode and decode at the same time, but
	 * two threads cannot encode at the same time.
	 * 
	 * @return a codec configured for its maximum supported body length
	 */
	public static MessageCodec getCRC32Checksum() {
		return new ChecksumCodec(new CRC32(), new CRC32());
	}

	/**
	 * Frame encoding with a CRC32 checksum at the end. Messages cannot be
	 * longer than 65521 bytes to make sure that they will fit in an UDP
	 * datagram. Thread-safety is guaranteed only between encoding and decoding,
	 * i.e., two different threads can encode and decode at the same time, but
	 * two threads cannot encode at the same time.
	 * 
	 * @param bodyLength
	 *            maximum length of messages
	 * @return a codec configured for the given body length
	 * @throws IllegalArgumentException
	 *             if the body length is too small or too large
	 */
	public static MessageCodec getCRC32Checksum(final int bodyLength) {
		return new ChecksumCodec(new CRC32(), new CRC32(), bodyLength);
	}

	/**
	 * Frame encoding with an Adler32 checksum at the end. Messages cannot be
	 * longer than 65521 bytes to make sure that they will fit in an UDP
	 * datagram. Thread-safety is guaranteed only between encoding and decoding,
	 * i.e., two different threads can encode and decode at the same time, but
	 * two threads cannot encode at the same time.
	 * 
	 * @return a codec configured for its maximum supported body length
	 */
	public static MessageCodec getAdler32Checksum() {
		return new ChecksumCodec(new Adler32(), new Adler32());
	}

	/**
	 * Frame encoding with an Adler32 checksum at the end. Messages cannot be
	 * longer than 65521 bytes to make sure that they will fit in an UDP
	 * datagram. Thread-safety is guaranteed only between encoding and decoding,
	 * i.e., two different threads can encode and decode at the same time, but
	 * two threads cannot encode at the same time.
	 * 
	 * @param bodyLength
	 *            maximum length of messages
	 * @return a codec configured for the given body length
	 * @throws IllegalArgumentException
	 *             if the body length is too small or too large
	 */
	public static MessageCodec getAdler32Checksum(final int bodyLength) {
		return new ChecksumCodec(new Adler32(), new Adler32(), bodyLength);
	}

	/**
	 * Frame encoding with an xxHash checksum at the end. Messages cannot be
	 * longer than 65521 bytes to make sure that they will fit in an UDP
	 * datagram. Thread-safety is guaranteed only between encoding and decoding,
	 * i.e., two different threads can encode and decode at the same time, but
	 * two threads cannot encode at the same time.
	 * 
	 * @return a codec configured for its maximum supported body length
	 */
	public static MessageCodec getXXHashChecksum() {
		return new ChecksumCodec(new XXHashChecksum(), new XXHashChecksum());
	}

	/**
	 * Frame encoding with an xxHash checksum at the end. Messages cannot be
	 * longer than 65521 bytes to make sure that they will fit in an UDP
	 * datagram. Thread-safety is guaranteed only between encoding and decoding,
	 * i.e., two different threads can encode and decode at the same time, but
	 * two threads cannot encode at the same time.
	 * 
	 * @param bodyLength
	 *            maximum length of messages
	 * @return a codec configured for the given body length
	 * @throws IllegalArgumentException
	 *             if the body length is too small or too large
	 */
	public static MessageCodec getXXHashChecksum(final int bodyLength) {
		return new ChecksumCodec(new XXHashChecksum(), new XXHashChecksum(), bodyLength);
	}

	/**
	 * Frame encoding that compresses messages using deflate. Messages cannot be
	 * longer than 65499 bytes to make sure that they will fit in an UDP
	 * datagram. Thread-safety is guaranteed only between encoding and decoding,
	 * i.e., two different threads can encode and decode at the same time, but
	 * two threads cannot encode at the same time.
	 * 
	 * @return a codec configured for its maximum supported body length
	 */
	public static MessageCodec getDeflateCompression() {
		return new DeflateCodec();
	}

	/**
	 * Frame encoding that compresses messages using deflate. Messages cannot be
	 * longer than 65499 bytes to make sure that they will fit in an UDP
	 * datagram. Thread-safety is guaranteed only between encoding and decoding,
	 * i.e., two different threads can encode and decode at the same time, but
	 * two threads cannot encode at the same time.
	 * 
	 * @param bodyLength
	 *            maximum length of messages
	 * @return a codec configured for the given body length
	 * @throws IllegalArgumentException
	 *             if the body length is too small or too large
	 */
	public static MessageCodec getDeflateCompression(final int bodyLength) {
		return new DeflateCodec(bodyLength);
	}

	/**
	 * Frame encoding that compresses messages using LZ4. Messages cannot be
	 * longer than 65252 bytes to make sure that they will fit in an UDP
	 * datagram. Thread-safety is guaranteed only between encoding and decoding,
	 * i.e., two different threads can encode and decode at the same time, but
	 * two threads cannot encode at the same time.
	 * 
	 * @return a codec configured for its maximum supported body length
	 */
	public static MessageCodec getLZ4Compression() {
		return new LZ4CompressionCodec();
	}

	/**
	 * Frame encoding that compresses messages using LZ4. Messages cannot be
	 * longer than 65252 bytes to make sure that they will fit in an UDP
	 * datagram. Thread-safety is guaranteed only between encoding and decoding,
	 * i.e., two different threads can encode and decode at the same time, but
	 * two threads cannot encode at the same time.
	 * 
	 * @param bodyLength
	 *            maximum length of messages
	 * @return a codec configured for the given body length
	 * @throws IllegalArgumentException
	 *             if the body length is too small or too large
	 */
	public static MessageCodec getLZ4Compression(final int bodyLength) {
		return new LZ4CompressionCodec(bodyLength);
	}

}
