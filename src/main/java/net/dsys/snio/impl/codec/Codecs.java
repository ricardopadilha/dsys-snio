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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.api.lang.Factory;
import net.dsys.snio.api.codec.MessageCodec;

/**
 * @author Ricardo Padilha
 */
public final class Codecs {

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
	@Nonnull
	public static MessageCodec getShort() {
		return new ShortHeaderCodec();
	}

	@Nonnull
	public static Factory<MessageCodec> getShortFactory() {
		return new Factory<MessageCodec>() {
			@Override
			public MessageCodec newInstance() {
				return getShort();
			}
		};
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
	@Nonnull
	public static MessageCodec getShort(@Nonnegative final int bodyLength) {
		return new ShortHeaderCodec(bodyLength);
	}

	@Nonnull
	public static Factory<MessageCodec> getShortFactory(@Nonnegative final int bodyLength) {
		return new Factory<MessageCodec>() {
			@Override
			public MessageCodec newInstance() {
				return getShort(bodyLength);
			}
		};
	}

	/**
	 * Frame encoding with a simple int length header. Messages cannot be longer
	 * than 2^31-5 bytes. This codec is thread-safe. Messages cannot be longer
	 * than 65531 bytes if they need to fit in an UDP datagram.
	 * 
	 * @param bodyLength
	 *            maximum length of messages
	 * @return a codec configured for the given body length
	 * @throws IllegalArgumentException
	 *             if the body length is too small or too large
	 */
	@Nonnull
	public static MessageCodec getDefault(@Nonnegative final int bodyLength) {
		return new IntHeaderCodec(bodyLength);
	}

	@Nonnull
	public static Factory<MessageCodec> getDefaultFactory(@Nonnegative final int bodyLength) {
		return new Factory<MessageCodec>() {
			@Override
			public MessageCodec newInstance() {
				return getDefault(bodyLength);
			}
		};
	}

	/**
	 * Frame encoding with a CRC32 checksum at the end. Messages cannot be
	 * longer than 65521 bytes if they need to fit in an UDP datagram.
	 * Thread-safety is guaranteed only between encoding and decoding, i.e., two
	 * different threads can encode and decode at the same time, but two threads
	 * cannot encode at the same time.
	 * 
	 * @param bodyLength
	 *            maximum length of messages
	 * @return a codec configured for the given body length
	 * @throws IllegalArgumentException
	 *             if the body length is too small or too large
	 */
	@Nonnull
	public static MessageCodec getCRC32Checksum(@Nonnegative final int bodyLength) {
		return new ChecksumCodec(getDefault(bodyLength), new CRC32(), new CRC32());
	}

	@Nonnull
	public static Factory<MessageCodec> getCRC32Factory(@Nonnegative final int bodyLength) {
		return new Factory<MessageCodec>() {
			@Override
			public MessageCodec newInstance() {
				return getCRC32Checksum(bodyLength);
			}
		};
	}

	/**
	 * Frame encoding with an Adler32 checksum at the end. Messages cannot be
	 * longer than 65521 bytes if they need to fit in an UDP datagram.
	 * Thread-safety is guaranteed only between encoding and decoding, i.e., two
	 * different threads can encode and decode at the same time, but two threads
	 * cannot encode at the same time.
	 * 
	 * @param bodyLength
	 *            maximum length of messages
	 * @return a codec configured for the given body length
	 * @throws IllegalArgumentException
	 *             if the body length is too small or too large
	 */
	@Nonnull
	public static MessageCodec getAdler32Checksum(@Nonnegative final int bodyLength) {
		return new ChecksumCodec(getDefault(bodyLength), new Adler32(), new Adler32());
	}

	@Nonnull
	public static Factory<MessageCodec> getAdler32Factory(@Nonnegative final int bodyLength) {
		return new Factory<MessageCodec>() {
			@Override
			public MessageCodec newInstance() {
				return getAdler32Checksum(bodyLength);
			}
		};
	}

	/**
	 * Frame encoding with an xxHash checksum at the end. Messages cannot be
	 * longer than 65521 bytes if they need to fit in an UDP datagram.
	 * Thread-safety is guaranteed only between encoding and decoding, i.e., two
	 * different threads can encode and decode at the same time, but two threads
	 * cannot encode at the same time.
	 * 
	 * @param bodyLength
	 *            maximum length of messages
	 * @return a codec configured for the given body length
	 * @throws IllegalArgumentException
	 *             if the body length is too small or too large
	 */
	@Nonnull
	public static MessageCodec getXXHashChecksum(@Nonnegative final int bodyLength) {
		return new ChecksumCodec(getDefault(bodyLength), new XXHashChecksum(), new XXHashChecksum());
	}

	@Nonnull
	public static Factory<MessageCodec> getXXHashFactory(@Nonnegative final int bodyLength) {
		return new Factory<MessageCodec>() {
			@Override
			public MessageCodec newInstance() {
				return getXXHashChecksum(bodyLength);
			}
		};
	}

	/**
	 * Frame encoding that compresses messages using deflate. Messages cannot be
	 * longer than 65499 bytes if they need to fit in an UDP datagram.
	 * Thread-safety is guaranteed only between encoding and decoding, i.e., two
	 * different threads can encode and decode at the same time, but two threads
	 * cannot encode at the same time.
	 * 
	 * @param bodyLength
	 *            maximum length of messages
	 * @return a codec configured for the given body length
	 * @throws IllegalArgumentException
	 *             if the body length is too small or too large
	 */
	@Nonnull
	public static MessageCodec getDeflateCompression(@Nonnegative final int bodyLength) {
		return new DeflateCodec(bodyLength);
	}

	@Nonnull
	public static Factory<MessageCodec> getDeflateFactory(@Nonnegative final int bodyLength) {
		return new Factory<MessageCodec>() {
			@Override
			public MessageCodec newInstance() {
				return getDeflateCompression(bodyLength);
			}
		};
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
	@Nonnull
	public static MessageCodec getLZ4Compression(@Nonnegative final int bodyLength) {
		return new LZ4CompressionCodec(bodyLength);
	}

	@Nonnull
	public static Factory<MessageCodec> getLZ4Factory(@Nonnegative final int bodyLength) {
		return new Factory<MessageCodec>() {
			@Override
			public MessageCodec newInstance() {
				return getLZ4Compression(bodyLength);
			}
		};
	}
}
