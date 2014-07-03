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

package net.dsys.snio.api.codec;

import java.nio.ByteBuffer;

/**
 * Interface definition for framing and delimitation of messages.
 * 
 * @author Ricardo Padilha
 */
public interface MessageCodec {

	/**
	 * @return maximum length of the message header
	 */
	int getHeaderLength();

	/**
	 * @return maximum length of the message body
	 */
	int getBodyLength();

	/**
	 * @return maximum length of the message footer
	 */
	int getFooterLength();

	/**
	 * @return maximum frame length
	 */
	int getFrameLength();

	/**
	 * @return the length of the given {@link ByteBuffer} after encoding
	 */
	int getEncodedLength(ByteBuffer out);

	/**
	 * @return <code>true</code> if the {@link ByteBuffer} fits in a frame
	 * @throws InvalidLengthException
	 *             if the message body is longer than {@link #getBodyLength()}
	 */
	boolean isValid(ByteBuffer out);

	/**
	 * @param in
	 *            {@link ByteBuffer} containing the message body
	 * @param out
	 *            {@link ByteBuffer} where the message will be placed
	 */
	void put(ByteBuffer in, ByteBuffer out) throws InvalidMessageException;

	/**
	 * @return <code>true</code> if there is a frame inside that
	 *         {@link ByteBuffer}
	 * @throws InvalidLengthException
	 *             if the length encoded in the stream invalid.
	 */
	boolean hasNext(ByteBuffer in) throws InvalidEncodingException;

	/**
	 * Only call after {@link #hasNext(ByteBuffer)} returns <code>true</code>.
	 * 
	 * @return the length of the given {@link ByteBuffer} after decoding
	 */
	int getDecodedLength(ByteBuffer in);

	/**
	 * @param in
	 *            {@link ByteBuffer} containing the stream from the channel
	 * @param out
	 *            {@link ByteBuffer} where the message body will be placed
	 */
	void get(ByteBuffer in, ByteBuffer out) throws InvalidEncodingException;

	/**
	 * Close this codec, releasing any resources.
	 */
	void close();

}
