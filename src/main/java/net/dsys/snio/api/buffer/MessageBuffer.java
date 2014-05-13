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

package net.dsys.snio.api.buffer;

/**
 * @author Ricardo Padilha
 */
public interface MessageBuffer<E> {

	/**
	 * Obtains the sequence number of the next free position on the buffer.
	 * Blocks the caller if there are no free positions available.
	 * 
	 * @return the sequence number to be used later on {@link #release(long)}
	 */
	long acquire() throws InterruptedException;

	/**
	 * Tries to acquire up to <code>n</code> free positions on the buffer.
	 * Blocks the caller if there are no free positions available.
	 * 
	 * @param n
	 *            the number of slots to acquire
	 * @return the last sequence number to be used later on
	 *         {@link #release(long, long)}
	 */
	long acquire(int n) throws InterruptedException;

	/**
	 * Returns an estimate of the remaining items in this buffer.
	 * 
	 * @return the number of items available for processing
	 */
	int remaining();

	/**
	 * Get the buffer element at the given sequence number for writing.
	 * 
	 * @param sequence
	 *            a sequence number obtained through {@link #acquire()} or
	 *            {@link #acquire(int)}
	 * @return the buffer element at the given sequence number
	 */
	E get(long sequence);

	/**
	 * Releases all the messages up to the given sequence number for
	 * publication. This will make the buffer element available for processing
	 * by consumers.
	 * 
	 * @param sequence
	 *            releases all messages up to the given sequence number
	 */
	void release(long sequence) throws InterruptedException;

}
