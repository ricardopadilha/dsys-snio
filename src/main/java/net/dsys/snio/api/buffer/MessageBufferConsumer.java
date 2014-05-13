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
public interface MessageBufferConsumer<T> extends MessageBuffer<T> {

	/**
	 * Obtains the object attached to a buffer position.
	 * 
	 * @param sequence
	 *            a sequence number obtained through {@link #acquire()} or
	 *            {@link #acquire(int)}
	 * @return the attached object
	 */
	Object attachment(long sequence);

	/**
	 * @return a new producer for this consumer.
	 */
	MessageBufferProducer<T> createProducer();
}
