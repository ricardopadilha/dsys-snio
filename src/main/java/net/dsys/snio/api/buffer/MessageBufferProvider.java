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

import javax.annotation.Nonnull;

import net.dsys.snio.api.pool.KeyProcessor;

/**
 * @author Ricardo Padilha
 */
public interface MessageBufferProvider<T> {

	/**
	 * Returns the message buffer that the application uses to send messages.
	 * 
	 * @param processor
	 *            the processor that will be notified when a new message is
	 *            sent.
	 * @see #getChannelInput()
	 */
	@Nonnull
	MessageBufferProducer<T> getAppOutput(@Nonnull KeyProcessor<T> processor);

	/**
	 * @return the message channel where the underlying channel will read from,
	 *         i.e., the messages to be sent.
	 * @see #getAppOutput(KeyProcessor)
	 */
	@Nonnull
	MessageBufferConsumer<T> getChannelInput();

	/**
	 * @return the message channel where messages coming from the network
	 *         channel will be published.
	 * @see #getAppInput()
	 */
	@Nonnull
	MessageBufferProducer<T> getChannelOutput();

	/**
	 * @return the message channel with which the application receives messages.
	 * @see MessageBufferProvider#getChannelOutput()
	 */
	@Nonnull
	MessageBufferConsumer<T> getAppInput();

	/**
	 * Closes all message channels related to this provider.
	 */
	void close();

}
