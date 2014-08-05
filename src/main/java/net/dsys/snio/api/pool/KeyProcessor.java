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

package net.dsys.snio.api.pool;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;
import javax.annotation.meta.When;

import net.dsys.commons.api.future.CallbackFuture;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;

/**
 * @author Ricardo Padilha
 */
public interface KeyProcessor<T> {

	/**
	 * Exceptions should be redirected to the {@link Future} returned by
	 * {@link #connect(SelectionKey)}.
	 */
	void connect(@Nonnull SelectionKey key);

	@Nonnull
	CallbackFuture<Void> getConnectionFuture();

	/**
	 * @param thread
	 * @param key
	 *            if <code>null</code>, the registration failed due to a
	 *            ChannelClosedException, i.e., that the channel was closed
	 *            before the registration could be completed.
	 * @param type
	 */
	void registered(@Nonnull SelectorThread thread, @Nonnull(when = When.MAYBE) SelectionKey key,
			@Nonnull SelectionType type);

	@Nonnull
	MessageBufferConsumer<T> getInputBuffer();

	@Nonnull
	MessageBufferProducer<T> getOutputBuffer();

	long read(@Nonnull SelectionKey key) throws IOException;

	long write(@Nonnull SelectionKey key) throws IOException;

	/**
	 * Add WRITE interest to this processor's key.
	 */
	void wakeupWriter();

	/**
	 * Close this processor properly, i.e., cancel from within the selector
	 * threads.
	 */
	void close(@Nonnull SelectorExecutor executor, @Nonnull Callable<Void> closeTask);

	@Nonnull
	CallbackFuture<Void> getCloseFuture();

}
