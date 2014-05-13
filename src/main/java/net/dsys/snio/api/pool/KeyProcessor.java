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
	void connect(SelectionKey key);

	Future<Void> getConnectionFuture();

	/**
	 * @param thread
	 * @param key
	 *            if <code>null</code>, the registration failed due to a
	 *            ChannelClosedException, i.e., that the channel was closed
	 *            before the registration could be completed.
	 * @param type
	 */
	void registered(SelectorThread thread, SelectionKey key, SelectionType type);

	MessageBufferConsumer<T> getInputBuffer();

	MessageBufferProducer<T> getOutputBuffer();

	long read(SelectionKey key) throws IOException;

	long write(SelectionKey key) throws IOException;

	/**
	 * Add WRITE interest to this processor's key.
	 */
	void wakeupWriter();

	/**
	 * Close this processor properly, i.e., cancel from within the selector
	 * threads.
	 */
	void close(SelectorExecutor executor, Callable<Void> closeTask);

	Future<Void> getCloseFuture();

}
