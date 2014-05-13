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

import net.dsys.snio.api.channel.AcceptListener;
import net.dsys.snio.api.channel.CloseListener;

/**
 * @author Ricardo Padilha
 */
public interface KeyAcceptor<T> {

	void onAccept(AcceptListener<T> listener);

	void onClose(CloseListener<T> listener);

	Future<Void> getBindFuture();

	void registered(SelectorThread thread, SelectionKey key);

	void accept(SelectionKey key) throws IOException;

	/**
	 * Close this processor properly, i.e.,
	 * cancel from within the selector threads.
	 */
	void close(SelectorExecutor executor, Callable<Void> closeTask);

	Future<Void> getCloseFuture();

}
