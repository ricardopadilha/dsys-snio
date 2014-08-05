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

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import net.dsys.commons.impl.future.SettableCallbackFuture;

/**
 * @author Ricardo Padilha
 */
public interface SelectorExecutor {

	void bind(@Nonnull SelectableChannel channel, @Nonnull Acceptor acceptor);

	void connect(@Nonnull SelectableChannel channel, @Nonnull Processor processor);

	void register(@Nonnull SelectableChannel channel, @Nonnull Processor processor);

	void cancelBind(@Nonnull SelectionKey key, @Nonnull SettableCallbackFuture<Void> future,
			@Nonnull Callable<Void> task);

	void cancelConnect(@Nonnull SelectionKey readKey, @Nonnull SettableCallbackFuture<Void> readFuture,
			@Nonnull SelectionKey writeKey, @Nonnull SettableCallbackFuture<Void> writeFuture);

}
