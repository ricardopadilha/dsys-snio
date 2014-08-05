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

package net.dsys.snio.api.io;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.NetworkChannel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.dsys.commons.api.future.CallbackFuture;

/**
 * This interface extends the {@link NetworkChannel} bind operation to support
 * asynchronous behavior. When {@link #bind(SocketAddress)} is called, it will
 * not block and return immediately. Callers wishing to wait for the effective
 * completion of the bind need to call {@link #getBindFuture()} and wait for its
 * completion.
 * 
 * @author Ricardo Padilha
 */
public interface AsyncBindable {

	/**
	 * @see NetworkChannel#bind(SocketAddress)
	 */
	@Nonnull
	NetworkChannel bind(@Nullable SocketAddress local) throws IOException;

	/**
	 * The {@code backlog} parameter is the maximum number of pending
	 * connections on the socket. Its exact semantics are implementation
	 * specific. In particular, an implementation may impose a maximum length or
	 * may choose to ignore the parameter altogether. If the {@code backlog}
	 * parameter has the value {@code 0}, or a negative value, then an
	 * implementation specific default is used.
	 * 
	 * @see NetworkChannel#bind(SocketAddress)
	 */
	@Nonnull
	NetworkChannel bind(@Nullable SocketAddress local, int backlog) throws IOException;

	/**
	 * @return a {@link Future} that is done when this channel is bound. Any
	 *         exceptions raised during the binding is returned by this future.
	 */
	@Nonnull
	CallbackFuture<Void> getBindFuture();

}
