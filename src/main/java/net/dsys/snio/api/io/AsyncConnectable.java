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
import java.util.concurrent.Future;

/**
 * This interface defines an asynchronous connect operation. When
 * {@link #connect(SocketAddress)} is called, it will not block and return
 * immediately. Callers wishing to wait for the effective completion of the
 * connection need to call {@link #getConnectFuture()} and wait for its
 * completion.
 * 
 * @author Ricardo Padilha
 */
public interface AsyncConnectable {

	/**
	 * Asynchronous call to connect the channel. Callers must wait on the
	 * returned {@link Future} before sending or receiving data.
	 * 
	 * @return a future that can be used to synchronize calls until the channel
	 *         is connected.
	 */
	void connect(SocketAddress remote) throws IOException;

	/**
	 * @return a {@link Future} that is done when this channel is connected. Any
	 *         exceptions raised during the connection is returned by this
	 *         future.
	 */
	Future<Void> getConnectFuture();

}
