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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

/**
 * This interface extends the behavior of {@link Closeable}. When
 * {@link #close()} is called, it will not block and return immediately. Callers
 * wishing to wait for the effective completion of {@link #close()} need to call
 * {@link #getCloseFuture()} and wait for its completion.
 * 
 * @author Ricardo Padilha
 */
public interface AsyncCloseable extends Closeable {

	/**
	 * Tell this object to start its closing procedures. This will release any
	 * system resources associated with it. If this object is already closed
	 * then invoking this method has no effect. This method does not wait for
	 * completion and returns immediately.
	 * 
	 * @throws IOException
	 *             if an I/O error occurs
	 * @see #getCloseFuture()
	 */
	@Override
	void close() throws IOException;

	/**
	 * @return a {@link Future} that is done when this channel is closed. Any
	 *         exceptions raised during the closing is returned by this future.
	 */
	Future<Void> getCloseFuture();

}
