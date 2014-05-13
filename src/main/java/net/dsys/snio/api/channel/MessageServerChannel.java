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

package net.dsys.snio.api.channel;

import net.dsys.snio.api.io.AsyncServerChannel;

/**
 * @author Ricardo Padilha
 */
public interface MessageServerChannel<E> extends AsyncServerChannel {

	/**
	 * Provide an action to be performed when a new client connection is
	 * accepted.
	 */
	MessageServerChannel<E> onAccept(AcceptListener<E> listener);

	/**
	 * Provide an action to be performed when a connection is closed.
	 */
	MessageServerChannel<E> onClose(CloseListener<E> listener);

}
