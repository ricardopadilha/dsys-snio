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

import java.nio.channels.SelectionKey;

/**
 * Encapsulation for the selection key interests.
 * This is better for switch statements.
 * 
 * @author Ricardo Padilha
 * @see SelectionKey
 */
public enum SelectionType {

	/**
	 * @see SelectionKey#OP_READ
	 */
	OP_READ(SelectionKey.OP_READ),
	/**
	 * @see SelectionKey#OP_WRITE
	 */
	OP_WRITE(SelectionKey.OP_WRITE),
	/**
	 * @see SelectionKey#OP_CONNECT
	 */
	OP_CONNECT(SelectionKey.OP_CONNECT),
	/**
	 * @see SelectionKey#OP_ACCEPT
	 */
	OP_ACCEPT(SelectionKey.OP_ACCEPT);

	private final int op;

	private SelectionType(final int op) {
		this.op = op;
	}

	public int getOp() {
		return op;
	}

}
