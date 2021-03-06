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

package net.dsys.snio.api.group;

import java.util.NoSuchElementException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * @author Ricardo Padilha
 */
public interface GroupData<T> extends Iterable<T> {

	/**
	 * @return the size of this group data.
	 */
	@Nonnegative
	int size();

	/**
	 * @throws NoSuchElementException if there is no data for the given index.
	 */
	@Nonnull
	T get(@Nonnegative int index);

}
