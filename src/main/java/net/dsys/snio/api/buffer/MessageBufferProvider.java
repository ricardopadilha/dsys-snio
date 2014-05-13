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

package net.dsys.snio.api.buffer;

import net.dsys.snio.api.pool.KeyProcessor;

/**
 * @author Ricardo Padilha
 */
public interface MessageBufferProvider<T> {

	MessageBufferProducer<T> getAppOut(KeyProcessor<T> processor);

	MessageBufferConsumer<T> getChannelIn();

	MessageBufferProducer<T> getChannelOut();

	MessageBufferConsumer<T> getAppIn();

	void close();

}
