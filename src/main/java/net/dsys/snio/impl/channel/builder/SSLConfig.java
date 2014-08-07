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

package net.dsys.snio.impl.channel.builder;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;

import net.dsys.commons.impl.builder.Mandatory;

/**
 * @author Ricardo Padilha
 */
public final class SSLConfig {

	private SSLContext context;

	public SSLConfig() {
		this.context = null;
	}

	@Nonnull
	@Mandatory(restrictions = "context != null")
	public SSLConfig setContext(@Nonnull final SSLContext context) {
		if (context == null) {
			throw new NullPointerException("context == null");
		}
		this.context = context;
		return this;
	}

	@Nonnull
	public SSLContext getContext() {
		if (context == null) {
			throw new IllegalStateException("SSL context undefined");
		}
		return context;
	}
}
