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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.api.lang.BinaryUnit;
import net.dsys.commons.impl.builder.Mandatory;
import net.dsys.commons.impl.builder.OptionGroup;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.limit.RateLimiter;
import net.dsys.snio.impl.codec.Codecs;
import net.dsys.snio.impl.limit.RateLimiters;

/**
 * @author Ricardo Padilha
 */
public final class ClientConfig {

	private MessageCodec codec;
	private RateLimiter limiter;

	public ClientConfig() {
		codec = null;
		limiter = RateLimiters.noRateLimit();
	}

	@Nonnull
	@Mandatory(restrictions = "codec != null")
	@OptionGroup(name = "codec", seeAlso = "setMessageLength(length)")
	public ClientConfig setMessageCodec(@Nonnull final MessageCodec codec) {
		if (codec == null) {
			throw new NullPointerException("codec == null");
		}
		this.codec = codec;
		return this;
	}

	@Nonnull
	@Mandatory(restrictions = "length > 0")
	@OptionGroup(name = "codec", seeAlso = "setMessageCodec(codec)")
	public ClientConfig setMessageLength(@Nonnegative final int length) {
		if (length < 1) {
			throw new IllegalArgumentException("length < 1");
		}
		this.codec = Codecs.getDefault(length);
		return this;
	}

	@Nonnull
	@Mandatory(restrictions = "limiter != null")
	@OptionGroup(name = "limiter", seeAlso = "setRateLimit(value, unit)")
	public ClientConfig setRateLimiter(@Nonnull final RateLimiter limiter) {
		if (limiter == null) {
			throw new NullPointerException("limiter == null");
		}
		this.limiter = limiter;
		return this;
	}

	@Nonnull
	@Mandatory(restrictions = "value >= 1 && unit != null")
	@OptionGroup(name = "limiter", seeAlso = "setRateLimiter(limiter)")
	public ClientConfig setRateLimit(@Nonnegative final long value, @Nonnull final BinaryUnit unit) {
		this.limiter = RateLimiters.limit(value, unit);
		return this;
	}

	@Nonnull
	public MessageCodec getMessageCodec() {
		if (codec == null) {
			throw new IllegalStateException("message codec undefined");
		}
		return codec;
	}

	@Nonnull
	public RateLimiter getRateLimiter() {
		if (limiter == null) {
			throw new IllegalStateException("rate limiter undefined");
		}
		return limiter;
	}

}
