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
import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.impl.builder.Mandatory;
import net.dsys.commons.impl.builder.OptionGroup;
import net.dsys.snio.api.codec.MessageCodec;
import net.dsys.snio.api.limit.RateLimiter;
import net.dsys.snio.impl.codec.Codecs;
import net.dsys.snio.impl.limit.RateLimiters;

/**
 * @author Ricardo Padilha
 */
public final class ServerConfig {

	private Factory<MessageCodec> codecs;
	private Factory<RateLimiter> limiters;

	public ServerConfig() {
		codecs = null;
		limiters = RateLimiters.noLimitFactory();
	}

	@Nonnull
	@Mandatory(restrictions = "codecs != null")
	@OptionGroup(name = "codec", seeAlso = "setMessageLength(length)")
	public ServerConfig setMessageCodec(@Nonnull final Factory<MessageCodec> codecs) {
		if (codecs == null) {
			throw new NullPointerException("codecs == null");
		}
		this.codecs = codecs;
		return this;
	}

	@Nonnull
	@Mandatory(restrictions = "length > 0")
	@OptionGroup(name = "codec", seeAlso = "setMessageCodec(codecs)")
	public ServerConfig setMessageLength(@Nonnegative final int length) {
		if (length < 1) {
			throw new IllegalArgumentException("length < 1");
		}
		this.codecs = Codecs.getDefaultFactory(length);
		return this;
	}

	@Nonnull
	@Mandatory(restrictions = "limiters != null")
	@OptionGroup(name = "limiter", seeAlso = "setRateLimit(value, unit)")
	public ServerConfig setRateLimiter(@Nonnull final Factory<RateLimiter> limiters) {
		if (limiters == null) {
			throw new NullPointerException("limiters == null");
		}
		this.limiters = limiters;
		return this;
	}

	@Nonnull
	@Mandatory(restrictions = "value >= 1 && unit != null")
	@OptionGroup(name = "limiter", seeAlso = "setRateLimiter(limiters)")
	public ServerConfig setRateLimit(@Nonnegative final long value, @Nonnull final BinaryUnit unit) {
		this.limiters = RateLimiters.limitFactory(value, unit);
		return this;
	}

	@Nonnull
	public Factory<MessageCodec> getMessageCodecs() {
		if (codecs == null) {
			throw new IllegalStateException("message codec factory undefined");
		}
		return codecs;
	}

	@Nonnull
	public Factory<RateLimiter> getRateLimiters() {
		if (limiters == null) {
			throw new IllegalStateException("rate limiter factory undefined");
		}
		return limiters;
	}

}
