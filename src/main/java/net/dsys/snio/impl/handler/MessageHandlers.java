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

package net.dsys.snio.impl.handler;

import static net.dsys.snio.impl.handler.ExecutionType.DECOUPLED;
import static net.dsys.snio.impl.handler.ExecutionType.ZERO_COPY;
import static net.dsys.snio.impl.handler.HandlerType.MULTI_THREADED;
import static net.dsys.snio.impl.handler.HandlerType.SINGLE_THREADED;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import net.dsys.commons.api.exception.Bug;
import net.dsys.commons.api.lang.Cleaner;
import net.dsys.commons.api.lang.Copier;
import net.dsys.commons.api.lang.Factory;
import net.dsys.commons.api.lang.Interruptible;
import net.dsys.commons.impl.builder.Mandatory;
import net.dsys.commons.impl.builder.OptionGroup;
import net.dsys.commons.impl.builder.Optional;
import net.dsys.commons.impl.lang.ByteBufferCopier;
import net.dsys.commons.impl.lang.ByteBufferFactory;
import net.dsys.commons.impl.lang.DaemonThreadFactory;
import net.dsys.commons.impl.lang.DirectByteBufferFactory;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.channel.AcceptListener;
import net.dsys.snio.api.handler.MessageConsumer;
import net.dsys.snio.api.handler.MessageConsumerFactory;
import net.dsys.snio.api.handler.MessageHandler;
import net.dsys.snio.api.handler.MessageProducer;

/**
 * @author Ricardo Padilha
 */
public final class MessageHandlers {

	private MessageHandlers() {
		// no instantiation
		return;
	}

	@Nonnull
	public static <T> Interruptible syncConsumer(@Nonnull final MessageBufferConsumer<T> in,
			@Nonnull final MessageConsumer<T> consumer) {
		return new ConsumerThread<>(in, consumer);
	}

	@Nonnull
	public static <T> Interruptible asyncConsumer(@Nonnull final MessageBufferConsumer<T> in,
			@Nonnull final MessageConsumer<T> consumer,
			@Nonnull final T holder, @Nonnull final Copier<T> copier, @Nonnull final Cleaner<T> cleaner) {
		return new ConsumerThread<>(in, consumer, holder, copier, cleaner);
	}

	@Nonnull
	public static <T> Interruptible syncProducer(@Nonnull final MessageBufferProducer<T> out,
			@Nonnull final MessageProducer<T> producer) {
		return new ProducerThread<>(out, producer);
	}

	@Nonnull
	public static <T> Interruptible asyncProducer(@Nonnull final MessageBufferProducer<T> out,
			@Nonnull final MessageProducer<T> producer,
			@Nonnull final T holder, @Nonnull final Copier<T> copier, @Nonnull final Cleaner<T> cleaner) {
		return new ProducerThread<>(out, producer, holder, copier, cleaner);
	}

	@Nonnull
	public static HandlerBuilder buildHandler() {
		return new HandlerBuilder();
	}

	/**
	 * @author Ricardo Padilha
	 */
	@ParametersAreNonnullByDefault
	public static final class HandlerBuilder {

		private static AtomicInteger counter = new AtomicInteger();

		private String name;
		private HandlerType handlerType;
		private ExecutionType threadType;
		private ExecutorService executor;
		private MessageConsumer<ByteBuffer> consumer;
		private MessageConsumerFactory<ByteBuffer> consumerFactory;
		private AcceptListener<ByteBuffer> delegate;
		private int length;
		private boolean useDirectBuffer;

		HandlerBuilder() {
			this.name = "MessageHandler-" + counter.getAndIncrement();
			this.handlerType = null;
			this.threadType = ZERO_COPY;
			this.consumer = null;
			this.consumerFactory = null;
			this.delegate = null;
			this.length = 0;
			this.useDirectBuffer = false;
		}

		@Optional(defaultValue = "MessageHandler-#", restrictions = "name != null")
		@OptionGroup(name = "executor", seeAlso = "setExecutor(executor)")
		public HandlerBuilder setName(final String name) {
			if (name == null) {
				throw new NullPointerException("name == null");
			}
			this.name = name;
			return this;
		}

		@Optional(defaultValue = "Executors.newCachedThreadPool(new DaemonThreadFactory(name))",
				restrictions = "executor != null")
		@OptionGroup(name = "executor", seeAlso = "setName(name)")
		public HandlerBuilder setExecutor(final ExecutorService executor) {
			if (executor == null) {
				throw new NullPointerException("executor == null");
			}
			this.executor = executor;
			return this;
		}

		@Mandatory(restrictions = "consumer != null")
		@OptionGroup(name = "consumer", seeAlso = "useManyConsumers(factory)")
		public HandlerBuilder useSingleConsumer(final MessageConsumer<ByteBuffer> consumer) {
			if (consumer == null) {
				throw new NullPointerException("consumer == null");
			}
			this.handlerType = SINGLE_THREADED;
			this.consumer = consumer;
			this.consumerFactory = null;
			return this;
		}

		@Mandatory(restrictions = "factory != null")
		@OptionGroup(name = "consumer", seeAlso = "useSingleConsumer(consumer)")
		public HandlerBuilder useManyConsumers(final MessageConsumerFactory<ByteBuffer> factory) {
			if (factory == null) {
				throw new NullPointerException("consumerFactory == null");
			}
			this.handlerType = MULTI_THREADED;
			this.consumerFactory = factory;
			this.consumer = null;
			return this;
		}

		@Optional(defaultValue = "useZeroCopyProcessing()", restrictions = "none")
		@OptionGroup(name = "execution", seeAlso = "useDecoupledProcessing(length)")
		public HandlerBuilder useZeroCopyProcessing() {
			this.threadType = ZERO_COPY;
			this.length = 0;
			return this;
		}

		@Optional(defaultValue = "useZeroCopyProcessing()", restrictions = "messageLength > 0")
		@OptionGroup(name = "execution", seeAlso = "useZeroCopyProcessing()")
		public HandlerBuilder useDecoupledProcessing(@Nonnegative final int messageLength) {
			this.threadType = DECOUPLED;
			this.length = messageLength;
			return this;
		}

		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useHeapBuffer()")
		public HandlerBuilder useDirectBuffer() {
			this.useDirectBuffer = true;
			return this;
		}

		@Optional(defaultValue = "useHeapBuffer()")
		@OptionGroup(name = "bufferType", seeAlso = "useDirectBuffer()")
		public HandlerBuilder useHeapBuffer() {
			this.useDirectBuffer = false;
			return this;
		}

		@Optional(defaultValue = "null")
		public HandlerBuilder setDelegate(final AcceptListener<ByteBuffer> delegate) {
			this.delegate = delegate;
			return this;
		}

		public MessageHandler<ByteBuffer> build() {
			ExecutorService exec = executor;
			if (exec == null) {
				exec = Executors.newCachedThreadPool(new DaemonThreadFactory(name));
			}
			final ConsumerThreadFactory<ByteBuffer> threads;
			switch (threadType) {
			case DECOUPLED: {
				final Factory<ByteBuffer> factory;
				if (useDirectBuffer) {
					factory = new DirectByteBufferFactory(length);
				} else {
					factory = new ByteBufferFactory(length);
				}
				final ByteBufferCopier copier = new ByteBufferCopier();
				threads = ConsumerThread.createAsyncFactory(factory, copier, copier);
				break;
			}
			case ZERO_COPY: {
				threads = ConsumerThread.createSyncFactory();
				break;
			}
			default: {
				throw new Bug("Unsupported ThreadType: " + threadType);
			}
			}
			final MessageHandler<ByteBuffer> handler;
			switch (handlerType) {
			case MULTI_THREADED: {
				handler = new MessageHandlerImpl<>(exec, threads, consumerFactory, delegate);
				break;
			}
			case SINGLE_THREADED: {
				handler = new MessageHandlerImpl<>(exec, threads, consumer, delegate);
				break;
			}
			default: {
				throw new Bug("Unsupported HandlerType: " + handlerType);
			}
			}
			return handler;
		}
	}
}
