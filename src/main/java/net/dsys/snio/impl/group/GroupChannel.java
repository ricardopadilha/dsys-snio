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

package net.dsys.snio.impl.group;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.NetworkChannel;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import net.dsys.commons.api.future.CallbackFuture;
import net.dsys.commons.api.lang.Copier;
import net.dsys.commons.impl.future.MergingCallbackFuture;
import net.dsys.snio.api.buffer.MessageBufferConsumer;
import net.dsys.snio.api.buffer.MessageBufferProducer;
import net.dsys.snio.api.channel.CloseListener;
import net.dsys.snio.api.channel.MessageChannel;
import net.dsys.snio.api.group.GroupSocketAddress;

/**
 * @author Ricardo Padilha
 */
final class GroupChannel<T> implements MessageChannel<T> {

	private final MessageBufferConsumer<T> in;
	private final ChannelFactory<T> factory;
	private final Copier<T> copier;

	private MessageChannel<T>[] channels;
	private MessageBufferProducer<T> out;
	private SocketAddress local;
	private CallbackFuture<Void> bindFuture;
	private CallbackFuture<Void> connectFuture;
	private CallbackFuture<Void> closeFuture;

	GroupChannel(@Nonnull final MessageBufferConsumer<T> in, @Nonnull final ChannelFactory<T> factory,
			@Nonnull final Copier<T> copier) {
		if (in == null) {
			throw new NullPointerException("in == null");
		}
		if (factory == null) {
			throw new NullPointerException("factory == null");
		}
		if (copier == null) {
			throw new NullPointerException("copier == null");
		}
		this.in = in;
		this.factory = factory;
		this.copier = copier;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageChannel<T> onClose(final CloseListener<T> listener) {
		for (final MessageChannel<T> channel : channels) {
			channel.onClose(listener);
		}
		return this;
	}

	void open(@Nonnegative final int size) throws IOException {
		if (channels != null) {
			return;
		}
		@SuppressWarnings("unchecked")
		final MessageChannel<T>[] channels = new MessageChannel[size];
		for (int i = 0; i < size; i++) {
			channels[i] = factory.open();
		}
		this.channels = channels;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isOpen() {
		if (channels == null) {
			return false;
		}
		boolean open = true;
		for (final MessageChannel<T> channel : channels) {
			open &= channel.isOpen();
		}
		return open;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GroupChannel<T> bind(final SocketAddress local) throws IOException {
		if (!(local instanceof GroupSocketAddress)) {
			throw new IllegalArgumentException("GroupChannel can only bind to a GroupSocketAddress");
		}
		final GroupSocketAddress group = (GroupSocketAddress) local;
		if (group.size() != channels.length) {
			throw new IllegalArgumentException("local.size() != channels.length");
		}
		final int k = channels.length;
		for (int i = 0; i < k; i++) {
			channels[i].bind(group.get(i));
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkChannel bind(final SocketAddress local, final int backlog) throws IOException {
		if (!(local instanceof GroupSocketAddress)) {
			throw new IllegalArgumentException("GroupChannel can only bind to a GroupSocketAddress");
		}
		final GroupSocketAddress group = (GroupSocketAddress) local;
		if (group.size() != channels.length) {
			throw new IllegalArgumentException("local.size() != channels.length");
		}
		final int k = channels.length;
		for (int i = 0; i < k; i++) {
			channels[i].bind(group.get(i), backlog);
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CallbackFuture<Void> getBindFuture() {
		if (bindFuture == null) {
			final MergingCallbackFuture.Builder<Void> builder = MergingCallbackFuture.builder();
			for (final MessageChannel<T> channel : channels) {
				builder.add(channel.getBindFuture());
			}
			this.bindFuture = builder.build();
		}
		return bindFuture;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SocketAddress getLocalAddress() throws IOException {
		if (local == null) {
			final GroupSocketAddress.Builder builder = GroupSocketAddress.build();
			for (final MessageChannel<T> channel : channels) {
				builder.add(channel.getLocalAddress());
			}
			local = builder.build();
		}
		return local;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void connect(final SocketAddress remote) throws IOException {
		if (!(remote instanceof GroupSocketAddress)) {
			throw new IllegalArgumentException("!(remote instanceof GroupSocketAddress)");
		}
		final GroupSocketAddress group = (GroupSocketAddress) remote;
		if (group.size() != channels.length) {
			throw new IllegalArgumentException("remote.size() != channels.length");
		}
		final int k = channels.length;
		for (int i = 0; i < k; i++) {
			channels[i].connect(group.get(i));
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CallbackFuture<Void> getConnectFuture() {
		if (connectFuture == null) {
			final MergingCallbackFuture.Builder<Void> builder = MergingCallbackFuture.builder();
			for (final MessageChannel<T> channel : channels) {
				builder.add(channel.getConnectFuture());
			}
			this.connectFuture = builder.build();
		}
		return connectFuture;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageBufferConsumer<T> getInputBuffer() {
		return in;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MessageBufferProducer<T> getOutputBuffer() {
		if (out == null) {
			final GroupMessageBufferProducer.Builder<T> builder = GroupMessageBufferProducer.build();
			builder.setCopier(copier);
			for (final MessageChannel<T> channel : channels) {
				builder.add(channel.getOutputBuffer());
			}
			out = builder.build();
		}
		return out;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		if (channels == null) {
			return;
		}
		for (final MessageChannel<T> channel : channels) {
			channel.close();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CallbackFuture<Void> getCloseFuture() {
		if (closeFuture == null) {
			final MergingCallbackFuture.Builder<Void> builder = MergingCallbackFuture.builder();
			for (final MessageChannel<T> channel : channels) {
				builder.add(channel.getCloseFuture());
			}
			this.closeFuture = builder.build();
		}
		return closeFuture;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<SocketOption<?>> supportedOptions() {
		final Set<SocketOption<?>> options = new HashSet<>();
		for (final MessageChannel<T> channel : channels) {
			options.addAll(channel.supportedOptions());
		}
		return options;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <E> GroupChannel<T> setOption(final SocketOption<E> name, final E value) throws IOException {
		for (final MessageChannel<T> channel : channels) {
			channel.setOption(name, value);
		}
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <E> E getOption(final SocketOption<E> name) throws IOException {
		for (final MessageChannel<T> channel : channels) {
			final E value = channel.getOption(name);
			if (value != null) {
				return value;
			}
		}
		return null;
	}

}
