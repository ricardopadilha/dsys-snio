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

package net.dsys.snio.demo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Reference implementation for oneway tests. It takes a single blocking socket
 * and sends as much as it can, as often as it can.
 * 
 * @author Ricardo Padilha
 */
public final class RefOnewayServer {

	private RefOnewayServer() {
		return;
	}

	public static void main(final String[] args) throws IOException, InterruptedException {
		final int length = Integer.parseInt(getArg("length", "65535", args));
		final int port = Integer.parseInt(getArg("port", "12345", args));

		final ServerSocketChannel server = ServerSocketChannel.open();
		server.configureBlocking(true);
		server.bind(new InetSocketAddress(port));

		final SocketChannel channel = server.accept();
		// one thread per client
		final ExecutorService executor = Executors.newCachedThreadPool();
		executor.execute(createAppHandler(channel, length));

		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		executor.shutdown();
	}

	static Runnable createAppHandler(final SocketChannel channel, final int length) {
		return new Runnable() {
			private final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(length);
			@Override
			public void run() {
				try {
					while (!Thread.interrupted()) {
						channel.read(receiveBuffer);
						receiveBuffer.clear();
					}
				} catch (final Throwable e) {
					Thread.currentThread().interrupt();
				}
			}
		};
	}

	private static String getArg(final String name, final String defaultValue, final String[] args) {
		if (args == null || name == null) {
			return defaultValue;
		}
		final String key = "--" + name;
		final int k = args.length - 1;
		for (int i = 0; i < k; i++) {
			if (key.equals(args[i])) {
				return args[i + 1];
			}
		}
		return defaultValue;
	}
}
