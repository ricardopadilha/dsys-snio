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
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Ricardo Padilha
 */
public final class RefOnewayClient {

	private RefOnewayClient() {
		return;
	}

	public static void main(final String[] args) throws IOException, InterruptedException {
		final int length = Integer.parseInt(getArg("length", "65535", args));
		final String host = getArg("host", "localhost", args);
		final int port = Integer.parseInt(getArg("port", "12345", args));

		final SocketChannel client = SocketChannel.open();
		client.configureBlocking(true);
		client.connect(new InetSocketAddress(host, port));

		final ExecutorService executor = Executors.newCachedThreadPool(); // unbounded!
		executor.submit(createClient(client, length));

		synchronized (executor) {
			executor.wait();
		}
		executor.shutdown();
	}

	private static Runnable createClient(final SocketChannel client, final int length) {
		return new Runnable() {
			private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(length);
			@Override
			public void run() {
				try {
					while (!Thread.interrupted()) {
						client.write(sendBuffer);
						sendBuffer.clear();
					}
				} catch (final IOException e) {
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
		for (int i = 0, k = args.length - 1; i < k; i++) {
			if (key.equals(args[i])) {
				return args[i + 1];
			}
		}
		return defaultValue;
	}

}
