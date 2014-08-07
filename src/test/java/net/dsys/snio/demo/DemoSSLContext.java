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
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * @author Ricardo Padilha
 */
public final class DemoSSLContext {

	private DemoSSLContext() {
		// no instantiation
	}

	public static SSLContext getDemoContext() throws KeyStoreException, NoSuchAlgorithmException, CertificateException,
			UnrecoverableKeyException, KeyManagementException, IOException {
		final char[] password = "password".toCharArray();

		// First initialize the key and trust material.
		final KeyStore ksKeys = KeyStore.getInstance("JKS");
		try (final InputStream in = SSLEchoClient.class.getResourceAsStream("nodes.jks")) {
			ksKeys.load(in, password);
		}

		final KeyStore ksTrust = KeyStore.getInstance("JKS");
		try (final InputStream in = SSLEchoClient.class.getResourceAsStream("nodes.jks")) {
			ksTrust.load(in, password);
		}

		// KeyManager's decide which key material to use.
		final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
		kmf.init(ksKeys, password);

		// TrustManager's decide whether to allow connections.
		final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
		tmf.init(ksTrust);

		final SSLContext context = SSLContext.getInstance("TLS");
		context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
		return context;
	}
}
