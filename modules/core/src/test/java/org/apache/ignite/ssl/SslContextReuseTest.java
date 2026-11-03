/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ssl;

import java.io.File;
import java.io.FileOutputStream;
import java.security.KeyStore;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.ignite.testframework.TestCertificateAuthority.PWD;
import static org.junit.Assert.assertArrayEquals;

/** Context snapshots keep their material; consumers obtain a replacement from the factory after initialization. */
public class SslContextReuseTest extends GridCommonAbstractTest {
    /** Stores generated independently for each test. */
    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    /** A new context uses initialized TLS settings; the previous context, engines and socket factories keep their settings. */
    @Test
    public void testCreateReturnsInitializedContextSnapshot() throws Exception {
        SslContextFactory factory = fileFactory();
        String[] firstProtocols = {"TLSv1.2"};
        String[] firstCiphers = {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};

        factory.setProtocols(firstProtocols);
        factory.setCipherSuites(firstCiphers);

        SSLContext previous = factory.create();
        SSLEngine existing = previous.createSSLEngine();
        SSLSocketFactory capturedFactory = previous.getSocketFactory();
        String[] replacementProtocols = {"TLSv1.3"};
        String[] replacementCiphers = {"TLS_AES_128_GCM_SHA256"};

        factory.setProtocols(replacementProtocols);
        factory.setCipherSuites(replacementCiphers);

        SslBundle replacement = factory.init();
        SSLContext cur = factory.create();
        String host = "peer.example";
        int port = 10800;
        SSLEngine addressed = cur.createSSLEngine(host, port);
        SSLEngine fromPrevious = previous.createSSLEngine();

        assertSame(replacement.context(), cur);
        assertNotSame(previous, cur);
        assertEquals(host, addressed.getPeerHost());
        assertEquals(port, addressed.getPeerPort());
        assertArrayEquals(replacementProtocols, addressed.getEnabledProtocols());
        assertArrayEquals(replacementCiphers, addressed.getEnabledCipherSuites());
        assertArrayEquals(firstProtocols, existing.getEnabledProtocols());
        assertArrayEquals(firstCiphers, existing.getEnabledCipherSuites());
        assertArrayEquals(firstProtocols, fromPrevious.getEnabledProtocols());
        assertArrayEquals(firstCiphers, fromPrevious.getEnabledCipherSuites());

        try (SSLSocket captured = (SSLSocket)capturedFactory.createSocket();
             SSLSocket replacementSocket = (SSLSocket)cur.getSocketFactory().createSocket()) {
            assertArrayEquals(firstProtocols, captured.getEnabledProtocols());
            assertArrayEquals(firstCiphers, captured.getEnabledCipherSuites());
            assertArrayEquals(replacementProtocols, replacementSocket.getEnabledProtocols());
            assertArrayEquals(replacementCiphers, replacementSocket.getEnabledCipherSuites());
        }
    }

    /** An initialized factory serializes its configuration and initializes fresh runtime state after deserialization. */
    @Test
    public void testInitializedFactorySerializationKeepsConfiguration() throws Exception {
        SslContextFactory factory = fileFactory();
        String[] protocols = {"TLSv1.2"};

        factory.setProtocols(protocols);

        SslBundle original = factory.init();
        SslContextFactory restored = TEST_JDK_MARSHALLER.unmarshal(TEST_JDK_MARSHALLER.marshal(factory),
            getClass().getClassLoader());

        assertEquals(factory.getKeyStoreFilePath(), restored.getKeyStoreFilePath());
        assertEquals(factory.getTrustStoreFilePath(), restored.getTrustStoreFilePath());
        assertArrayEquals(protocols, restored.getProtocols());

        SSLContext shared = restored.create();

        assertNotSame(factory.create(), shared);
        assertSame(shared, restored.create());
        assertArrayEquals(protocols, shared.createSSLEngine().getEnabledProtocols());
        assertEquals(original.certificateChains(), restored.sslBundle().certificateChains());
    }

    /** Creates a file-backed factory with a generated private key and currently valid certificate chain. */
    private SslContextFactory fileFactory() throws Exception {
        KeyStore store = new TestCertificateAuthority("context-reuse-ca").keyStoreWithPrivateKey();
        File storeFile = tmp.newFile("context.jks");

        try (FileOutputStream out = new FileOutputStream(storeFile)) {
            store.store(out, PWD);
        }

        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreType(store.getType());
        factory.setKeyStoreFilePath(storeFile.getAbsolutePath());
        factory.setKeyStorePassword(PWD);
        factory.setTrustStoreType(store.getType());
        factory.setTrustStoreFilePath(storeFile.getAbsolutePath());
        factory.setTrustStorePassword(PWD);

        return factory;
    }
}
