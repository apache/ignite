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

import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.ssl.SslContextUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests hot reload of an {@link SslContextFactory}: {@link AbstractSslContextFactory#reload()} must re-read the
 * key store from disk and start serving the updated certificate, while keeping the cache semantics of
 * {@link AbstractSslContextFactory#create()}.
 */
public class SslContextFactoryReloadTest extends GridCommonAbstractTest {
    /** Key store file the factory reads from; replaced on disk to simulate certificate rotation. */
    private Path keyStore;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        keyStore = Files.createTempFile("ignite-ssl-reload-", ".jks");

        copyKeyStore("node01");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (keyStore != null)
            Files.deleteIfExists(keyStore);
    }

    /** Reload must re-read the key store and serve the new certificate; {@code create()} returns the active one. */
    @Test
    public void testReloadServesNewCertificate() throws Exception {
        SslContextFactory factory = reloadableFactory();

        SSLContext ctxBefore = factory.create();

        assertSame(ctxBefore, factory.create());

        X509Certificate certBefore = servedCertificate(ctxBefore);

        copyKeyStore("node02");

        SSLContext ctxAfter = factory.reload();

        assertNotSame("reload() must build a new context", ctxBefore, ctxAfter);

        assertSame(ctxAfter, factory.create());

        assertFalse(
            "Reloaded context must serve a different certificate",
            Arrays.equals(certBefore.getEncoded(), servedCertificate(ctxAfter).getEncoded()));
    }

    /** Reload must rebuild the context of our own factories and answer {@code null} if nothing changed. */
    @Test
    public void testReloadHelper() throws Exception {
        SslContextFactory factory = reloadableFactory();

        SSLContext ctxBefore = factory.create();

        SSLContext ctxAfter = SslContextUtils.reload(factory, ctxBefore);

        assertNotNull("Our own factory must be rebuilt, not reported as unchanged", ctxAfter);
        assertSame(ctxAfter, factory.create());

        // A factory that hands out the very same instance is reported as "nothing changed".
        Factory<SSLContext> caching = () -> ctxAfter;

        assertNull("Context already in use must be reported as nothing to reload",
            SslContextUtils.reload(caching, ctxAfter));

        // The same factory is a genuine reload for a caller that did not have that context yet.
        assertSame(ctxAfter, SslContextUtils.reload(caching, ctxBefore));
    }

    /** Check must read the rotated store but leave the factory handing out the context already in use. */
    @Test
    public void testCheckHelperChangesNothing() throws Exception {
        SslContextFactory factory = reloadableFactory();

        SSLContext inUse = factory.create();

        SSLContext built = SslContextUtils.build(factory, inUse);

        assertNotNull("A freshly built context must not be reported as unchanged", built);
        // The factory must keep handing out the context in use.
        assertSame(inUse, factory.create());
    }

    /**
     * @return SSL context factory reading the certificate from {@link #keyStore} and trusting any peer.
     */
    private SslContextFactory reloadableFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /**
     * @param name Test key store name (see {@code tests.properties}).
     */
    private void copyKeyStore(String name) throws Exception {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), keyStore, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * @param srvCtx Server SSL context under test.
     * @return Certificate the context presents on a new TLS connection from a trust-all client.
     */
    private X509Certificate servedCertificate(SSLContext srvCtx) throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();

        try (SSLServerSocket srvSock = (SSLServerSocket)srvCtx.getServerSocketFactory()
            .createServerSocket(0, 0, loopback)) {

            IgniteInternalFuture<?> accepted = GridTestUtils.runAsync(() -> {
                try (SSLSocket srvSide = (SSLSocket)srvSock.accept()) {
                    srvSide.startHandshake();

                    // Read one byte to let the client finish the handshake and drive the exchange.
                    srvSide.getInputStream().read();
                }

                return null;
            });

            try (SSLSocket cli = (SSLSocket)trustAllContext().getSocketFactory()
                .createSocket(loopback, srvSock.getLocalPort())) {

                cli.startHandshake();

                X509Certificate cert = (X509Certificate)cli.getSession().getPeerCertificates()[0];

                OutputStream out = cli.getOutputStream();
                out.write(1);
                out.flush();

                accepted.get();

                return cert;
            }
        }
    }

    /**
     * @return Client-side context that trusts any server certificate.
     */
    private SSLContext trustAllContext() throws Exception {
        SSLContext cliCtx = SSLContext.getInstance("TLS");

        cliCtx.init(null, new TrustManager[] {SslContextFactory.getDisabledTrustManager()}, null);

        return cliCtx;
    }
}
