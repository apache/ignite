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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests hot reload of an {@link SslContextFactory}: {@link AbstractSslContextFactory#reload()} must re-read the
 * key store from disk and start serving the updated certificate, while keeping the cache semantics of
 * {@link AbstractSslContextFactory#create()}.
 */
public class SslContextFactoryReloadTest extends GridCommonAbstractTest {
    /** Executor for the accepting side of the loopback handshake. */
    private ExecutorService exec;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        exec = Executors.newSingleThreadExecutor();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (exec != null)
            exec.shutdownNow();
    }

    /**
     * Points a factory at a key store file, then overwrites that file with a different key store and reloads the
     * factory. The certificate served over a real TLS handshake must change, while {@code create()} keeps returning
     * the currently active (cached) context.
     */
    @Test
    public void testReloadServesNewCertificate() throws Exception {
        Path keyStore = Files.createTempFile("advui-reload-", ".jks");

        try {
            copyKeyStore("node01", keyStore);

            SslContextFactory factory = factory(keyStore);

            SSLContext ctx1 = factory.create();

            // create() must cache the context.
            assertSame(ctx1, factory.create());

            X509Certificate cert1 = serverCertificate(ctx1);

            copyKeyStore("node02", keyStore);

            SSLContext ctx2 = factory.reload();

            assertNotSame("reload() must build a new context", ctx1, ctx2);

            // create() must return the reloaded context.
            assertSame(ctx2, factory.create());

            X509Certificate cert2 = serverCertificate(ctx2);

            assertFalse(
                "Reloaded context must serve a different certificate",
                Arrays.equals(cert1.getEncoded(), cert2.getEncoded()));
        }
        finally {
            Files.deleteIfExists(keyStore);
        }
    }

    /**
     * {@link AbstractSslContextFactory#reload(javax.cache.configuration.Factory)} must reload our own factories and
     * return a freshly built context.
     */
    @Test
    public void testStaticReloadHelper() throws Exception {
        Path keyStore = Files.createTempFile("advui-reload-", ".jks");

        try {
            copyKeyStore("node01", keyStore);

            SslContextFactory factory = factory(keyStore);

            SSLContext ctx1 = factory.create();

            SSLContext ctx2 = AbstractSslContextFactory.reload(factory);

            assertNotSame(ctx1, ctx2);
            assertSame(ctx2, factory.create());
        }
        finally {
            Files.deleteIfExists(keyStore);
        }
    }

    /**
     * @param keyStore Key store file the factory reads its certificate from.
     * @return Factory with a disabled trust manager (only the key side is exercised in this test).
     */
    private SslContextFactory factory(Path keyStore) {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /**
     * @param name Test key store name (see {@code tests.properties}).
     * @param dest Destination file.
     */
    private void copyKeyStore(String name, Path dest) throws Exception {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), dest, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Performs a loopback TLS handshake using the given context on the server side and a trust-all context on the
     * client side, and returns the certificate the server presented.
     *
     * @param srvCtx Server SSL context under test.
     * @return Server certificate seen by the client.
     */
    private X509Certificate serverCertificate(SSLContext srvCtx) throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();

        try (SSLServerSocket srvSock = (SSLServerSocket)srvCtx.getServerSocketFactory()
            .createServerSocket(0, 0, loopback)) {

            Future<Void> accepted = exec.submit((Callable<Void>)() -> {
                try (SSLSocket s = (SSLSocket)srvSock.accept()) {
                    s.startHandshake();

                    // Read one byte to let the client finish the handshake and drive the exchange.
                    s.getInputStream().read();
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
        TrustManager trustAll = new X509TrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] chain, String authType) {
                // No-op.
            }

            @Override public void checkServerTrusted(X509Certificate[] chain, String authType) {
                // No-op.
            }

            @Override public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };

        SSLContext ctx = SSLContext.getInstance("TLS");

        ctx.init(null, new TrustManager[] {trustAll}, null);

        return ctx;
    }
}
