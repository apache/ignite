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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.ssl.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.http.HttpException;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpTester;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.util.BufferUtil;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyRestProtocol.DFLT_JETTY_PORT;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;
import static org.apache.ignite.ssl.TestCertificateAuthority.trustStoreOf;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * HTTPS of HTTP REST with {@link ConnectorConfiguration#getHttpSslFactory()}.
 * Server certificates are issued for {@code localhost}, and clients connect to {@code 127.0.0.1}.
 */
@WithSystemProperty(key = IGNITE_JETTY_PORT, value = DFLT_JETTY_PORT)
public class JettyRestHttpsTest extends GridCommonAbstractTest {
    /** Upper bound for waiting for an event that must happen. */
    private static final long TIMEOUT_MS = 30_000;

    /** */
    private static final int PORT = Integer.parseInt(DFLT_JETTY_PORT);

    /** Test property read by the HTTPS connector in the Jetty XML configuration. */
    private static final String JETTY_KEY_STORE = "ignite.test.JettyRestHttpsTest.keyStorePath";

    /** */
    private static final String JETTY_HTTPS_XML = "jetty/https.xml";

    /** */
    private final long now = System.currentTimeMillis();

    /** */
    private Path dir;

    /** */
    private TestCertificateAuthority ca1;

    /** */
    private TestCertificateAuthority ca2;

    /** Connector configuration of the next started node. */
    private ConnectorConfiguration connCfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setConnectorConfiguration(connCfg);
    }

    /**
     * Each test gets two test CAs and a temporary directory for the stores of its server factory.
     * The CA certificates are valid from a day before to a year after the test start.
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        dir = Files.createTempDirectory("jetty-rest-https");
        ca1 = new TestCertificateAuthority("ca1", now - DAYS.toMillis(1), now + DAYS.toMillis(365));
        ca2 = new TestCertificateAuthority("ca2", now - DAYS.toMillis(1), now + DAYS.toMillis(365));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        U.delete(dir);

        super.afterTest();
    }

    /**
     * Given: the built-in HTTP connector and an HTTP SSL factory.
     * When: the node starts, and clients connect over HTTPS and over plain HTTP.
     * Then: the HTTPS client gets the factory certificate and a response, the plain HTTP client gets no response,
     *     and HTTP REST is registered for TLS reload.
     */
    @Test
    public void testBuiltInConnectorServesHttpsFromFactory() throws Exception {
        SslContextFactory factory = serverFactory(ca1, ca1);

        connCfg = new ConnectorConfiguration().setHttpSslFactory(factory);

        IgniteEx node = startGrid(0);

        try (HttpsConnection conn = new HttpsConnection(client(), "localhost")) {
            assertEquals(200, conn.version());
            assertEquals(ca1.subject(), conn.serverIssuer());
        }

        assertThrows(log, () -> plainHttpVersion(), IOException.class, null);
        assertTrue(node.context().ssl().factories().contains(Map.entry(factory, "HTTP REST")));
    }

    /**
     * Given: an HTTP SSL factory, and the built-in connector, which leaves the Host check of Jetty enabled.
     * When: a client sends a request with a Host that the server certificate does not name.
     * Then: the server rejects the request with status 400.
     */
    @Test
    public void testHostMustMatchServerCertificate() throws Exception {
        connCfg = new ConnectorConfiguration().setHttpSslFactory(serverFactory(ca1, ca1));

        startGrid(0);

        try (HttpsConnection conn = new HttpsConnection(client(), "127.0.0.1")) {
            assertEquals(400, conn.version());
        }
    }

    /**
     * Given: a Jetty XML connector for plain HTTP whose secure request customizer does not check the Host,
     *     and an HTTP SSL factory.
     * When: a client connects over HTTPS with a Host that the server certificate does not name.
     * Then: the client gets the factory certificate and a response.
     */
    @Test
    public void testJettyXmlHttpConnectorServesHttpsFromFactory() throws Exception {
        connCfg = new ConnectorConfiguration()
            .setJettyPath("jetty/http-customizer.xml")
            .setHttpSslFactory(serverFactory(ca1, ca1));

        startGrid(0);

        try (HttpsConnection conn = new HttpsConnection(client(), "127.0.0.1")) {
            assertEquals(200, conn.version());
            assertEquals(ca1.subject(), conn.serverIssuer());
        }
    }

    /**
     * Given: HTTP REST requires client certificates. The server has a certificate of CA 1 and trusts only CA 1.
     *     A client with a CA 1 certificate keeps a connection open.
     * When: a reload fails on a missing key store. Then the server gets a certificate of CA 2, trusts both CAs,
     *     and reloads.
     * Then: until the successful reload, new connections get the CA 1 certificate, and a client of CA 2 is rejected.
     *     After it, a client of CA 2 connects and gets the CA 2 certificate, and the open connection keeps working.
     */
    @Test
    public void testReloadRotatesKeysAndTrustForNewConnections() throws Exception {
        SslContextFactory factory = serverFactory(ca1, ca1);

        connCfg = new ConnectorConfiguration().setHttpSslFactory(factory).setHttpSslClientAuth(true);

        IgniteEx node = startGrid(0);

        SSLContext client1 = client(ca1.keyManagers("client1", now - HOURS.toMillis(1), now + DAYS.toMillis(30)));
        SSLContext client2 = client(ca2.keyManagers("client2", now - HOURS.toMillis(1), now + DAYS.toMillis(30)));

        try (HttpsConnection established = new HttpsConnection(client1, "localhost")) {
            assertEquals(200, established.version());
            assertThrows(log, () -> issuerOfNewConnection(client2), IOException.class, null);

            Files.delete(Path.of(factory.getKeyStoreFilePath()));

            assertThrows(log, () -> {
                node.context().ssl().reload(factory);

                return null;
            }, Exception.class, null);
            assertEquals(ca1.subject(), issuerOfNewConnection(client1));

            save(ca2.keyStore("localhost", now - HOURS.toMillis(1), now + DAYS.toMillis(30)), Path.of(factory.getKeyStoreFilePath()));
            save(trustStoreOf(ca1, ca2), Path.of(factory.getTrustStoreFilePath()));
            node.context().ssl().reload(factory);

            try (HttpsConnection conn = new HttpsConnection(client2, "localhost")) {
                assertEquals(200, conn.version());
                assertEquals(ca2.subject(), conn.serverIssuer());
            }

            assertEquals(200, established.version());
            assertEquals(ca1.subject(), established.serverIssuer());
        }
    }

    /**
     * Given: an HTTP SSL factory with hot reload enabled has loaded a certificate that expires in a day,
     *     so hot reload with a lead time of two days is due.
     *     The startup check of the node blocks while it loads the key store.
     * When: a client opens a connection during the load, the key store gets a fresh certificate, and the load finishes.
     * Then: the connection opened during the load gets the previous certificate and keeps working after the load.
     *     A new connection gets the fresh certificate.
     */
    @Test
    public void testScheduledRenewalKeepsEstablishedConnections() throws Exception {
        BlockingReloadFactory factory = new BlockingReloadFactory();

        configure(factory, ca1, ca1);
        save(ca1.keyStore("localhost", now - HOURS.toMillis(1), now + DAYS.toMillis(1)), Path.of(factory.getKeyStoreFilePath()));
        factory.setHotReloadEnabled(true);
        factory.setHotReloadBeforeExpiryMs(DAYS.toMillis(2));

        connCfg = new ConnectorConfiguration().setHttpSslFactory(factory);

        startGrid(0);

        assertTrue(factory.reloadStarted.await(TIMEOUT_MS, MILLISECONDS));

        SSLContext client = client();

        try (HttpsConnection established = new HttpsConnection(client, "localhost")) {
            assertEquals(200, established.version());

            save(ca2.keyStore("localhost", now - HOURS.toMillis(1), now + DAYS.toMillis(30)), Path.of(factory.getKeyStoreFilePath()));
            factory.finishReload.countDown();

            assertTrue(waitForCondition(() -> factory.getHotReloadTime() > System.currentTimeMillis(), TIMEOUT_MS));
            assertEquals(ca2.subject(), issuerOfNewConnection(client));

            assertEquals(200, established.version());
            assertEquals(ca1.subject(), established.serverIssuer());
        }
    }

    /**
     * Given: an HTTP SSL factory that enables only TLS 1.2 and a cipher suite that Jetty excludes by default.
     * When: clients that offer only that protocol and cipher suite, another cipher suite or another protocol connect.
     * Then: the first connection uses that protocol and cipher suite, and the others fail.
     */
    @Test
    public void testFactoryProtocolsAndCipherSuitesApplyExactly() throws Exception {
        String suite = "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA";
        SslContextFactory factory = serverFactory(ca1, ca1);

        factory.setProtocols("TLSv1.2");
        factory.setCipherSuites(suite);

        connCfg = new ConnectorConfiguration().setHttpSslFactory(factory);

        startGrid(0);

        try (HttpsConnection conn = new HttpsConnection(client(), "localhost", "TLSv1.2", suite)) {
            assertEquals(200, conn.version());
            assertEquals("TLSv1.2", conn.sock.getSession().getProtocol());
            assertEquals(suite, conn.sock.getSession().getCipherSuite());
        }

        assertThrows(log, () -> version("TLSv1.2", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"), IOException.class, null);
        assertThrows(log, () -> version("TLSv1.3", "TLS_AES_128_GCM_SHA256"), IOException.class, null);
    }

    /** Sends the version command from a client without a certificate that offers only the given protocol and suite. */
    private int version(String protocol, String suite) throws Exception {
        try (HttpsConnection conn = new HttpsConnection(client(), "localhost", protocol, suite)) {
            return conn.version();
        }
    }

    /**
     * Given: configurations that cannot get HTTPS from an HTTP SSL factory: a Jetty XML connector with its own TLS,
     *     a Jetty XML connector whose default protocol is not HTTP, and client authentication without the factory.
     * When: the node starts with each of them.
     * Then: the start fails with the reason, and nothing listens on the HTTP port.
     */
    @Test
    @WithSystemProperty(key = JETTY_KEY_STORE, value = "")
    public void testRejectedConfigurationsDoNotServeHttp() throws Exception {
        setJettyKeyStore(ca1);

        Map<ConnectorConfiguration, String> rejected = Map.of(
            new ConnectorConfiguration()
                .setJettyPath(JETTY_HTTPS_XML)
                .setHttpSslFactory(serverFactory(ca1, ca1)),
            "Jetty connector that configures TLS",
            new ConnectorConfiguration()
                .setJettyPath("jetty/proxy.xml")
                .setHttpSslFactory(serverFactory(ca1, ca1)),
            "default protocol is HTTP",
            new ConnectorConfiguration().setHttpSslClientAuth(true),
            "client authentication requires an HTTP SSL factory");

        for (Map.Entry<ConnectorConfiguration, String> e : rejected.entrySet()) {
            connCfg = e.getKey();

            Throwable err = assertThrows(log, () -> startGrid(0), Exception.class, null);

            assertTrue(X.getFullStackTrace(err), X.getFullStackTrace(err).contains(e.getValue()));
            assertThrows(log, () -> plainHttpVersion(), ConnectException.class, null);
        }
    }

    /**
     * Given: a Jetty XML connector with its own TLS and no HTTP SSL factory.
     * When: the node starts, and a client connects over HTTPS.
     * Then: the client gets the certificate of the Jetty configuration, and HTTP REST is not registered for TLS reload.
     */
    @Test
    @WithSystemProperty(key = JETTY_KEY_STORE, value = "")
    public void testJettyXmlHttpsStaysOutsideReload() throws Exception {
        setJettyKeyStore(ca2);

        connCfg = new ConnectorConfiguration().setJettyPath(JETTY_HTTPS_XML);

        IgniteEx node = startGrid(0);

        assertEquals(ca2.subject(), issuerOfNewConnection(client()));
        assertTrue(node.context().ssl().factories().isEmpty());
    }

    /**
     * Given: an HTTP SSL factory, and another process holds the only port that HTTP REST may use.
     * When: the node starts.
     * Then: the node starts without HTTP REST, and HTTP REST is not registered for TLS reload.
     */
    @Test
    public void testHttpsThatDidNotStartIsNotRegistered() throws Exception {
        connCfg = new ConnectorConfiguration().setHttpSslFactory(serverFactory(ca1, ca1)).setPortRange(1);

        try (ServerSocket ignored = new ServerSocket(PORT, 1, InetAddress.getByName("127.0.0.1"))) {
            IgniteEx node = startGrid(0);

            assertTrue(node.context().ssl().factories().isEmpty());
        }
    }

    /**
     * Creates a server factory with a {@code localhost} certificate of the first CA and a trust store with the second CA.
     * The certificate is valid for 30 days. The stores are files in the test directory.
     */
    private SslContextFactory serverFactory(TestCertificateAuthority keys, TestCertificateAuthority trust) throws Exception {
        return configure(new SslContextFactory(), keys, trust);
    }

    /** Points the factory at new key and trust store files in the test directory. */
    private SslContextFactory configure(SslContextFactory factory, TestCertificateAuthority keys, TestCertificateAuthority trust)
        throws Exception {
        Path keyStore = Files.createTempFile(dir, "keys", ".jks");
        Path trustStore = Files.createTempFile(dir, "trust", ".jks");

        save(keys.keyStore("localhost", now - HOURS.toMillis(1), now + DAYS.toMillis(30)), keyStore);
        save(trust.trustStore(), trustStore);

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(PASSWORD);
        factory.setTrustStoreFilePath(trustStore.toString());
        factory.setTrustStorePassword(PASSWORD);

        return factory;
    }

    /** Creates a client context that trusts both CAs. Omit the key managers for a client without a certificate. */
    private SSLContext client(KeyManager... keyManagers) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");

        tmf.init(trustStoreOf(ca1, ca2));

        SSLContext ctx = SSLContext.getInstance("TLS");

        ctx.init(keyManagers, tmf.getTrustManagers(), null);

        return ctx;
    }

    /** @return Issuer of the certificate that a new connection gets. */
    private static X500Principal issuerOfNewConnection(SSLContext client) throws Exception {
        try (HttpsConnection conn = new HttpsConnection(client, "localhost")) {
            assertEquals(200, conn.version());

            return conn.serverIssuer();
        }
    }

    /** Sends the version command over plain HTTP. */
    private static int plainHttpVersion() throws IOException {
        try (Socket sock = new Socket("127.0.0.1", PORT)) {
            sock.setSoTimeout((int)TIMEOUT_MS);

            return request(sock, "localhost");
        }
    }

    /**
     * Sends the version command and reads the response.
     *
     * @return Response status.
     */
    private static int request(Socket sock, String host) throws IOException {
        HttpTester.Request req = HttpTester.newRequest();

        req.setURI("/ignite?cmd=version");
        req.put(HttpHeader.HOST, host + ':' + PORT);

        BufferUtil.writeTo(req.generate(), sock.getOutputStream());

        try {
            HttpTester.Response res = HttpTester.parseResponse(sock.getInputStream());

            if (res == null)
                throw new EOFException("No HTTP response");

            assertEquals(HttpVersion.HTTP_1_1, res.getVersion());

            return res.getStatus();
        }
        catch (HttpException.RuntimeException e) {
            throw new IOException("Invalid HTTP response", e);
        }
    }

    /** Gives the Jetty XML connector a localhost certificate issued by the given CA. */
    private void setJettyKeyStore(TestCertificateAuthority ca) throws Exception {
        Path keyStore = dir.resolve("jetty-keys.jks");

        save(ca.keyStore("localhost", now - HOURS.toMillis(1), now + DAYS.toMillis(30)), keyStore);
        System.setProperty(JETTY_KEY_STORE, keyStore.toString());
    }

    /** HTTPS connection to the HTTP REST port that sends requests over one TLS session. */
    private static class HttpsConnection implements AutoCloseable {
        /** */
        private final SSLSocket sock;

        /** */
        private final String host;

        /**
         * @param client Client context.
         * @param host Host that requests name.
         * @param tls Enabled protocol and cipher suite, or nothing for the client defaults.
         */
        private HttpsConnection(SSLContext client, String host, String... tls) throws IOException {
            this.host = host;

            sock = (SSLSocket)client.getSocketFactory().createSocket("127.0.0.1", PORT);

            sock.setSoTimeout((int)TIMEOUT_MS);

            if (tls.length > 0) {
                sock.setEnabledProtocols(new String[] {tls[0]});
                sock.setEnabledCipherSuites(new String[] {tls[1]});
            }
        }

        /**
         * Sends the version command.
         *
         * @return Response status.
         */
        private int version() throws IOException {
            return request(sock, host);
        }

        /** @return Issuer of the certificate presented by the server. */
        private X500Principal serverIssuer() throws SSLException {
            return ((X509Certificate)sock.getSession().getPeerCertificates()[0]).getIssuerX500Principal();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            sock.close();
        }
    }

    /**
     * Factory whose first reload, the startup check of hot reload, waits before it reads the key store.
     * The test changes the key store meanwhile and then lets the reload finish.
     */
    private static class BlockingReloadFactory extends SslContextFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final transient AtomicInteger keyLoads = new AtomicInteger();

        /** */
        private final transient CountDownLatch reloadStarted = new CountDownLatch(1);

        /** */
        private final transient CountDownLatch finishReload = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override protected KeyStore loadKeyStore(String type, String path, char[] pwd) throws SSLException {
            if (path.equals(getKeyStoreFilePath()) && keyLoads.incrementAndGet() == 2) {
                reloadStarted.countDown();

                try {
                    finishReload.await(TIMEOUT_MS, MILLISECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new SSLException(e);
                }
            }

            return super.loadKeyStore(type, path, pwd);
        }
    }
}
