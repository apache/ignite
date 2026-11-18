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
import java.net.InetSocketAddress;
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
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyRestProtocol.JettySslContextFactory;
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
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_PORT;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyRestProtocol.DFLT_JETTY_PORT;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;
import static org.apache.ignite.ssl.TestCertificateAuthority.trustStoreOf;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertArrayEquals;

/**
 * HTTPS of HTTP REST with {@link ConnectorConfiguration#getHttpSslFactory()}.
 * Server certificates are issued for {@code localhost}, and clients connect to {@code 127.0.0.1}.
 */
@WithSystemProperty(key = IGNITE_JETTY_PORT, value = DFLT_JETTY_PORT)
public class JettyRestHttpsTest extends GridCommonAbstractTest {
    /** Upper bound for waiting for an event that must happen. */
    private static final long TIMEOUT_MS = 30_000;

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
        SslContextFactory factory = serverFactory();

        connCfg = new ConnectorConfiguration().setHttpSslFactory(factory);

        IgniteEx node = startGrid(0);

        assertEquals(ca1.subject(), issuerOfNewConnection(client()));
        assertThrows(log, () -> plainHttpVersion(node.localNode().attribute(ATTR_REST_JETTY_PORT)), IOException.class, null);
        assertTrue(node.context().ssl().factories().contains(Map.entry(factory, "HTTP REST")));
    }

    /**
     * Given: an HTTP SSL factory, and the built-in connector, which leaves the Host check of Jetty enabled.
     * When: a client sends a request with a Host that the server certificate does not name.
     * Then: the server rejects the request with status 400.
     */
    @Test
    public void testHostMustMatchServerCertificate() throws Exception {
        connCfg = new ConnectorConfiguration().setHttpSslFactory(serverFactory());

        startGrid(0);

        try (SSLSocket sock = connect(client())) {
            assertEquals(400, request(sock, "127.0.0.1"));
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
            .setHttpSslFactory(serverFactory());

        startGrid(0);

        try (SSLSocket sock = connect(client())) {
            assertEquals(200, request(sock, "127.0.0.1"));
            assertEquals(ca1.subject(), serverIssuer(sock));
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
        SslContextFactory factory = serverFactory();

        connCfg = new ConnectorConfiguration().setHttpSslFactory(factory).setHttpSslClientAuth(true);

        IgniteEx node = startGrid(0);

        SSLContext client1 = clientWithCertificate(ca1);
        SSLContext client2 = clientWithCertificate(ca2);

        try (SSLSocket established = connect(client1)) {
            assertEquals(200, request(established, "localhost"));
            assertThrows(log, () -> issuerOfNewConnection(client2), SSLException.class, null);

            Files.delete(Path.of(factory.getKeyStoreFilePath()));

            assertThrows(log, () -> {
                node.context().ssl().reload(factory);

                return null;
            }, Exception.class, null);
            assertEquals(ca1.subject(), issuerOfNewConnection(client1));

            save(serverKeys(ca2, 30), Path.of(factory.getKeyStoreFilePath()));
            save(trustStoreOf(ca1, ca2), Path.of(factory.getTrustStoreFilePath()));
            node.context().ssl().reload(factory);

            assertEquals(ca2.subject(), issuerOfNewConnection(client2));

            assertEquals(200, request(established, "localhost"));
            assertEquals(ca1.subject(), serverIssuer(established));
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

        configureServer(factory, 1);
        factory.setHotReloadEnabled(true);
        factory.setHotReloadBeforeExpiryMs(DAYS.toMillis(2));

        connCfg = new ConnectorConfiguration().setHttpSslFactory(factory);

        startGrid(0);

        assertTrue(factory.reloadStarted.await(TIMEOUT_MS, MILLISECONDS));

        SSLContext client = client();

        try (SSLSocket established = connect(client)) {
            assertEquals(200, request(established, "localhost"));

            save(serverKeys(ca2, 30), Path.of(factory.getKeyStoreFilePath()));
            factory.finishReload.countDown();

            assertTrue(waitForCondition(() -> factory.getHotReloadTime() > System.currentTimeMillis(), TIMEOUT_MS));
            assertEquals(ca2.subject(), issuerOfNewConnection(client));

            assertEquals(200, request(established, "localhost"));
            assertEquals(ca1.subject(), serverIssuer(established));
        }
    }

    /**
     * Given: the factory enables only TLS 1.2, a TLS 1.2 suite Jetty normally excludes, and a TLS 1.3 suite.
     * When: clients offer the configured TLS 1.2 suite, another TLS 1.2 suite, or TLS 1.3 with its configured suite.
     * Then: only the first client connects.
     */
    @Test
    public void testFactoryProtocolsAndCipherSuitesApplyExactly() throws Exception {
        String suite = "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA";
        String tls13Suite = "TLS_AES_128_GCM_SHA256";
        SslContextFactory factory = serverFactory();

        factory.setProtocols("TLSv1.2");
        factory.setCipherSuites(suite, tls13Suite);

        connCfg = new ConnectorConfiguration().setHttpSslFactory(factory);

        startGrid(0);

        SSLSession ses = requestWithTls("TLSv1.2", suite);

        assertEquals("TLSv1.2", ses.getProtocol());
        assertEquals(suite, ses.getCipherSuite());

        assertThrows(log, () -> requestWithTls("TLSv1.2", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"), SSLException.class, null);
        assertThrows(log, () -> requestWithTls("TLSv1.3", tls13Suite), SSLException.class, null);
    }

    /**
     * Given: the factory enables no protocols or cipher suites, and Jetty requires a client certificate.
     * When: the factory reloads with a protocol and cipher enabled.
     * Then: each engine keeps the settings from the context that created it and requires a client certificate.
     */
    @Test
    public void testNewEngineUsesReloadedTlsSettings() throws Exception {
        SslContextFactory factory = serverFactory();

        factory.setProtocols();
        factory.setCipherSuites();

        JettySslContextFactory jetty = new JettySslContextFactory(factory, true);
        InetSocketAddress peer = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

        try {
            jetty.start();

            SSLEngine first = jetty.newSSLEngine(peer);

            factory.setProtocols("TLSv1.3");
            factory.setCipherSuites("TLS_AES_128_GCM_SHA256");
            factory.init();

            SSLEngine reloaded = jetty.newSSLEngine(peer);

            assertEquals(0, first.getEnabledProtocols().length);
            assertEquals(0, first.getEnabledCipherSuites().length);
            assertTrue(first.getNeedClientAuth());
            assertArrayEquals(factory.getProtocols(), reloaded.getEnabledProtocols());
            assertArrayEquals(factory.getCipherSuites(), reloaded.getEnabledCipherSuites());
            assertTrue(reloaded.getNeedClientAuth());
        }
        finally {
            jetty.stop();
        }
    }

    /**
     * Given: configurations that cannot get HTTPS from an HTTP SSL factory: a Jetty XML connector with its own TLS,
     *     a Jetty XML connector whose default protocol is not HTTP, and client authentication without the factory.
     * When: the node starts with each of them.
     * Then: the start fails with the reason, and nothing listens on the HTTP port.
     */
    @Test
    @WithSystemProperty(key = IGNITE_JETTY_PORT, value = "")
    @WithSystemProperty(key = JETTY_KEY_STORE, value = "")
    public void testRejectedConfigurationsDoNotServeHttp() throws Exception {
        setJettyKeyStore();

        SslContextFactory factory = serverFactory();

        Map<ConnectorConfiguration, String> rejected = Map.of(
            new ConnectorConfiguration()
                .setJettyPath(JETTY_HTTPS_XML)
                .setHttpSslFactory(factory),
            "Jetty connector that configures TLS",
            new ConnectorConfiguration()
                .setJettyPath("jetty/proxy.xml")
                .setHttpSslFactory(factory),
            "default protocol is HTTP",
            new ConnectorConfiguration().setHttpSslClientAuth(true),
            "client authentication requires an HTTP SSL factory");

        int port;

        try (ServerSocket sock = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            port = sock.getLocalPort();
        }

        System.setProperty(IGNITE_JETTY_PORT, Integer.toString(port));

        for (Map.Entry<ConnectorConfiguration, String> e : rejected.entrySet()) {
            connCfg = e.getKey().setPortRange(1);

            assertThrowsAnyCause(log, () -> startGrid(0), IgniteCheckedException.class, e.getValue());
            assertThrows(log, () -> plainHttpVersion(port), ConnectException.class, null);
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
        setJettyKeyStore();

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
    @WithSystemProperty(key = IGNITE_JETTY_PORT, value = "")
    public void testHttpsThatDidNotStartIsNotRegistered() throws Exception {
        connCfg = new ConnectorConfiguration().setHttpSslFactory(serverFactory()).setPortRange(1);

        try (ServerSocket sock = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            System.setProperty(IGNITE_JETTY_PORT, Integer.toString(sock.getLocalPort()));

            IgniteEx node = startGrid(0);

            assertTrue(node.context().ssl().factories().isEmpty());
        }
    }

    /** Creates a server factory that uses CA 1 for both keys and trust. */
    private SslContextFactory serverFactory() throws Exception {
        return configureServer(new SslContextFactory(), 30);
    }

    /** Configures the initial server certificate with the validity needed by the test. */
    private SslContextFactory configureServer(SslContextFactory factory, int validDays) throws Exception {
        Path keyStore = dir.resolve("keys.jks");
        Path trustStore = dir.resolve("trust.jks");

        save(serverKeys(ca1, validDays), keyStore);
        save(ca1.trustStore(), trustStore);

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(PASSWORD);
        factory.setTrustStoreFilePath(trustStore.toString());
        factory.setTrustStorePassword(PASSWORD);

        return factory;
    }

    /** Issues a server certificate for localhost. */
    private KeyStore serverKeys(TestCertificateAuthority ca, int validDays) throws Exception {
        return ca.keyStore("localhost", now - HOURS.toMillis(1), now + DAYS.toMillis(validDays));
    }

    /** Creates a client with a certificate issued by the given CA. */
    private SSLContext clientWithCertificate(TestCertificateAuthority ca) throws Exception {
        return client(ca.keyManagers("client", now - HOURS.toMillis(1), now + DAYS.toMillis(30)));
    }

    /** Creates a client context that trusts both CAs. Omit the key managers for a client without a certificate. */
    private SSLContext client(KeyManager... keyManagers) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");

        tmf.init(trustStoreOf(ca1, ca2));

        SSLContext ctx = SSLContext.getInstance("TLS");

        ctx.init(keyManagers, tmf.getTrustManagers(), null);

        return ctx;
    }

    /** Connects to the HTTP REST port advertised by the node. */
    private SSLSocket connect(SSLContext client) throws IOException {
        int port = grid(0).localNode().attribute(ATTR_REST_JETTY_PORT);

        return (SSLSocket)client.getSocketFactory().createSocket("127.0.0.1", port);
    }

    /** Sends the version command with the given TLS protocol and cipher suite. */
    private SSLSession requestWithTls(String protocol, String suite) throws Exception {
        try (SSLSocket sock = connect(client())) {
            sock.setEnabledProtocols(new String[] {protocol});
            sock.setEnabledCipherSuites(new String[] {suite});

            assertEquals(200, request(sock, "localhost"));

            return sock.getSession();
        }
    }

    /** @return Issuer of the certificate that a new connection gets. */
    private X500Principal issuerOfNewConnection(SSLContext client) throws Exception {
        try (SSLSocket sock = connect(client)) {
            assertEquals(200, request(sock, "localhost"));

            return serverIssuer(sock);
        }
    }

    /** @return Issuer of the certificate presented by the server. */
    private static X500Principal serverIssuer(SSLSocket sock) throws SSLException {
        return ((X509Certificate)sock.getSession().getPeerCertificates()[0]).getIssuerX500Principal();
    }

    /** Sends the version command over plain HTTP. */
    private static int plainHttpVersion(int port) throws IOException {
        try (Socket sock = new Socket("127.0.0.1", port)) {
            return request(sock, "localhost");
        }
    }

    /**
     * Sends the version command and reads the response.
     *
     * @return Response status.
     */
    private static int request(Socket sock, String host) throws IOException {
        sock.setSoTimeout((int)TIMEOUT_MS);

        HttpTester.Request req = HttpTester.newRequest();

        req.setURI("/ignite?cmd=version");
        req.put(HttpHeader.HOST, host + ':' + sock.getPort());

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

    /** Gives the Jetty XML connector a localhost certificate issued by CA 2. */
    private void setJettyKeyStore() throws Exception {
        Path keyStore = dir.resolve("jetty-keys.jks");

        save(serverKeys(ca2, 30), keyStore);
        System.setProperty(JETTY_KEY_STORE, keyStore.toString());
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
