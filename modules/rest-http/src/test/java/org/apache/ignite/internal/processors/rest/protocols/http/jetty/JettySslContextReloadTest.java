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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.GridRestProtocol;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_PORT;
import static org.apache.ignite.internal.ssl.SslContextRegistry.HTTP_REST;
import static org.apache.ignite.ssl.SslTestUtils.assertClientAuthentication;
import static org.apache.ignite.ssl.SslTestUtils.clientContext;
import static org.apache.ignite.ssl.SslTestUtils.node;
import static org.apache.ignite.ssl.SslTestUtils.reload;
import static org.apache.ignite.ssl.SslTestUtils.servedCertificate;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.TestCertificateAuthority.PWD;
import static org.apache.ignite.testframework.TestCertificateAuthority.chain;
import static org.apache.ignite.testframework.TestCertificateAuthority.save;
import static org.junit.Assert.assertArrayEquals;

/**
 * HTTPS of HTTP REST set up with {@link ConnectorConfiguration#setHttpSslFactory}: reload, client authentication, TLS parameters,
 * Jetty XML connectors and refused configurations. HTTP REST starts on old-key from old-ca and trusts both authorities unless a test
 * says otherwise; probes trust both authorities and present no client certificate unless a test needs one.
 */
public class JettySslContextReloadTest extends GridCommonAbstractTest {
    /** Bounds connection, handshake and response reads on the loopback probes. */
    private static final int SOCKET_TIMEOUT = 30_000;

    /** Jetty configuration with a plain HTTP connector and a secure request customizer that checks Host against the certificate. */
    private static final String JETTY_SNI_HOST_CHECK = "modules/rest-http/src/test/resources/jetty-http-customizer.xml";

    /** Jetty configuration with a connector that has an SSL connection factory of its own. */
    private static final String JETTY_SSL = "modules/rest-http/src/test/resources/jetty-ssl.xml";

    /** Jetty configuration with HTTP as its default protocol and an SSL connection factory of its own. */
    private static final String JETTY_HTTP_WITH_SSL = "modules/rest-http/src/test/resources/jetty-http-with-ssl.xml";

    /** Jetty configuration with a connector that expects the PROXY protocol before HTTP. */
    private static final String JETTY_PROXY = "modules/rest-http/src/test/resources/jetty-proxy.xml";

    /** Alias and subject of the key HTTP REST starts on. */
    private static final String OLD_KEY = "old-key";

    /** Key store file of {@code httpSslFactory}, which tests replace on disk. */
    private Path keyFile;

    /** Trust store file of {@code httpSslFactory}, which tests replace on disk. */
    private Path trustFile;

    /** Authority of the key HTTP REST starts on. */
    private TestCertificateAuthority oldCa;

    /** Replacement authority. */
    private TestCertificateAuthority newCa;

    /** Both authorities: HTTP REST trusts them at start, probes always. */
    private KeyStore trustBoth;

    /** Creates the stores used by tests that establish HTTPS connections through the factory. */
    private void prepareStores() throws Exception {
        keyFile = Files.createTempFile("ignite-https-key-", ".jks");
        trustFile = Files.createTempFile("ignite-https-trust-", ".jks");
        oldCa = new TestCertificateAuthority("old-ca");
        newCa = new TestCertificateAuthority("new-ca");
        trustBoth = TestCertificateAuthority.trustStore(oldCa, newCa);

        save(oldCa.keyStoreWithPrivateKey(OLD_KEY), keyFile);
        save(trustBoth, trustFile);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        if (keyFile != null)
            Files.deleteIfExists(keyFile);

        if (trustFile != null)
            Files.deleteIfExists(trustFile);
    }

    /**
     * Reload reports HTTP REST, and a new connection using the same client context gets the new certificate.
     * A connection opened before reload keeps working. The request names the host by an IP address that no
     * certificate contains.
     */
    @Test
    public void testNewConnectionsGetReloadedCertificate() throws Exception {
        prepareStores();

        String replacementAlias = "new-key";
        KeyStore newKeys = newCa.keyStoreWithPrivateKey(replacementAlias);
        IgniteEx g = startGrid(0, cfg -> {
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setHttpSslFactory(factory()));
        });
        SSLContext client = clientContext(null, trustBoth);

        try (SSLSocket open = socket(client, port(g))) {
            open.startHandshake();

            save(newKeys, keyFile);

            assertContains(log, reload(g), node(g) + ": reloaded " + HTTP_REST + "; loaded alias=" + replacementAlias + ", ");
            assertEquals(200, versionStatus(open, "127.0.0.1"));
        }

        assertEquals(chain(newKeys).get(0), servedCertificate(client, port(g)));
    }

    /** With client authentication, HTTP REST accepts a client of the authority it trusts now and refuses one it trusted before reload. */
    @Test
    public void testClientAuthFollowsReloadedTrustStore() throws Exception {
        prepareStores();

        KeyStore oldClient = oldCa.keyStoreWithPrivateKey("old-client");
        KeyStore newClient = newCa.keyStoreWithPrivateKey("new-client");

        save(oldCa.trustStore(), trustFile);

        IgniteEx g = startGrid(0, cfg -> {
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setHttpSslFactory(factory()).setHttpSslClientAuth(true));
        });

        assertClientAuthentication(port(g), oldClient, newClient, trustBoth);

        save(newCa.trustStore(), trustFile);

        reload(g);

        assertClientAuthentication(port(g), newClient, oldClient, trustBoth);
    }

    /** Jetty takes the protocols and cipher suites of the factory at start and still removes the cipher suites it excludes. */
    @Test
    public void testFactoryTlsParametersAppliedWithJettyExclusions() throws Exception {
        prepareStores();

        String[] jettyExclusions = new org.eclipse.jetty.util.ssl.SslContextFactory.Server().getExcludeCipherSuites();
        String excluded = Arrays.stream(SSLContext.getDefault().getSupportedSSLParameters().getCipherSuites())
            .filter(cipher -> Arrays.stream(jettyExclusions).anyMatch(cipher::matches))
            .findFirst().orElseThrow(() -> new AssertionError("The JVM must support a cipher suite excluded by Jetty"));
        String allowed = "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256";
        String protocol = "TLSv1.2";
        SslContextFactory factory = factory();

        factory.setProtocols(protocol);
        factory.setCipherSuites(allowed, excluded);

        IgniteEx g = startGrid(0, cfg -> {
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setHttpSslFactory(factory));
        });
        org.eclipse.jetty.util.ssl.SslContextFactory jetty = jettySslFactory(g);

        // Check the engines directly: JVM handshake policy can independently reject legacy cipher suites.
        assertArrayEquals(new String[] {allowed, excluded}, jetty.getSslContext().createSSLEngine().getEnabledCipherSuites());
        assertArrayEquals(new String[] {allowed}, jetty.newSSLEngine().getEnabledCipherSuites());

        try (SSLSocket sock = socket(clientContext(null, trustBoth), port(g))) {
            sock.startHandshake();

            assertEquals(protocol, sock.getSession().getProtocol());
            assertEquals(allowed, sock.getSession().getCipherSuite());
        }
    }

    /** A Jetty XML connector gets TLS and keeps its own secure request customizer, which checks Host against the certificate. */
    @Test
    public void testJettyConnectorFromFileGetsTls() throws Exception {
        prepareStores();

        IgniteEx g = startGrid(0, cfg -> {
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setJettyPath(JETTY_SNI_HOST_CHECK).setHttpSslFactory(factory()));
        });

        try (SSLSocket sock = socket(clientContext(null, trustBoth), port(g))) {
            assertEquals("Host named by the certificate", 200, versionStatus(sock, OLD_KEY));
        }

        try (SSLSocket sock = socket(clientContext(null, trustBoth), port(g))) {
            assertEquals("Host absent from the certificate", 400, versionStatus(sock, "127.0.0.1"));
        }
    }

    /** TLS set up in Jetty XML without {@code httpSslFactory} serves HTTPS as before, and reload does not see it. */
    @Test
    public void testLegacyJettyTlsNotReloaded() throws Exception {
        IgniteEx g = startGrid(0, cfg -> {
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setJettyPath(JETTY_SSL));
        });

        // The file reads the checked-in node01 store, as legacy configurations do, so the probe takes the checked-in stores too.
        // Jetty adds a secure request customizer that wants Host to name that certificate.
        try (SSLSocket sock = socket(GridTestUtils.sslTrustedFactory("node01", "trustboth").create(), port(g))) {
            assertEquals(200, versionStatus(sock, "node01"));
        }

        assertEquals(node(g) + ": no SSL context factory is configured\n", reload(g));
    }

    /** A node does not start with client authentication on but no factory: the setting would do nothing. */
    @Test
    public void testClientAuthWithoutFactoryRefused() {
        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(0, cfg -> {
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setHttpSslClientAuth(true));
        }), IgniteCheckedException.class, "applies only to TLS set up with httpSslFactory");
    }

    /** A node does not start when the connector in its Jetty configuration has an SSL connection factory of its own. */
    @Test
    public void testJettySslConnectionFactoryRefused() {
        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(0, cfg -> {
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setJettyPath(JETTY_SSL).setHttpSslFactory(new SslContextFactory()));
        }), IgniteCheckedException.class, "must have no SSL connection factory of its own");
    }

    /** An existing SSL connection factory is refused even when the connector's default protocol is HTTP. */
    @Test
    public void testJettySslConnectionFactoryWithHttpDefaultRefused() {
        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(0, cfg -> {
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setJettyPath(JETTY_HTTP_WITH_SSL)
                .setHttpSslFactory(new SslContextFactory()));
        }), IgniteCheckedException.class, "must have no SSL connection factory of its own");
    }

    /** A node does not start when the connector in its Jetty configuration expects the PROXY protocol: TLS in front would break it. */
    @Test
    public void testJettyProxyProtocolRefused() {
        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(0, cfg -> {
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setJettyPath(JETTY_PROXY)
                .setHttpSslFactory(new SslContextFactory()));
        }), IgniteCheckedException.class, "with no PROXY protocol in front");
    }

    /** @return Factory reading {@link #keyFile} and {@link #trustFile}. */
    private SslContextFactory factory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreType("JKS");
        factory.setKeyStoreFilePath(keyFile.toString());
        factory.setKeyStorePassword(PWD);
        factory.setTrustStoreType("JKS");
        factory.setTrustStoreFilePath(trustFile.toString());
        factory.setTrustStorePassword(PWD);

        return factory;
    }

    /** @return SSL factory of the running Jetty connector; Jetty exposes the engines it creates only through it. */
    private static org.eclipse.jetty.util.ssl.SslContextFactory jettySslFactory(IgniteEx g) {
        Collection<GridRestProtocol> protos = GridTestUtils.getFieldValue(g.context().rest(), "protos");

        GridRestProtocol jetty = protos.stream().filter(GridJettyRestProtocol.class::isInstance).findFirst().orElseThrow();
        Server srv = GridTestUtils.getFieldValue(jetty, "httpSrv");

        return ((ServerConnector)srv.getConnectors()[0]).getConnectionFactory(SslConnectionFactory.class).getSslContextFactory();
    }

    /** @return HTTP REST port of the node. */
    private static int port(IgniteEx g) {
        return g.localNode().attribute(ATTR_REST_JETTY_PORT);
    }

    /**
     * @param ctx Context to connect with.
     * @param port Loopback port to connect to.
     * @return TLS socket for a test that uses the connection after the handshake.
     */
    private static SSLSocket socket(SSLContext ctx, int port) throws IOException {
        SSLSocket sock = (SSLSocket)ctx.getSocketFactory().createSocket();

        try {
            sock.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), SOCKET_TIMEOUT);
            sock.setSoTimeout(SOCKET_TIMEOUT);

            return sock;
        }
        catch (IOException | RuntimeException e) {
            U.closeQuiet(sock);

            throw e;
        }
    }

    /**
     * @param sock TLS connection to HTTP REST.
     * @param host Value of the Host header.
     * @return HTTP status of a version request; the server closes the connection after the response.
     */
    private static int versionStatus(SSLSocket sock, String host) throws IOException {
        String req = "GET /ignite?cmd=version HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n";

        sock.getOutputStream().write(req.getBytes(US_ASCII));

        String statusLine = new BufferedReader(new InputStreamReader(sock.getInputStream(), US_ASCII)).readLine();

        assertNotNull("HTTP REST must answer a version request [host=" + host + ']', statusLine);

        return Integer.parseInt(statusLine.split(" ")[1]);
    }
}
