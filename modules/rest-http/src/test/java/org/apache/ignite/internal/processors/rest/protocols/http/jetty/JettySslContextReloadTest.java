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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_PORT;
import static org.apache.ignite.internal.ssl.SslContextRegistry.HTTP_REST;
import static org.apache.ignite.ssl.SslTestUtils.place;
import static org.apache.ignite.ssl.SslTestUtils.reload;
import static org.apache.ignite.ssl.SslTestUtils.reloadFailure;
import static org.apache.ignite.ssl.SslTestUtils.servedSubject;
import static org.apache.ignite.ssl.SslTestUtils.status;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;

/** Tests HTTP REST certificate reload through {@link ConnectorConfiguration#setHttpSslFactory}. */
public class JettySslContextReloadTest extends GridCommonAbstractTest {
    /** Bounds connection, handshake and response reads on the loopback probes. */
    private static final int SOCKET_TIMEOUT = 30_000;

    /** Jetty configuration with a plain connector and an explicit secure request customizer. */
    private static final String JETTY_PLAIN = "modules/rest-http/src/test/resources/jetty-http-customizer.xml";

    /** Jetty configuration with a connector that has an SSL connection factory of its own. */
    private static final String JETTY_SSL = "modules/rest-http/src/test/resources/jetty-ssl.xml";

    /** Jetty configuration with a connector that expects the PROXY protocol before HTTP. */
    private static final String JETTY_PROXY = "modules/rest-http/src/test/resources/jetty-proxy.xml";

    /** Key store HTTP REST runs on; replaced on disk to rotate the certificate. */
    private Path keyStore;

    /** Jetty configuration, {@code null} for the built-in one. */
    private String jettyPath;

    /** Protocols of the factory, {@code null} for the defaults. */
    private String[] protocols;

    /** Whether HTTP REST requires a certificate from clients. */
    private boolean clientAuth;

    /** Whether the node starts without {@code httpSslFactory}. */
    private boolean noFactory;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(keyStorePassword().toCharArray());
        factory.setTrustStoreFilePath(keyStorePath("trustboth"));
        factory.setTrustStorePassword(keyStorePassword().toCharArray());

        if (protocols != null)
            factory.setProtocols(protocols);

        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration().setJettyPath(jettyPath).setHttpSslFactory(noFactory ? null : factory)
                .setHttpSslClientAuth(clientAuth));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        keyStore = Files.createTempFile("ignite-jetty-ssl-reload-", ".jks");

        place("node01", keyStore);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        if (keyStore != null)
            Files.deleteIfExists(keyStore);
    }

    /**
     * New HTTP REST connections use the renewed certificate without resuming old TLS sessions. Existing connections stay open,
     * and clients without certificates can connect when client authentication is disabled. A failed reload keeps the last
     * working context.
     */
    @Test
    public void testReload() throws Exception {
        IgniteEx g = startGrid(0);

        SSLContext client = trustedClient();

        try (SSLSocket open = socket(client, port(g))) {
            assertEquals("CN=node01", serverSubject(open));

            place("node02", keyStore);

            assertContains(log, reload(g), ": reloaded " + HTTP_REST);
            assertVersionResponse(open, "node01");
        }

        try (SSLSocket after = socket(client, port(g))) {
            assertEquals("CN=node02", serverSubject(after));

            assertVersionResponse(after, "node02");
        }

        try (SSLSocket byIp = socket(client, port(g))) {
            assertVersionResponse(byIp, "127.0.0.1");
        }

        try (SSLSocket anonymous = socket(anonymousClient(), port(g))) {
            assertVersionResponse(anonymous, "node02");
        }

        assertContains(log, status(g), "loaded subject=CN=node02,");

        Files.write(keyStore, "not a key store".getBytes());

        assertContains(log, reloadFailure(g), "failed on " + HTTP_REST);
        assertEquals("CN=node02", servedSubject(port(g)));
    }

    /** Jetty enables only the protocols the factory allows. */
    @Test
    public void testFactoryProtocols() throws Exception {
        protocols = new String[] {"TLSv1.2"};

        IgniteEx g = startGrid(0);

        try (SSLSocket sock = socket(trustedClient(), port(g))) {
            sock.startHandshake();

            assertEquals("TLSv1.2", sock.getSession().getProtocol());
        }
    }

    /** A connector from a Jetty configuration file gets TLS and keeps its HSTS and host-check settings. */
    @Test
    public void testJettyConnectorFromFileGetsTls() throws Exception {
        jettyPath = JETTY_PLAIN;

        IgniteEx g = startGrid(0);

        try (SSLSocket sock = socket(trustedClient(), port(g))) {
            assertEquals("CN=node01", serverSubject(sock));

            String headers = assertVersionResponse(sock, "localhost");

            assertTrue("HTTP REST must preserve the configured HSTS header: " + headers,
                headers.lines().anyMatch("Strict-Transport-Security: max-age=86400"::equalsIgnoreCase));
        }

        try (SSLSocket byIp = socket(trustedClient(), port(g))) {
            assertVersionResponse(byIp, "127.0.0.1");
        }
    }

    /** A node does not start when the connector in its Jetty configuration has an SSL connection factory of its own. */
    @Test
    public void testJettySslConnectionFactoryRefused() {
        jettyPath = JETTY_SSL;

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(0), IgniteCheckedException.class,
            "must have no SSL connection factory of its own");
    }

    /** A node does not start when the connector in its Jetty configuration expects the PROXY protocol: TLS in front would break it. */
    @Test
    public void testJettyProxyProtocolRefused() {
        jettyPath = JETTY_PROXY;

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(0), IgniteCheckedException.class, "with no PROXY protocol in front");
    }

    /** A node does not start with client authentication on but no factory: the setting would do nothing. */
    @Test
    public void testClientAuthWithoutFactoryRefused() {
        clientAuth = true;
        noFactory = true;

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(0), IgniteCheckedException.class,
            "applies only to TLS set up with httpSslFactory");
    }

    /**
     * Client authentication refuses untrusted and anonymous clients. A trusted client still gets an HTTP response after those
     * refusals.
     */
    @Test
    public void testClientAuth() throws Exception {
        clientAuth = true;

        int port = port(startGrid(0));

        assertHandshakeRefused(untrustedClient(), port);
        assertHandshakeRefused(anonymousClient(), port);

        try (SSLSocket sock = tls12Socket(trustedClient(), port)) {
            assertVersionResponse(sock, "node01");
        }
    }

    /**
     * @param cli Context to connect with.
     * @param port Port to connect to.
     */
    private static void assertHandshakeRefused(SSLContext cli, int port) throws Exception {
        try (SSLSocket sock = tls12Socket(cli, port)) {
            try {
                sock.startHandshake();

                fail("HTTP REST must require a trusted client certificate");
            }
            catch (SSLException | SocketException expected) {
                // A TLS alert or a connection reset is a refusal; a timeout must fail the test.
            }
        }
    }

    /** @return A new client context with the trusted node certificate. */
    private static SSLContext trustedClient() {
        return GridTestUtils.sslTrustedFactory("node01", "trustboth").create();
    }

    /** @return A client that presents no certificate, regardless of the default JVM key store. */
    private static SSLContext anonymousClient() throws Exception {
        return clientContext();
    }

    /** @return A client that presents its untrusted certificate even when the server asks for another authority. */
    private static SSLContext untrustedClient() throws Exception {
        KeyStore keys = store("connectorClient");

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        kmf.init(keys, keyStorePassword().toCharArray());

        X509KeyManager km = (X509KeyManager)kmf.getKeyManagers()[0];

        String alias = keys.aliases().nextElement();

        X509KeyManager always = new X509KeyManager() {
            @Override public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket sock) {
                return alias;
            }

            @Override public String[] getClientAliases(String keyType, Principal[] issuers) {
                return new String[] {alias};
            }

            @Override public String[] getServerAliases(String keyType, Principal[] issuers) {
                return km.getServerAliases(keyType, issuers);
            }

            @Override public String chooseServerAlias(String keyType, Principal[] issuers, Socket sock) {
                return km.chooseServerAlias(keyType, issuers, sock);
            }

            @Override public X509Certificate[] getCertificateChain(String alias) {
                return km.getCertificateChain(alias);
            }

            @Override public PrivateKey getPrivateKey(String alias) {
                return km.getPrivateKey(alias);
            }
        };

        return clientContext(always);
    }

    /** @param name Test store. */
    private static KeyStore store(String name) throws Exception {
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());

        try (InputStream in = Files.newInputStream(Path.of(keyStorePath(name)))) {
            store.load(in, keyStorePassword().toCharArray());
        }

        return store;
    }

    /**
     * @param keyManagers Key managers supplying the client certificate, or an empty array for an anonymous client.
     * @return A client context that trusts both test authorities.
     */
    private static SSLContext clientContext(KeyManager... keyManagers) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        tmf.init(store("trustboth"));

        SSLContext ctx = SSLContext.getInstance("TLS");

        ctx.init(keyManagers, tmf.getTrustManagers(), null);

        return ctx;
    }

    /** @return HTTP REST port of the node. */
    private static int port(IgniteEx g) {
        return g.localNode().attribute(ATTR_REST_JETTY_PORT);
    }

    /**
     * @param probe Context to connect with.
     * @param port Port to connect to.
     * @return Socket limited to TLS 1.2, where the server refuses a client certificate during the handshake.
     */
    private static SSLSocket tls12Socket(SSLContext probe, int port) throws Exception {
        SSLSocket sock = socket(probe, port);

        sock.setEnabledProtocols(new String[] {"TLSv1.2"});

        return sock;
    }

    /**
     * @param ctx Context to connect with.
     * @param port Port to connect to.
     * @return TLS socket to the port.
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

    /** @return Subject of the certificate the server presents on the connection. */
    private static String serverSubject(SSLSocket sock) throws IOException {
        sock.startHandshake();

        return ((X509Certificate)sock.getSession().getPeerCertificates()[0]).getSubjectX500Principal().getName();
    }

    /**
     * Checks that HTTP REST answers a version request successfully.
     *
     * @param sock Connection.
     * @param host Host to ask for.
     * @return Response headers, one per line.
     */
    private static String assertVersionResponse(SSLSocket sock, String host) throws IOException {
        String req = "GET /ignite?cmd=version HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n";

        sock.getOutputStream().write(req.getBytes(US_ASCII));

        BufferedReader res = new BufferedReader(new InputStreamReader(sock.getInputStream(), US_ASCII));

        String status = res.readLine();

        assertNotNull("HTTP REST must answer a version request", status);
        assertTrue("Unexpected HTTP status for Host " + host + ": " + status, status.startsWith("HTTP/1.1 200"));

        StringBuilder headers = new StringBuilder();

        for (String line = res.readLine(); line != null && !line.isEmpty(); line = res.readLine())
            headers.append(line).append('\n');

        return headers.toString();
    }
}
