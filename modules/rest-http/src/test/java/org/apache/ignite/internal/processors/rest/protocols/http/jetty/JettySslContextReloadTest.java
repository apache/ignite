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
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
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
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;

/** Tests HTTP REST served on the certificates of {@link ConnectorConfiguration#setHttpSslFactory}. */
public class JettySslContextReloadTest extends GridCommonAbstractTest {
    /** Jetty configuration with a connector without TLS. */
    private static final String JETTY_PLAIN = "modules/clients/src/test/resources/jetty/rest-jetty.xml";

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
     * HTTP REST answers on the rotated certificate, a connection opened before the reload keeps answering, a client does not resume
     * a session of the old certificate, a client without a certificate passes while client authentication is off, and a key store
     * HTTP REST cannot read leaves it on the last good one.
     */
    @Test
    public void testReload() throws Exception {
        IgniteEx g = startGrid(0);

        SSLContext client = GridTestUtils.sslTrustedFactory("node01", "trustboth").create();

        try (SSLSocket open = socket(client, port(g))) {
            assertEquals("CN=node01", subject(open));

            place("node02", keyStore);

            assertContains(log, reload(g), ": reloaded " + HTTP_REST);
            assertTrue(get(open, "node01").startsWith("HTTP/1.1 200"));
        }

        try (SSLSocket after = socket(client, port(g))) {
            assertEquals("CN=node02", subject(after));
            assertTrue(get(after, "node02").startsWith("HTTP/1.1 200"));
        }

        try (SSLSocket byIp = socket(client, port(g))) {
            assertTrue("A request by IP address must pass, whatever host the certificate names",
                get(byIp, "127.0.0.1").startsWith("HTTP/1.1 200"));
        }

        try (SSLSocket anon = socket(trustingOnly("trustboth"), port(g))) {
            assertTrue("A client without a certificate must pass while client authentication is off",
                get(anon, "node02").startsWith("HTTP/1.1 200"));
        }

        assertEquals("CN=node02", g.context().metric().registry("ssl.http.rest").findMetric("CertificateSubject").getAsString());

        Files.write(keyStore, "not a key store".getBytes());

        assertContains(log, reloadFailure(g), "failed on " + HTTP_REST);
        assertEquals("CN=node02", servedSubject(port(g)));
    }

    /** Jetty enables only the protocols the factory allows. */
    @Test
    public void testFactoryProtocols() throws Exception {
        protocols = new String[] {"TLSv1.2"};

        IgniteEx g = startGrid(0);

        try (SSLSocket sock = socket(GridTestUtils.sslTrustedFactory("node01", "trustboth").create(), port(g))) {
            sock.startHandshake();

            assertEquals("TLSv1.2", sock.getSession().getProtocol());
        }
    }

    /** A connector from a Jetty configuration file gets TLS in front of it. */
    @Test
    public void testJettyConnectorFromFileGetsTls() throws Exception {
        jettyPath = JETTY_PLAIN;

        IgniteEx g = startGrid(0);

        try (SSLSocket sock = socket(GridTestUtils.sslTrustedFactory("node01", "trustboth").create(), port(g))) {
            assertEquals("CN=node01", subject(sock));
            assertTrue(get(sock, "node01").startsWith("HTTP/1.1 200"));
        }

        try (SSLSocket byIp = socket(GridTestUtils.sslTrustedFactory("node01", "trustboth").create(), port(g))) {
            assertTrue("A request by IP address must pass, whatever host the certificate names",
                get(byIp, "127.0.0.1").startsWith("HTTP/1.1 200"));
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
     * With client authentication on, HTTP REST refuses a client whose certificate the trust store of the factory does not accept, and
     * a client that presents no certificate at all.
     */
    @Test
    public void testClientAuth() throws Exception {
        clientAuth = true;

        int port = port(startGrid(0));

        try (SSLSocket sock = tls12Socket(GridTestUtils.sslTrustedFactory("node01", "trustboth").create(), port)) {
            sock.startHandshake();
        }

        assertHandshakeRefused(presentingAlways("connectorClient", "trustboth"), port);
        assertHandshakeRefused(trustingOnly("trustboth"), port);
    }

    /**
     * @param cli Context to connect with.
     * @param port Port to connect to.
     */
    private static void assertHandshakeRefused(SSLContext cli, int port) throws Exception {
        try (SSLSocket sock = tls12Socket(cli, port)) {
            GridTestUtils.assertThrows(log, () -> {
                sock.startHandshake();

                return null;
            }, IOException.class, null);
        }
    }

    /**
     * @param trustStore Test trust store.
     * @return Context that trusts the store and presents no certificate.
     */
    private static SSLContext trustingOnly(String trustStore) throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");

        ctx.init(null, trustManagers(trustStore), null);

        return ctx;
    }

    /**
     * @param keyStore Test key store.
     * @param trustStore Test trust store.
     * @return Context that presents the key of the store whatever authorities the server names, so that the server has to refuse
     *      it, and trusts the trust store.
     */
    private static SSLContext presentingAlways(String keyStore, String trustStore) throws Exception {
        KeyStore keys = store(keyStore);

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        kmf.init(keys, keyStorePassword().toCharArray());

        X509ExtendedKeyManager km = (X509ExtendedKeyManager)kmf.getKeyManagers()[0];
        String alias = keys.aliases().nextElement();

        X509ExtendedKeyManager always = new X509ExtendedKeyManager() {
            @Override public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
                return alias;
            }

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

        SSLContext ctx = SSLContext.getInstance("TLS");

        ctx.init(new KeyManager[] {always}, trustManagers(trustStore), null);

        return ctx;
    }

    /** @param name Test store. */
    private static KeyStore store(String name) throws Exception {
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());

        try (InputStream in = Files.newInputStream(Path.of(keyStorePath(name)))) {
            store.load(in, keyStorePassword().toCharArray());
        }

        return store;
    }

    /** @param trustStore Test trust store. */
    private static TrustManager[] trustManagers(String trustStore) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        tmf.init(store(trustStore));

        return tmf.getTrustManagers();
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
        return (SSLSocket)ctx.getSocketFactory().createSocket(InetAddress.getLoopbackAddress(), port);
    }

    /** @return Subject of the certificate the server presents on the connection. */
    private static String subject(SSLSocket sock) throws IOException {
        return ((X509Certificate)sock.getSession().getPeerCertificates()[0]).getSubjectX500Principal().getName();
    }

    /**
     * @param sock Connection.
     * @param host Host to ask for.
     * @return Status line of the answer to a REST command.
     */
    private static String get(SSLSocket sock, String host) throws IOException {
        String req = "GET /ignite?cmd=version HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n";

        sock.getOutputStream().write(req.getBytes(US_ASCII));

        return new BufferedReader(new InputStreamReader(sock.getInputStream(), US_ASCII)).readLine();
    }
}
