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

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
    /** System property the Jetty configuration with stores of its own reads the key store path from. */
    private static final String KEY_STORE_PROP = "IGNITE_TEST_JETTY_KEY_STORE";

    /** Jetty configuration serving TLS from the key store at {@link #KEY_STORE_PROP}. */
    private static final String JETTY_OWN_STORES = "modules/rest-http/src/test/resources/jetty-ssl-own-stores.xml";

    /** Jetty configuration asking clients for a certificate, with no stores of its own. */
    private static final String JETTY_CLIENT_AUTH = "modules/rest-http/src/test/resources/jetty-ssl-client-auth.xml";

    /** Key store HTTP REST runs on; replaced on disk to rotate the certificate. */
    private Path keyStore;

    /** Jetty configuration, {@code null} for the built-in one. */
    private String jettyPath;

    /** Protocols of the factory, {@code null} for the defaults. */
    private String[] protocols;

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
            .setConnectorConfiguration(new ConnectorConfiguration().setJettyPath(jettyPath).setHttpSslFactory(factory));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        keyStore = Files.createTempFile("ignite-jetty-ssl-reload-", ".jks");

        place("node01", keyStore);

        System.setProperty(KEY_STORE_PROP, keyStore.toString());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        System.clearProperty(KEY_STORE_PROP);

        if (keyStore != null)
            Files.deleteIfExists(keyStore);
    }

    /** HTTP REST serves the rotated certificate, and a key store it cannot read leaves it on the last good one. */
    @Test
    public void testReload() throws Exception {
        IgniteEx g = startGrid(0);

        assertEquals("CN=node01", servedSubject(port(g)));

        place("node02", keyStore);

        assertContains(log, reload(g), ": reloaded " + HTTP_REST);
        assertEquals("CN=node02", servedSubject(port(g)));
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

        SSLContext probe = GridTestUtils.sslTrustedFactory("node01", "trustboth").create();

        try (SSLSocket sock = (SSLSocket)probe.getSocketFactory().createSocket(InetAddress.getLoopbackAddress(), port(g))) {
            sock.startHandshake();

            assertEquals("TLSv1.2", sock.getSession().getProtocol());
        }
    }

    /** A node does not start when both its Jetty configuration and the factory give HTTP REST certificates. */
    @Test
    public void testJettyStoresWithFactoryRefused() {
        jettyPath = JETTY_OWN_STORES;

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(0), IgniteCheckedException.class, "not from both");
    }

    /** Client authentication set in the Jetty configuration checks clients against the trust store of the factory. */
    @Test
    public void testClientAuthFromJetty() throws Exception {
        jettyPath = JETTY_CLIENT_AUTH;

        int port = port(startGrid(0));

        try (SSLSocket sock = tls12Socket(GridTestUtils.sslTrustedFactory("node01", "trustboth").create(), port)) {
            sock.startHandshake();
        }

        try (SSLSocket sock = tls12Socket(GridTestUtils.sslTrustedFactory("connectorClient", "trustboth").create(), port)) {
            GridTestUtils.assertThrows(log, () -> {
                sock.startHandshake();

                return null;
            }, IOException.class, null);
        }
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
        SSLSocket sock = (SSLSocket)probe.getSocketFactory().createSocket(InetAddress.getLoopbackAddress(), port);

        sock.setEnabledProtocols(new String[] {"TLSv1.2"});

        return sock;
    }
}
