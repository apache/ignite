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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientRetryNonePolicy;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.management.ssl.SslStatusTask;
import org.apache.ignite.internal.ssl.SslContextRegistry;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_PORT;
import static org.apache.ignite.internal.management.api.CommandUtils.execute;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;
import static org.junit.Assert.assertThrows;

/** Tests TLS reload against the listeners of a running node. */
public class SslContextReloadTest extends GridCommonAbstractTest {
    /** All four transports of the node share one factory. */
    private static final String TRANSPORTS = "binary REST, client connector, communication, discovery";

    /** */
    private static final int SOCKET_TIMEOUT = 10_000;

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** Established connections stay open and new handshakes use the last successfully loaded context. */
    @Test
    public void testConnectionsDuringReload() throws Exception {
        BlockingSslContextFactory factory = new BlockingSslContextFactory();
        IgniteEx node = startNode(factory);
        int clientPort = node.context().clientListener().port();

        assertStatus(node, "node01");

        // A reconnect after the switch would fail because this client trusts only the original authority.
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:" + clientPort)
            .setTimeout(SOCKET_TIMEOUT)
            .setSslMode(SslMode.REQUIRED)
            .setSslContextFactory(sslTrustedFactory("node01", "trustone"))
            .setRetryPolicy(new ClientRetryNonePolicy())
            .setPartitionAwarenessEnabled(false)
            .setClusterDiscoveryEnabled(false))) {
            assertTrue(client.cacheNames().isEmpty());

            factory.setKeyStoreFilePath(factory.replacementPath);

            IgniteInternalFuture<String> reload = GridTestUtils.runAsync(() -> {
                return execute(null, node, SslReloadTask.class, new NoArg(), node.cluster().nodes());
            });

            try {
                assertTrue("Reload did not start opening the replacement key store",
                    factory.opening.await(30, TimeUnit.SECONDS));

                assertServedCertificate("CN=node01", clientPort);
                assertTrue(client.cacheNames().isEmpty());
            }
            finally {
                factory.resume.countDown();
            }

            assertContains(log, reload.get(30, TimeUnit.SECONDS), ": reloaded " + TRANSPORTS);

            assertTrue(client.cacheNames().isEmpty());
            assertStatus(node, "node02");

            assertServedCertificate("CN=node02", clientPort);
            assertServedCertificate("CN=node02", ((TcpDiscoverySpi)node.configuration().getDiscoverySpi()).getLocalPort());
            assertServedCertificate("CN=node02", ((TcpCommunicationSpi)node.configuration().getCommunicationSpi()).boundPort());
            assertServedCertificate("CN=node02", node.localNode().<Integer>attribute(ATTR_REST_TCP_PORT));
        }
    }

    /** A key-store load failure leaves the original context in use. */
    @Test
    public void testFailedReloadKeepsContext() throws Exception {
        SslContextFactory factory = (SslContextFactory)sslTrustedFactory("node01", "trustboth");
        IgniteEx node = startNode(factory);
        SslContextRegistry.Entry entry = entry(node);
        SSLContext original = entry.context();
        int clientPort = node.context().clientListener().port();

        factory.setKeyStorePassword("wrong-password".toCharArray());

        assertThrows(SSLException.class, entry::reload);
        assertSame(original, entry.context());
        assertServedCertificate("CN=node01", clientPort);
        assertStatus(node, "node01");
    }

    /** A key manager that loads private keys on demand cannot hide an unreadable replacement key. */
    @Test
    public void testReloadRejectsUnreadablePrivateKey() throws Exception {
        SslContextFactory factory = (SslContextFactory)sslTrustedFactory("node01", "trustboth");

        factory.setKeyAlgorithm("NewSunX509");
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        SslContextRegistry.Entry entry = entry(startNode(factory));
        SSLContext original = entry.context();
        char[] pwd = keyStorePassword().toCharArray();
        KeyStore store = KeyStore.getInstance("JKS");

        try (InputStream input = Files.newInputStream(Path.of(keyStorePath("node02")))) {
            store.load(input, pwd);
        }

        store.setKeyEntry("node02", store.getKey("node02", pwd), "different-key-password".toCharArray(),
            store.getCertificateChain("node02"));

        Path replacement = Files.createTempFile("ignite-tls-reload", ".jks");

        try {
            try (OutputStream output = Files.newOutputStream(replacement)) {
                store.store(output, pwd);
            }

            factory.setKeyStoreFilePath(replacement.toString());

            SSLException err = assertThrows(SSLException.class, entry::reload);

            assertContains(log, err.getMessage(), "alias=node02");
            assertSame(original, entry.context());
        }
        finally {
            Files.deleteIfExists(replacement);
        }
    }

    /** Starts a node with all four TLS transports sharing the supplied factory. */
    private IgniteEx startNode(SslContextFactory factory) throws Exception {
        return startGrid(0, cfg -> {
            cfg.setSslContextFactory(factory)
                .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                    .setSslEnabled(true)
                    .setUseIgniteSslContextFactory(true))
                .setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true));
        });
    }

    /** Returns the entry for the factory shared by the listeners. */
    private static SslContextRegistry.Entry entry(IgniteEx node) {
        List<SslContextRegistry.Entry> entries = node.context().sslContexts().entries();

        assertEquals(1, entries.size());

        return entries.get(0);
    }

    /** Checks the loaded alias for the factory shared by the four listeners. */
    private void assertStatus(IgniteEx node, String alias) throws Exception {
        String status = execute(null, node, SslStatusTask.class, new NoArg(), node.cluster().nodes());

        assertContains(log, status, ": " + TRANSPORTS);
        assertContains(log, status, "loaded alias=" + alias + ',');
    }

    /** Checks the leaf certificate presented on a fresh TLS connection. */
    private static void assertServedCertificate(String subject, int port) throws Exception {
        SSLContext ctx = sslTrustedFactory("node01", "trustboth").create();

        try (SSLSocket sock = (SSLSocket)ctx.getSocketFactory().createSocket()) {
            sock.connect(new InetSocketAddress("127.0.0.1", port), SOCKET_TIMEOUT);
            sock.setSoTimeout(SOCKET_TIMEOUT);
            sock.startHandshake();

            assertEquals("Peer certificate on port " + port, subject, sock.getSession().getPeerPrincipal().getName());
        }
    }

    /** Pauses the file open used for the replacement key store. */
    private static class BlockingSslContextFactory extends SslContextFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String replacementPath = keyStorePath("node02");

        /** */
        private final CountDownLatch opening = new CountDownLatch(1);

        /** */
        private final CountDownLatch resume = new CountDownLatch(1);

        /** */
        private BlockingSslContextFactory() {
            setKeyStoreFilePath(keyStorePath("node01"));
            setKeyStorePassword(keyStorePassword().toCharArray());
            setTrustStoreFilePath(keyStorePath("trustboth"));
            setTrustStorePassword(keyStorePassword().toCharArray());
        }

        /** {@inheritDoc} */
        @Override protected InputStream openFileInputStream(String path) throws IOException {
            if (replacementPath.equals(path)) {
                opening.countDown();

                try {
                    if (!resume.await(30, TimeUnit.SECONDS))
                        throw new IOException("Timed out waiting to open the replacement key store");
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IOException(e);
                }
            }

            return super.openFileInputStream(path);
        }
    }
}
