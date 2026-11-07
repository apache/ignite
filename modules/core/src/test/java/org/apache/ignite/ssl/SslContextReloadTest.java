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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientRetryNonePolicy;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_PORT;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;
import static org.junit.Assert.assertThrows;

/** Tests TLS reload across TCP transports. */
public class SslContextReloadTest extends GridCommonAbstractTest {
    /** */
    private static final int SOCKET_TIMEOUT = 10_000;

    /** */
    private static final String KEYS_1 = "node01";

    /** */
    private static final String KEYS_2 = "node02";

    /** */
    private static final String KEYS_3 = "node03";

    /** */
    private static final String TRUST_1 = "trustone";

    /** */
    private static final String TRUST_2 = "trusttwo";

    /** */
    private static final String TRUST_1_AND_2 = "trustboth";

    /** */
    private static final String LOOPBACK_ADDRESS = "127.0.0.1";

    /** Discovery addresses shared within one test. */
    private final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testNewConnectionsUseReloadedKeysAndTrust() throws Exception {
        ReloadableSslContextFactory factory = new ReloadableSslContextFactory(KEYS_1, TRUST_1);
        IgniteEx node = startNode(0, factory::create);
        int[] ports = tlsPorts(node);

        for (int port : ports)
            assertEquals(certificate(KEYS_1), connectAndGetServerCertificate(port, KEYS_1, TRUST_1));

        ClientConfiguration clientCfg = clientConfiguration(node).setSslContextFactory(sslTrustedFactory(KEYS_1, TRUST_1));

        try (IgniteClient clientWithOldTrust = Ignition.startClient(clientCfg)) {
            clientWithOldTrust.cacheNames();

            factory.reload(KEYS_2, TRUST_2);

            for (int port : ports) {
                assertEquals(certificate(KEYS_2), connectAndGetServerCertificate(port, KEYS_3, TRUST_2));
                assertThrows(SSLException.class, () -> connectAndGetServerCertificate(port, KEYS_1, TRUST_1_AND_2));
            }

            clientWithOldTrust.cacheNames();
        }

        IgniteEx peerWithNewTrust = startNode(1, sslTrustedFactory(KEYS_3, TRUST_2));

        assertTrue(((TcpDiscoverySpi)node.configuration().getDiscoverySpi()).pingNode(peerWithNewTrust.localNode().id()));

        int result = node.compute(node.cluster().forNodeId(peerWithNewTrust.localNode().id())).call(() -> 1);

        assertEquals(1, result);
    }

    /** */
    @Test
    public void testUsePreviousContextDuringAndAfterFailedReload() throws Exception {
        String replacementKeyStore = keyStorePath(KEYS_2);
        CountDownLatch reloadStarted = new CountDownLatch(1);
        CountDownLatch allowReload = new CountDownLatch(1);

        ReloadableSslContextFactory factory = new ReloadableSslContextFactory(KEYS_1, TRUST_1) {
            /** */
            private static final long serialVersionUID = 0L;

            /** {@inheritDoc} */
            @Override protected InputStream openFileInputStream(String path) throws IOException {
                if (replacementKeyStore.equals(path)) {
                    reloadStarted.countDown();

                    try {
                        if (!allowReload.await(60, TimeUnit.SECONDS))
                            throw new IOException("Timed out waiting to open the replacement key store");
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new IOException(e);
                    }
                }

                return super.openFileInputStream(path);
            }
        };

        IgniteEx node = startNode(0, factory);
        int[] ports = tlsPorts(node);

        ClientConfiguration clientCfg = clientConfiguration(node).setSslContextFactory(sslTrustedFactory(KEYS_1, TRUST_1));

        try (IgniteClient client = Ignition.startClient(clientCfg)) {
            client.cacheNames();

            factory.setKeyStorePassword("wrong-password".toCharArray());

            IgniteInternalFuture<?> reload = GridTestUtils.runAsync(() -> {
                assertThrows(IgniteException.class, () -> factory.reload(KEYS_2, TRUST_2));
            });

            try {
                assertTrue("Reload did not reach the replacement key store", reloadStarted.await(30, TimeUnit.SECONDS));

                for (int port : ports)
                    assertEquals(certificate(KEYS_1), connectAndGetServerCertificate(port, KEYS_1, TRUST_1));

                client.cacheNames();
            }
            finally {
                allowReload.countDown();
            }

            reload.get(30, TimeUnit.SECONDS);

            for (int port : ports)
                assertEquals(certificate(KEYS_1), connectAndGetServerCertificate(port, KEYS_1, TRUST_1));

            client.cacheNames();
        }
    }

    /** */
    @Test
    public void testRestInitializesContextOnceOnPortRetry() throws Exception {
        SSLContext ctx = sslTrustedFactory(KEYS_1, TRUST_1).create();
        AtomicInteger factoryCalls = new AtomicInteger();

        try (ServerSocket occupied = new ServerSocket()) {
            occupied.bind(new InetSocketAddress(LOOPBACK_ADDRESS, GridTestUtils.getFreeCommPort()));

            IgniteEx node = startGrid(0, cfg -> {
                ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

                cfg.setClientConnectorConfiguration(null)
                    .setConnectorConfiguration(new ConnectorConfiguration()
                        .setPort(occupied.getLocalPort())
                        .setSslEnabled(true)
                        .setSslFactory(() -> {
                            factoryCalls.incrementAndGet();

                            return ctx;
                        }));
            });

            int port = node.localNode().attribute(ATTR_REST_TCP_PORT);

            assertTrue("REST did not retry the occupied port", port > occupied.getLocalPort());
            assertEquals("SSL context must be initialized once before the first connection", 1, factoryCalls.get());
            assertEquals(certificate(KEYS_1), connectAndGetServerCertificate(port, KEYS_1, TRUST_1));
        }
    }

    /** */
    @Test
    public void testThinClientReconnectUsesReloadedContext() throws Exception {
        ReloadableSslContextFactory clientFactory = new ReloadableSslContextFactory(KEYS_1, TRUST_1);
        IgniteEx node = startNode(0, sslTrustedFactory(KEYS_1, TRUST_1));

        ClientConfiguration cfg = clientConfiguration(node).setSslContextFactory(clientFactory::create);

        try (IgniteClient client = Ignition.startClient(cfg)) {
            client.cacheNames();

            stopGrid(0);

            assertThrows(ClientConnectionException.class, client::cacheNames);

            clientFactory.reload(KEYS_3, TRUST_2);

            startNode(0, sslTrustedFactory(KEYS_2, TRUST_2));

            client.cacheNames();
        }
    }

    /** */
    @Test
    public void testThinClientReconnectDoesNotReloadKeyStore() throws Exception {
        Factory<SSLContext> serverFactory = sslTrustedFactory(KEYS_1, TRUST_1);
        IgniteEx node = startNode(0, serverFactory);
        Path keyStore = Files.createTempFile("thin-client-keys", ".jks");
        final String storeType = "JKS";

        try {
            Files.copy(Path.of(keyStorePath(KEYS_1)), keyStore, StandardCopyOption.REPLACE_EXISTING);

            ClientConfiguration cfg = clientConfiguration(node)
                .setSslClientCertificateKeyStorePath(keyStore.toString())
                .setSslClientCertificateKeyStoreType(storeType)
                .setSslClientCertificateKeyStorePassword(keyStorePassword())
                .setSslTrustCertificateKeyStorePath(keyStorePath(TRUST_1))
                .setSslTrustCertificateKeyStoreType(storeType)
                .setSslTrustCertificateKeyStorePassword(keyStorePassword());

            try (IgniteClient client = Ignition.startClient(cfg)) {
                client.cacheNames();

                Files.delete(keyStore);
                stopGrid(0);

                assertThrows(ClientConnectionException.class, client::cacheNames);

                startNode(0, serverFactory);

                client.cacheNames();
            }
        }
        finally {
            Files.deleteIfExists(keyStore);
        }
    }

    /** */
    @Test
    public void testThinClientReportsContextFactoryFailure() throws Exception {
        IgniteEx node = startNode(0, sslTrustedFactory(KEYS_1, TRUST_1));
        IllegalStateException failure = new IllegalStateException("Cannot load client TLS credentials");

        ClientConfiguration cfg = new ClientConfiguration()
            .setAddresses(LOOPBACK_ADDRESS + ":" + node.context().clientListener().port())
            .setSslMode(SslMode.REQUIRED)
            .setSslContextFactory(() -> {
                throw failure;
            });

        ClientConnectionException err = assertThrows(ClientConnectionException.class, () -> Ignition.startClient(cfg).close());

        assertSame(failure, X.cause(err, IllegalStateException.class));
    }

    /** */
    private IgniteEx startNode(int idx, Factory<SSLContext> factory) throws Exception {
        return startGrid(idx, cfg -> {
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

            // Paired connections ensure the compute request opens or uses an outgoing connection.
            ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setUsePairedConnections(true);

            cfg.setSslContextFactory(factory)
                .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                    .setSslEnabled(true)
                    .setSslClientAuth(true))
                .setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true).setSslClientAuth(true));
        });
    }

    /** */
    private static int[] tlsPorts(IgniteEx node) {
        return new int[] {
            ((TcpDiscoverySpi)node.configuration().getDiscoverySpi()).getLocalPort(),
            ((TcpCommunicationSpi)node.configuration().getCommunicationSpi()).boundPort(),
            node.context().clientListener().port(),
            node.localNode().<Integer>attribute(ATTR_REST_TCP_PORT)
        };
    }

    /** */
    private static ClientConfiguration clientConfiguration(IgniteEx node) {
        return new ClientConfiguration()
            .setAddresses(LOOPBACK_ADDRESS + ":" + node.context().clientListener().port())
            .setRequestTimeout(SOCKET_TIMEOUT)
            .setSslMode(SslMode.REQUIRED)
            .setRetryPolicy(new ClientRetryNonePolicy())
            .setPartitionAwarenessEnabled(false)
            .setClusterDiscoveryEnabled(false);
    }

    /** */
    private static Certificate certificate(String keys) throws Exception {
        return KeyStore.getInstance(Path.of(keyStorePath(keys)).toFile(), keyStorePassword().toCharArray()).getCertificate(keys);
    }

    /** A fresh TLS context prevents session resumption. */
    private static Certificate connectAndGetServerCertificate(int port, String keys, String trust) throws IOException {
        SSLContext ctx = sslTrustedFactory(keys, trust).create();

        try (SSLSocket sock = (SSLSocket)ctx.getSocketFactory().createSocket()) {
            // TLS 1.2 completes the client handshake only after the server checks the client certificate.
            sock.setEnabledProtocols(new String[] {"TLSv1.2"});
            sock.connect(new InetSocketAddress(LOOPBACK_ADDRESS, port), SOCKET_TIMEOUT);
            sock.setSoTimeout(SOCKET_TIMEOUT);

            try {
                sock.startHandshake();
            }
            catch (SocketException e) {
                throw new SSLException(e);
            }

            return sock.getSession().getPeerCertificates()[0];
        }
    }

    /** */
    private static class ReloadableSslContextFactory extends SslContextFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private ReloadableSslContextFactory(String keys, String trust) {
            setKeyStoreFilePath(keyStorePath(keys));
            setKeyStorePassword(keyStorePassword().toCharArray());
            setTrustStoreFilePath(keyStorePath(trust));
            setTrustStorePassword(keyStorePassword().toCharArray());
        }

        /** */
        private void reload(String keys, String trust) {
            setKeyStoreFilePath(keyStorePath(keys));
            setTrustStoreFilePath(keyStorePath(trust));

            init();
        }
    }
}
