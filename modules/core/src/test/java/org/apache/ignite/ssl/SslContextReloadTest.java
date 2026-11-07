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
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
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
import org.apache.ignite.internal.processors.rest.client.message.GridClientPingPacket;
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
import static org.junit.Assert.assertArrayEquals;
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
    private static final String LOOPBACK_ADDRESS = "127.0.0.1";

    /** Discovery addresses shared within one test. */
    private final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testNewConnectionsUseReloadedContext() throws Exception {
        SslContextFactory serverFactory = sslFactory(KEYS_1, trustIssuersOf(KEYS_1));
        IgniteEx node = startNode(serverFactory::create);

        serverFactory.setKeyStoreFilePath(keyStorePath(KEYS_2));
        serverFactory.setTrustManagers(trustIssuersOf(KEYS_2));
        serverFactory.init();

        SSLContext client = sslFactory(KEYS_2, trustIssuersOf(KEYS_2)).create();

        for (int port : tlsPorts(node))
            connectAndHandshake(port, client);
    }

    /** */
    @Test
    public void testExistingConnectionSurvivesReload() throws Exception {
        SslContextFactory serverFactory = sslFactory(KEYS_1, trustIssuersOf(KEYS_1));
        IgniteEx node = startNode(serverFactory);

        ClientConfiguration cfg = clientConfiguration(node).setSslContextFactory(sslFactory(KEYS_1, trustIssuersOf(KEYS_1)));

        try (IgniteClient client = Ignition.startClient(cfg)) {
            client.cacheNames();

            serverFactory.setKeyStoreFilePath(keyStorePath(KEYS_2));
            serverFactory.setTrustManagers(trustIssuersOf(KEYS_2));
            serverFactory.init();

            client.cacheNames();
        }
    }

    /** */
    @Test
    public void testNewConnectionsDoNotWaitForReload() throws Exception {
        String replacementKeyStore = keyStorePath(KEYS_2);
        CountDownLatch reloadStarted = new CountDownLatch(1);
        CountDownLatch allowReload = new CountDownLatch(1);

        SslContextFactory factory = new SslContextFactory() {
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

        TrustManager[] trustedCertificates = trustIssuersOf(KEYS_1);

        factory.setKeyStoreFilePath(keyStorePath(KEYS_1));
        factory.setKeyStorePassword(keyStorePassword().toCharArray());
        factory.setTrustManagers(trustedCertificates);

        IgniteEx node = startNode(factory);
        SSLContext client = sslFactory(KEYS_1, trustedCertificates).create();

        factory.setKeyStoreFilePath(replacementKeyStore);

        IgniteInternalFuture<?> reload = GridTestUtils.runAsync(factory::init);

        try {
            assertTrue("Reload did not reach the replacement key store", reloadStarted.await(30, TimeUnit.SECONDS));

            for (int port : tlsPorts(node))
                connectAndHandshake(port, client);
        }
        finally {
            allowReload.countDown();
            reload.get(30, TimeUnit.SECONDS);
        }
    }

    /** */
    @Test
    public void testRestInitializesContextOnceOnPortRetry() throws Exception {
        SSLContext ctx = sslFactory(KEYS_1, trustIssuersOf(KEYS_1)).create();
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

            try (SSLSocket sock = (SSLSocket)ctx.getSocketFactory().createSocket()) {
                sock.connect(new InetSocketAddress(LOOPBACK_ADDRESS, port), SOCKET_TIMEOUT);
                sock.setSoTimeout(SOCKET_TIMEOUT);
                sock.startHandshake();
                sock.getOutputStream().write(GridClientPingPacket.PING_PACKET);

                assertArrayEquals(GridClientPingPacket.PING_PACKET,
                    sock.getInputStream().readNBytes(GridClientPingPacket.PING_PACKET.length));
            }
        }
    }

    /** */
    @Test
    public void testThinClientReconnectUsesReloadedContext() throws Exception {
        SslContextFactory clientFactory = sslFactory(KEYS_1, trustIssuersOf(KEYS_1));
        IgniteEx node = startNode(sslFactory(KEYS_1, trustIssuersOf(KEYS_1)));

        ClientConfiguration cfg = clientConfiguration(node).setSslContextFactory(clientFactory::create);

        try (IgniteClient client = Ignition.startClient(cfg)) {
            client.cacheNames();

            stopGrid(0);

            assertThrows(ClientConnectionException.class, client::cacheNames);

            clientFactory.setKeyStoreFilePath(keyStorePath(KEYS_2));
            clientFactory.setTrustManagers(trustIssuersOf(KEYS_2));
            clientFactory.init();

            startNode(sslFactory(KEYS_2, trustIssuersOf(KEYS_2)));

            client.cacheNames();
        }
    }

    /** */
    @Test
    public void testThinClientReconnectDoesNotReloadKeyStore() throws Exception {
        Factory<SSLContext> serverFactory = sslFactory(KEYS_1, trustIssuersOf(KEYS_1));
        IgniteEx node = startNode(serverFactory);
        Path keyStore = Files.createTempFile("thin-client-keys", ".jks");
        final String storeType = "JKS";

        try {
            Files.copy(Path.of(keyStorePath(KEYS_1)), keyStore, StandardCopyOption.REPLACE_EXISTING);

            ClientConfiguration cfg = clientConfiguration(node)
                .setSslClientCertificateKeyStorePath(keyStore.toString())
                .setSslClientCertificateKeyStoreType(storeType)
                .setSslClientCertificateKeyStorePassword(keyStorePassword())
                .setSslTrustCertificateKeyStorePath(keyStorePath(KEYS_1))
                .setSslTrustCertificateKeyStoreType(storeType)
                .setSslTrustCertificateKeyStorePassword(keyStorePassword());

            try (IgniteClient client = Ignition.startClient(cfg)) {
                client.cacheNames();

                Files.delete(keyStore);
                stopGrid(0);

                assertThrows(ClientConnectionException.class, client::cacheNames);

                startNode(serverFactory);

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
        IgniteEx node = startNode(sslFactory(KEYS_1, trustIssuersOf(KEYS_1)));
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
    private IgniteEx startNode(Factory<SSLContext> factory) throws Exception {
        return startGrid(0, cfg -> {
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

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
    private static void connectAndHandshake(int port, SSLContext ctx) throws IOException {
        try (SSLSocket sock = (SSLSocket)ctx.getSocketFactory().createSocket()) {
            sock.connect(new InetSocketAddress(LOOPBACK_ADDRESS, port), SOCKET_TIMEOUT);
            sock.setSoTimeout(SOCKET_TIMEOUT);
            sock.startHandshake();
        }
    }

    /** */
    private static SslContextFactory sslFactory(String keys, TrustManager[] trustManagers) {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStorePath(keys));
        factory.setKeyStorePassword(keyStorePassword().toCharArray());
        factory.setTrustManagers(trustManagers);

        return factory;
    }

    /** */
    private static TrustManager[] trustIssuersOf(String keys) throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");

        trustStore.load(null, null);

        KeyStore keyStore = KeyStore.getInstance(Path.of(keyStorePath(keys)).toFile(), keyStorePassword().toCharArray());
        Certificate[] chain = keyStore.getCertificateChain(keys);

        for (int i = 1; i < chain.length; i++)
            trustStore.setCertificateEntry(keys + i, chain[i]);

        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        factory.init(trustStore);

        return factory.getTrustManagers();
    }
}
