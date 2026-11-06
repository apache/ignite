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
import java.net.SocketException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientRetryNonePolicy;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
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

    /** Discovery addresses shared within one test. */
    private final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** An established thin client survives rotation. */
    @Test
    public void testRotateKeyAndTrust() throws Exception {
        ReloadableSslContextFactory factory = new ReloadableSslContextFactory();
        IgniteEx node = startNode(0, factory);
        int[] ports = tlsPorts(node);

        for (int port : ports)
            assertCertificate(port, "node01", "trustone", "CN=node01");

        // This client trusts only the original CA, so it cannot reconnect after the server rotates.
        try (IgniteClient client = thinClient(node)) {
            client.cacheNames();

            factory.reload("node02", "trusttwo");

            for (int port : ports) {
                assertCertificate(port, "node03", "trusttwo", "CN=node02");
                assertClientCertificateRejected(port);
            }

            client.cacheNames();
        }

        // The joining server trusts only the new CA. It did not exist before the reload.
        IgniteEx peer = startNode(1, sslTrustedFactory("node03", "trusttwo"));

        assertTrue(((TcpDiscoverySpi)node.configuration().getDiscoverySpi()).pingNode(peer.localNode().id()));

        int result = node.compute(node.cluster().forNodeId(peer.localNode().id())).call(() -> 1);

        assertEquals(1, result);
    }

    /** Checks both new and established connections. */
    @Test
    public void testUsePreviousContextDuringAndAfterFailedReload() throws Exception {
        String replacement = keyStorePath("node02");
        CountDownLatch opening = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);

        ReloadableSslContextFactory factory = new ReloadableSslContextFactory() {
            /** */
            private static final long serialVersionUID = 0L;

            /** {@inheritDoc} */
            @Override protected InputStream openFileInputStream(String path) throws IOException {
                if (replacement.equals(path)) {
                    opening.countDown();

                    try {
                        if (!resume.await(60, TimeUnit.SECONDS))
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

        try (IgniteClient client = thinClient(node)) {
            client.cacheNames();

            factory.setKeyStorePassword("wrong-password".toCharArray());

            IgniteInternalFuture<?> reload = GridTestUtils.runAsync(() -> {
                assertThrows(IgniteException.class, () -> factory.reload("node02", "trusttwo"));
            });

            try {
                assertTrue("Reload did not reach the replacement key store", opening.await(30, TimeUnit.SECONDS));

                for (int port : ports)
                    assertCertificate(port, "node01", "trustone", "CN=node01");

                client.cacheNames();
            }
            finally {
                resume.countDown();
            }

            reload.get(30, TimeUnit.SECONDS);

            for (int port : ports)
                assertCertificate(port, "node01", "trustone", "CN=node01");

            client.cacheNames();
        }
    }

    /** All four listeners share one factory. */
    private IgniteEx startNode(int idx, Factory<SSLContext> factory) throws Exception {
        return startGrid(idx, cfg -> {
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

            // Paired connections ensure the compute request opens or uses an outgoing connection.
            ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setUsePairedConnections(true);

            cfg.setSslContextFactory(factory)
                .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                    .setSslEnabled(true)
                    .setSslClientAuth(true)
                    .setUseIgniteSslContextFactory(true))
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

    /** Thin client that trusts only the original CA. */
    private static IgniteClient thinClient(IgniteEx node) {
        return Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:" + node.context().clientListener().port())
            .setTimeout(SOCKET_TIMEOUT)
            .setSslMode(SslMode.REQUIRED)
            .setSslContextFactory(sslTrustedFactory("node01", "trustone"))
            .setRetryPolicy(new ClientRetryNonePolicy())
            .setPartitionAwarenessEnabled(false)
            .setClusterDiscoveryEnabled(false));
    }

    /** Checks the server certificate after mutual TLS. */
    private static void assertCertificate(int port, String keys, String trust, String subject) throws IOException {
        try (SSLSocket sock = probeSocket(port, keys, trust)) {
            sock.startHandshake();

            assertEquals("Peer certificate on port " + port, subject, sock.getSession().getPeerPrincipal().getName());
        }
    }

    /** The probe trusts the replacement server certificate. */
    private static void assertClientCertificateRejected(int port) throws IOException {
        try (SSLSocket sock = probeSocket(port, "node01", "trustboth")) {
            try {
                sock.startHandshake();

                fail("Port " + port + " accepted a client from the old CA");
            }
            catch (SSLException | SocketException expected) {
                // A TLS alert or connection reset can report a rejected client certificate.
            }
        }
    }

    /** A fresh TLS context prevents session resumption. */
    private static SSLSocket probeSocket(int port, String keys, String trust) throws IOException {
        SSLContext ctx = sslTrustedFactory(keys, trust).create();
        SSLSocket sock = (SSLSocket)ctx.getSocketFactory().createSocket();

        try {
            // TLS 1.2 completes the client handshake only after the server checks the client certificate.
            sock.setEnabledProtocols(new String[] {"TLSv1.2"});
            sock.connect(new InetSocketAddress("127.0.0.1", port), SOCKET_TIMEOUT);
            sock.setSoTimeout(SOCKET_TIMEOUT);

            return sock;
        }
        catch (IOException | RuntimeException e) {
            U.closeQuiet(sock);

            throw e;
        }
    }

    /** Test factory with access to {@code init()}. */
    private static class ReloadableSslContextFactory extends SslContextFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private ReloadableSslContextFactory() {
            setKeyStoreFilePath(keyStorePath("node01"));
            setKeyStorePassword(keyStorePassword().toCharArray());
            setTrustStoreFilePath(keyStorePath("trustone"));
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
