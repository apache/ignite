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

package org.apache.ignite.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.sql.DriverManager.getConnection;
import static org.apache.ignite.Ignition.startClient;
import static org.apache.ignite.internal.client.GridClientFactory.start;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.DISCO_METRICS;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLIENT_CONNECTOR_METRIC_GROUP_NAME;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.GridTcpRestProtocol.REST_CONNECTOR_METRIC_GROUP_NAME;
import static org.apache.ignite.internal.util.nio.GridNioServer.SESSIONS_CNT_METRIC_NAME;
import static org.apache.ignite.internal.util.nio.GridNioServer.SSL_ENABLED_METRIC_NAME;
import static org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter.SSL_HANDSHAKE_DURATION_HISTOGRAM_METRIC_NAME;
import static org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter.SSL_REJECTED_SESSIONS_CNT_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.SSL_REJECTED_CONNECTIONS_CNT_METRIC_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Checks SSL metrics for all types of connections to the node. */
public class SslConnectorsMetricTest extends GridCommonAbstractTest {
    /** Cipher suite supported by cluster nodes. */
    private static final String CIPHER_SUITE = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";

    /** Cipher suite not supported by cluster nodes. */
    private static final String NON_SUP_CIPHER_SUITE = "TLS_RSA_WITH_AES_128_GCM_SHA256";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);
    }

    /** Checks the status of the SSL metric if SSL is not configured on the node. */
    @Test
    public void testSslDisabled() throws Exception {
        IgniteEx srv = startClusterNode(0, false);

        assertFalse(reg(srv, DISCO_METRICS).<BooleanMetric>findMetric(SSL_ENABLED_METRIC_NAME).value());
        assertFalse(reg(srv, COMMUNICATION_METRICS_GROUP_NAME).<BooleanMetric>findMetric(SSL_ENABLED_METRIC_NAME).value());
        assertFalse(reg(srv, CLIENT_CONNECTOR_METRIC_GROUP_NAME).<BooleanMetric>findMetric(SSL_ENABLED_METRIC_NAME).value());
        assertFalse(reg(srv, REST_CONNECTOR_METRIC_GROUP_NAME).<BooleanMetric>findMetric(SSL_ENABLED_METRIC_NAME).value());
        assertNull(reg(srv, DISCO_METRICS).<BooleanMetric>findMetric(SSL_REJECTED_CONNECTIONS_CNT_METRIC_NAME));
        assertNull(reg(srv, COMMUNICATION_METRICS_GROUP_NAME).<BooleanMetric>findMetric(SSL_REJECTED_SESSIONS_CNT_METRIC_NAME));
        assertNull(reg(srv, CLIENT_CONNECTOR_METRIC_GROUP_NAME).<BooleanMetric>findMetric(SSL_REJECTED_SESSIONS_CNT_METRIC_NAME));
        assertNull(reg(srv, REST_CONNECTOR_METRIC_GROUP_NAME).<BooleanMetric>findMetric(SSL_REJECTED_SESSIONS_CNT_METRIC_NAME));
        assertNull(reg(srv, COMMUNICATION_METRICS_GROUP_NAME).<BooleanMetric>findMetric(SSL_HANDSHAKE_DURATION_HISTOGRAM_METRIC_NAME));
        assertNull(reg(srv, CLIENT_CONNECTOR_METRIC_GROUP_NAME).<BooleanMetric>findMetric(SSL_HANDSHAKE_DURATION_HISTOGRAM_METRIC_NAME));
        assertNull(reg(srv, REST_CONNECTOR_METRIC_GROUP_NAME).<BooleanMetric>findMetric(SSL_HANDSHAKE_DURATION_HISTOGRAM_METRIC_NAME));
        assertNotNull(reg(srv, COMMUNICATION_METRICS_GROUP_NAME).<BooleanMetric>findMetric(SESSIONS_CNT_METRIC_NAME));
        assertNotNull(reg(srv, CLIENT_CONNECTOR_METRIC_GROUP_NAME).<BooleanMetric>findMetric(SESSIONS_CNT_METRIC_NAME));
        assertNotNull(reg(srv, REST_CONNECTOR_METRIC_GROUP_NAME).<BooleanMetric>findMetric(SESSIONS_CNT_METRIC_NAME));
    }

    /** Tests SSL metrics produced by JDBC connection. */
    @Test
    public void testJdbc() throws Exception {
        IgniteEx srv = startClusterNode(0, true);

        MetricRegistry reg = reg(srv, CLIENT_CONNECTOR_METRIC_GROUP_NAME);

        try (Connection ignored = getConnection(jdbcConnectionConfiguration("thinClient", "trusttwo", CIPHER_SUITE))) {
            assertSslCommunicationMetrics(reg, 1, 1, 0);
        }

        assertSslCommunicationMetrics(reg, 1, 0, 0);

        assertThrowsWithCause(() -> getConnection(jdbcConnectionConfiguration("client", "trusttwo", CIPHER_SUITE)), SQLException.class);
        assertSslCommunicationMetrics(reg, 2, 0, 1);

        assertThrowsWithCause(() ->
            getConnection(jdbcConnectionConfiguration("thinClient", "trusttwo", NON_SUP_CIPHER_SUITE)),
            SQLException.class
        );
        assertSslCommunicationMetrics(reg, 3, 0, 2);
    }

    /** Tests SSL metrics produced by REST TCP client connection. */
    @Test
    public void testRestClientConnector() throws Exception {
        IgniteEx srv = startClusterNode(0, true);

        MetricRegistry reg = reg(srv, REST_CONNECTOR_METRIC_GROUP_NAME);

        try (
            GridClient ignored = start(gridClientConfiguration("connectorClient", "trustthree", CIPHER_SUITE))
        ) {
            assertSslCommunicationMetrics(reg, 1, 1, 0);
        }

        assertSslCommunicationMetrics(reg, 1, 0, 0);

        try (GridClient ignored = start(gridClientConfiguration("client", "trustthree", CIPHER_SUITE))) {
            // GridClient makes 2 additional connection attempts if an SSL error occurs and does not throw an exception if they all fail.
        }

        assertSslCommunicationMetrics(reg, 4, 0, 3);

        try (GridClient ignored = start(gridClientConfiguration("connectorClient", "trustthree", NON_SUP_CIPHER_SUITE))) {
            // GridClient makes 2 additional connection attempts if an SSL error occurs and does not throw an exception if they all fail.
        }

        assertSslCommunicationMetrics(reg, 7, 0, 6);
    }

    /** Tests SSL discovery metrics produced by node connection. */
    @Test
    public void testDiscovery() throws Exception {
        IgniteEx srv = startClusterNode(0, true);

        startGrid(nodeConfiguration(1, true, "client", "trustone", CIPHER_SUITE, "TLSv1.2"));

        assertTrue(reg(srv, DISCO_METRICS).<BooleanMetric>findMetric(SSL_ENABLED_METRIC_NAME).value());
        assertEquals(0, reg(srv, DISCO_METRICS).<LongMetric>findMetric(SSL_REJECTED_CONNECTIONS_CNT_METRIC_NAME).value());

        assertNodeJoinFails(2, true, "thinClient", "trusttwo", CIPHER_SUITE, "TLSv1.2");
        assertNodeJoinFails(2, false, "thinClient", "trusttwo", CIPHER_SUITE, "TLSv1.2");
        assertNodeJoinFails(2, true, "client", "trustone", NON_SUP_CIPHER_SUITE, "TLSv1.2");
        assertNodeJoinFails(2, false, "node01", "trustone", NON_SUP_CIPHER_SUITE, "TLSv1.2");
        assertNodeJoinFails(2, true, "client", "trustone", CIPHER_SUITE, "TLSv1.1");
        assertNodeJoinFails(2, false, "node01", "trustone", CIPHER_SUITE, "TLSv1.1");

        // In case of an SSL error, the client and server nodes make 2 additional connection attempts.
        assertTrue(waitForCondition(() ->
            18 == reg(srv, DISCO_METRICS).<LongMetric>findMetric(SSL_REJECTED_CONNECTIONS_CNT_METRIC_NAME).value(),
            getTestTimeout()
        ));
    }

    /** Tests SSL communication metrics produced by node connection. */
    @Test
    public void testCommunication() throws Exception {
        IgniteEx srv = startClusterNode(0, true);

        MetricRegistry reg = reg(srv, COMMUNICATION_METRICS_GROUP_NAME);

        assertSslCommunicationMetrics(reg, 0, 0, 0);

        try (
            IgniteEx cliNode = startGrid(nodeConfiguration(1, true, "client", "trustone", CIPHER_SUITE, "TLSv1.2"));
            IgniteEx srvNode = startGrid(nodeConfiguration(2, true, "client", "trustone", CIPHER_SUITE, "TLSv1.2"))
        ) {
            assertSslCommunicationMetrics(reg, 2, 2, 0);
            assertSslCommunicationMetrics(reg(cliNode, COMMUNICATION_METRICS_GROUP_NAME), 0, 1, 0);
            assertSslCommunicationMetrics(reg(srvNode, COMMUNICATION_METRICS_GROUP_NAME), 0, 1, 0);
        }

        assertSslCommunicationMetrics(reg, 2, 0, 0);
    }

    /** Tests SSL metrics produced by thin client connection. */
    @Test
    public void testClientConnector() throws Exception {
        IgniteEx srv = startClusterNode(0, true);

        MetricRegistry reg = reg(srv, CLIENT_CONNECTOR_METRIC_GROUP_NAME);

        try (IgniteClient ignored = startClient(clientConfiguration("thinClient", "trusttwo", CIPHER_SUITE))) {
            assertSslCommunicationMetrics(reg, 1, 1, 0);
        }

        assertSslCommunicationMetrics(reg, 1, 0, 0);

        assertThrowsWithCause(() -> startClient(clientConfiguration("client", "trustboth", CIPHER_SUITE)), ClientConnectionException.class);
        assertSslCommunicationMetrics(reg, 2, 0, 1);

        assertThrowsWithCause(() ->
            startClient(clientConfiguration("thinClient", "trusttwo", NON_SUP_CIPHER_SUITE)),
            ClientConnectionException.class
        );

        assertSslCommunicationMetrics(reg, 3, 0, 2);
    }

    /** Starts node that imitates a cluster node to which connections will be performed. */
    private IgniteEx startClusterNode(int idx, boolean sslEnabled) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        if (sslEnabled)
            cfg.setSslContextFactory(sslContextFactory("server", "trustone", CIPHER_SUITE, "TLSv1.2"));

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setSslEnabled(sslEnabled)
            .setSslClientAuth(sslEnabled)
            .setUseIgniteSslContextFactory(false)
            .setSslContextFactory(sslContextFactory("thinServer", "trusttwo", CIPHER_SUITE, "TLSv1.2")));

        cfg.setConnectorConfiguration(new ConnectorConfiguration()
            .setSslClientAuth(sslEnabled)
            .setSslEnabled(sslEnabled)
            .setSslFactory(sslContextFactory("connectorServer", "trustthree", CIPHER_SUITE, "TLSv1.2")));

        return startGrid(cfg);
    }

    /** @return JDBC connection configuration with specified SSL options. */
    private String jdbcConnectionConfiguration(String keyStore, String trustStore, String cipherSuite) {
        return "jdbc:ignite:thin://" + Config.SERVER + "?sslMode=require" +
            "&sslClientCertificateKeyStoreUrl=" + keyStorePath(keyStore) +
            "&sslClientCertificateKeyStorePassword=" + keyStorePassword() +
            "&sslTrustCertificateKeyStoreUrl=" + keyStorePath(trustStore) +
            "&sslTrustCertificateKeyStorePassword=" + keyStorePassword() +
            "&sslCipherSuites=" + cipherSuite;
    }

    /** @return Node connection configuration with specified SSL options. */
    private IgniteConfiguration nodeConfiguration(
        int idx,
        boolean client,
        String keyStore,
        String trustStore,
        String cipherSuite,
        String protocol
    ) throws Exception {
       return getConfiguration(getTestIgniteInstanceName(idx))
            .setSslContextFactory(sslContextFactory(keyStore, trustStore, cipherSuite, protocol))
            .setClientMode(client);
    }

    /** @return Grid client connection configuration with specified SSL options. */
    private GridClientConfiguration gridClientConfiguration(String keyStore, String trustStore, String cipherSuite) {
        SslContextFactory sslCtxFactory = sslContextFactory(keyStore, trustStore, cipherSuite, "TLSv1.2");

        return new GridClientConfiguration()
            .setServers(Collections.singleton("127.0.0.1:11211"))
            .setSslContextFactory(sslCtxFactory::create);
    }

    /** @return Thin client connection configuration with specified SSL options. */
    private ClientConfiguration clientConfiguration(String keyStore, String trustStore, String cipherSuite) {
       return new ClientConfiguration()
            .setAddresses("127.0.0.1:10800")
            .setSslMode(SslMode.REQUIRED)
            .setSslContextFactory(sslContextFactory(keyStore, trustStore, cipherSuite, "TLSv1.2"));
    }

    /** Asserts that the node connection failed if the connection was performed with the specified SSL options. */
    @SuppressWarnings("ThrowableNotThrown")
    private void assertNodeJoinFails(
        int idx,
        boolean client,
        String keyStore,
        String trustStore,
        String cipherSuite,
        String protocol
    ) throws Exception {
        IgniteConfiguration cfg = nodeConfiguration(idx, client, keyStore, trustStore, cipherSuite, protocol);

        if (client)
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(1); // To avoid client rejoin after failed connection.

        GridTestUtils.assertThrowsWithCause(() -> startGrid(cfg), IgniteCheckedException.class);
    }

    /** Obtains the metric registry with the specified name from Ignite instance. */
    private MetricRegistry reg(IgniteEx ignite, String name) {
        return ignite.context().metric().registry(name);
    }

    /** Checks SSL communication metrics. */
    private void assertSslCommunicationMetrics(MetricRegistry mreg, long handshakeCnt, long sesCnt, long rejectedSesCnt) throws Exception {
        assertEquals(true, mreg.<BooleanMetric>findMetric(SSL_ENABLED_METRIC_NAME).value());
        assertTrue(waitForCondition(() -> sesCnt == mreg.<IntMetric>findMetric(SESSIONS_CNT_METRIC_NAME).value(), getTestTimeout()));
        assertTrue(waitForCondition(() ->
            handshakeCnt == Arrays.stream(mreg.<HistogramMetric>findMetric(SSL_HANDSHAKE_DURATION_HISTOGRAM_METRIC_NAME).value()).sum(),
            getTestTimeout()
        ));
        assertTrue(waitForCondition(() ->
            rejectedSesCnt == mreg.<LongMetric>findMetric(SSL_REJECTED_SESSIONS_CNT_METRIC_NAME).value(),
            getTestTimeout()
        ));
    }

    /** Creates {@link SslContextFactory} with specified options. */
    private SslContextFactory sslContextFactory(String keyStore, String trustStore, String cipherSuite, String protocol) {
        SslContextFactory res = (SslContextFactory)sslTrustedFactory(keyStore, trustStore);

        res.setCipherSuites(cipherSuite);
        res.setProtocol(protocol);

        return res;
    }
}
