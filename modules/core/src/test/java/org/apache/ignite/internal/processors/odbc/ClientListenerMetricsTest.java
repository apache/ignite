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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLIENT_CONNECTOR_METRICS;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_ACEPTED;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_AUTHENTICATION;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_TIMEOUT;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_TOTAL;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.METRIC_ACTIVE;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_STORE_TYPE;

/**
 * Client listener metrics tests.
 */
public class ClientListenerMetricsTest extends GridCommonAbstractTest {
    /**
     * Check that valid connections and disconnections to the grid affect metrics.
     */
    @Test
    public void testClientListenerMetricsAccept() throws Exception {
        try (IgniteEx ignite = startGrid(0)) {
            MetricRegistry mreg = ignite.context().metric().registry(CLIENT_CONNECTOR_METRICS);

            checkConnectionsMetrics(mreg, 0, 0);

            IgniteClient client0 = Ignition.startClient(getClientConfiguration());

            checkConnectionsMetrics(mreg, 1, 1);

            client0.close();

            checkConnectionsMetrics(mreg, 1, 0);

            IgniteClient client1 = Ignition.startClient(getClientConfiguration());

            checkConnectionsMetrics(mreg, 2, 1);

            IgniteClient client2 = Ignition.startClient(getClientConfiguration());

            checkConnectionsMetrics(mreg, 3, 2);

            client1.close();

            checkConnectionsMetrics(mreg, 3, 1);

            client2.close();

            checkConnectionsMetrics(mreg, 3, 0);
        }
    }

    /**
     * Check that failed connection attempts to the grid affect metrics.
     */
    @Test
    public void testClientListenerMetricsReject() throws Exception {
        cleanPersistenceDir();

        IgniteConfiguration nodeCfg = getConfiguration()
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setHandshakeTimeout(2000))
            .setAuthenticationEnabled(true)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        try (IgniteEx ignite = startGrid(nodeCfg)) {
            ignite.cluster().state(ClusterState.ACTIVE);
            MetricRegistry mreg = ignite.context().metric().registry(CLIENT_CONNECTOR_METRICS);

            checkRejectMetrics(mreg, 0, 0, 0);

            ClientConfiguration cfgSsl = getClientConfiguration()
                .setSslMode(SslMode.REQUIRED)
                .setSslClientCertificateKeyStorePath(GridTestUtils.keyStorePath("client"))
                .setSslClientCertificateKeyStoreType(DFLT_STORE_TYPE)
                .setSslClientCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStorePath(GridTestUtils.keyStorePath("trustone"))
                .setSslTrustCertificateKeyStoreType(DFLT_STORE_TYPE)
                .setSslTrustCertificateKeyStorePassword("123456");

            GridTestUtils.assertThrows(log, () -> {
                Ignition.startClient(cfgSsl);
                return null;
            }, ClientException.class, null);

            checkRejectMetrics(mreg, 1, 0, 1);

            ClientConfiguration cfgAuth = getClientConfiguration()
                .setUserName("SomeRandomInvalidName")
                .setUserPassword("42");

            GridTestUtils.assertThrows(log, () -> {
                Ignition.startClient(cfgAuth);
                return null;
            }, ClientAuthenticationException.class, null);

            checkRejectMetrics(mreg, 1, 1, 2);
        }
    }

    /**
     * Check that failed connection attempts to the grid affect metrics.
     */
    @Test
    public void testClientListenerMetricsRejectGeneral() throws Exception {
        IgniteConfiguration nodeCfg = getConfiguration()
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setThinClientEnabled(false));

        try (IgniteEx ignite = startGrid(nodeCfg)) {
            MetricRegistry mreg = ignite.context().metric().registry(CLIENT_CONNECTOR_METRICS);

            checkRejectMetrics(mreg, 0, 0, 0);

            GridTestUtils.assertThrows(log, () -> {
                Ignition.startClient(getClientConfiguration());
                return null;
            }, RuntimeException.class, "Thin client connection is not allowed");

            checkRejectMetrics(mreg, 0, 0, 1);
        }
    }

    /** */
    private static ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration()
            .setAddresses(Config.SERVER)
            // When PA is enabled, async client channel init executes and spoils the metrics.
            .setPartitionAwarenessEnabled(false)
            .setSendBufferSize(0)
            .setReceiveBufferSize(0);
    }

    /**
     * Wait for specific metric to change
     * @param mreg Metric registry.
     * @param metric Metric to check.
     * @param value Metric value to wait for.
     * @param timeout Timeout.
     */
    private void waitForMetricValue(MetricRegistry mreg, String metric, long value, long timeout)
        throws IgniteInterruptedCheckedException {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return mreg.<IntMetric>findMetric(metric).value() == value;
            }
        }, timeout);
        assertEquals(mreg.<IntMetric>findMetric(metric).value(), value);
    }

    /**
     * Check client metrics
     * @param mreg Client metric registry
     * @param rejectedTimeout Expected number of connection attepmts rejected by timeout.
     * @param rejectedAuth Expected number of connection attepmts rejected because of failed authentication.
     * @param rejectedTotal Expected number of connection attepmts rejected in total.
     */
    private void checkRejectMetrics(MetricRegistry mreg, int rejectedTimeout, int rejectedAuth, int rejectedTotal)
        throws IgniteInterruptedCheckedException {
        waitForMetricValue(mreg, METRIC_REJECTED_TOTAL, rejectedTotal, 10_000);
        assertEquals(rejectedTimeout, mreg.<IntMetric>findMetric(METRIC_REJECTED_TIMEOUT).value());
        assertEquals(rejectedAuth, mreg.<IntMetric>findMetric(METRIC_REJECTED_AUTHENTICATION).value());
        assertEquals(0, mreg.<IntMetric>findMetric(MetricUtils.metricName("thin", METRIC_ACEPTED)).value());
        assertEquals(0, mreg.<IntMetric>findMetric(MetricUtils.metricName("thin", METRIC_ACTIVE)).value());
    }

    /**
     * Check client metrics
     * @param mreg Client metric registry
     * @param accepted Expected number of accepted connections.
     * @param active Expected number of active connections.
     */
    private void checkConnectionsMetrics(MetricRegistry mreg, int accepted, int active)
        throws IgniteInterruptedCheckedException {
        waitForMetricValue(mreg, MetricUtils.metricName("thin", METRIC_ACTIVE), active, 10_000);
        assertEquals(accepted, mreg.<IntMetric>findMetric(MetricUtils.metricName("thin", METRIC_ACEPTED)).value());
        assertEquals(0, mreg.<IntMetric>findMetric(METRIC_REJECTED_TIMEOUT).value());
        assertEquals(0, mreg.<IntMetric>findMetric(METRIC_REJECTED_AUTHENTICATION).value());
        assertEquals(0, mreg.<IntMetric>findMetric(METRIC_REJECTED_TOTAL).value());
    }
}
