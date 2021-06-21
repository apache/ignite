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
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLIENT_METRICS;
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
        try (IgniteEx ignite = startGrid(0))
        {
            MetricRegistry mreg = ignite.context().metric().registry(CLIENT_METRICS);

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
        ClientConnectorConfiguration cliConCfg = new ClientConnectorConfiguration();
        cliConCfg.setHandshakeTimeout(500);

        IgniteConfiguration nodeCfg = new IgniteConfiguration();
        nodeCfg.setClientConnectorConfiguration(cliConCfg);
        nodeCfg.setAuthenticationEnabled(true);

        try (IgniteEx ignite = startGrid(nodeCfg))
        {
            MetricRegistry mreg = ignite.context().metric().registry(CLIENT_METRICS);

            checkRejectMetrics(mreg, 0, 0, 0);

            ClientConfiguration cfgSsl = getClientConfiguration()
                .setSslMode(SslMode.REQUIRED)
                .setSslClientCertificateKeyStorePath("client.jks")
                .setSslClientCertificateKeyStoreType(DFLT_STORE_TYPE)
                .setSslClientCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStorePath("trust.jks")
                .setSslTrustCertificateKeyStoreType(DFLT_STORE_TYPE)
                .setSslTrustCertificateKeyStorePassword("123456");

            try {
                Ignition.startClient(cfgSsl);
            }
            catch (ClientException ignored) {}

            checkRejectMetrics(mreg, 1, 0, 1);

            ClientConfiguration cfgAuth = getClientConfiguration()
                .setUserName("SomeRandomInvalidName")
                .setUserPassword("42");

            try {
                Ignition.startClient(cfgAuth);
            }
            catch (ClientAuthenticationException ignored) {}

            checkRejectMetrics(mreg, 1, 1, 2);
        }
    }

    /**
     * Check that failed connection attempts to the grid affect metrics.
     */
    @Test
    public void testClientListenerMetricsRejectGeneral() throws Exception {
        ClientConnectorConfiguration cliConCfg = new ClientConnectorConfiguration();
        cliConCfg.setThinClientEnabled(false);

        IgniteConfiguration nodeCfg = new IgniteConfiguration();
        nodeCfg.setClientConnectorConfiguration(cliConCfg);

        try (IgniteEx ignite = startGrid(nodeCfg))
        {
            MetricRegistry mreg = ignite.context().metric().registry(CLIENT_METRICS);

            checkRejectMetrics(mreg, 0, 0, 0);

            try {
                Ignition.startClient(getClientConfiguration());
            }
            catch (ClientException ignored) {}

            checkRejectMetrics(mreg, 0, 0, 1);
        }
    }

    /** */
    private static ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration()
                .setAddresses(Config.SERVER)
                .setSendBufferSize(0)
                .setReceiveBufferSize(0);
    }

    /**
     * Check client metrics
     * @param mreg Client metric registry
     * @param rejectedTimeout Expected number of connection attepmts rejected by timeout.
     * @param rejectedAuth Expected number of connection attepmts rejected because of failed authentication.
     * @param rejectedTotal Expected number of connection attepmts rejected in total.
     */
    private void checkRejectMetrics(MetricRegistry mreg, int rejectedTimeout, int rejectedAuth, int rejectedTotal) {
        assertEquals(rejectedTimeout, mreg.<IntMetric>findMetric("connections.rejectedByTimeout").value());
        assertEquals(rejectedAuth, mreg.<IntMetric>findMetric("connections.rejectedAuthentication").value());
        assertEquals(rejectedTotal, mreg.<IntMetric>findMetric("connections.rejectedTotal").value());
        assertEquals(0, mreg.<IntMetric>findMetric("connections.thin.accepted").value());
        assertEquals(0, mreg.<IntMetric>findMetric("connections.thin.active").value());
    }

    /**
     * Check client metrics
     * @param mreg Client metric registry
     * @param accepted Expected number of accepted connections.
     * @param active Expected number of active connections.
     */
    private void checkConnectionsMetrics(MetricRegistry mreg, int accepted, int active) {
        assertEquals(0, mreg.<IntMetric>findMetric("connections.rejectedByTimeout").value());
        assertEquals(0, mreg.<IntMetric>findMetric("connections.rejectedAuthentication").value());
        assertEquals(0, mreg.<IntMetric>findMetric("connections.rejectedTotal").value());
        assertEquals(accepted, mreg.<IntMetric>findMetric("connections.thin.accepted").value());
        assertEquals(active, mreg.<IntMetric>findMetric("connections.thin.active").value());
    }
}
