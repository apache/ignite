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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import javax.management.DynamicMBean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLIENT_CONNECTOR_METRICS;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_ACEPTED;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_AUTHENTICATION;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_TIMEOUT;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_TOTAL;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.METRIC_ACTIVE;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.METRIC_ACTIVE_COMPUTE_TASKS;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.METRIC_MAX_COMPUTE_TASKS;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_STORE_TYPE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Client listener metrics tests.
 */
public class ClientListenerMetricsTest extends GridCommonAbstractTest {
    /** Maximum active tasks. */
    private static final int MAX_TASKS = 3;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        TestComputeTask.reset();

        stopAllGrids();

        super.afterTest();
    }

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

    /** Check active and max compute tasks per connection metrics. */
    @Test
    public void testComputeTasksMetrics() throws Exception {
        IgniteConfiguration nodeCfg = getConfigurationWithCompute();

        startGrid(nodeCfg);

        List<IgniteInternalFuture<Object>> futs = new ArrayList<>();

        try (
            IgniteClient client1 = Ignition.startClient(getClientConfiguration());
            IgniteClient client2 = Ignition.startClient(getClientConfiguration())
        ) {
            checkComputeTasksMetrics(0);

            futs.addAll(startTasksAndCheckMetrics(client1, 1, 1));

            futs.addAll(startTasksAndCheckMetrics(client2, 2, 2));

            futs.addAll(startTasksAndCheckMetrics(client2, 1, MAX_TASKS));

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> client2.compute().execute(TestComputeTask.class.getName(), null),
                ClientException.class,
                "Active compute tasks per connection limit (" + MAX_TASKS + ") exceeded"
            );

            checkComputeTasksMetrics(MAX_TASKS);

            // Finish on 'client1' does not affect busiest connection.
            TestComputeTask.finish(0);
            checkComputeTasksMetrics(MAX_TASKS);

            // Finish some tasks on 'client2' affects busiest connection.
            TestComputeTask.finish(1);
            checkComputeTasksMetrics(MAX_TASKS - 1);

            TestComputeTask.finish(2);
            checkComputeTasksMetrics(MAX_TASKS - 2);

            // Last task of 'client2'.
            TestComputeTask.finish(3);
            checkComputeTasksMetrics(0);

            // Wait for all results before clients are closed.
            for (IgniteInternalFuture<Object> fut : futs)
                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    /** Check that closed connection with an unfinished task does not affect compute tasks metrics. */
    @Test
    public void testComputeTasksMetricsAfterClientDisconnect() throws Exception {
        startGrid(getConfigurationWithCompute());

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            startTasksAndCheckMetrics(client, 1, 1);
        }

        // Task is still running, but its resources are released on disconnect.
        checkComputeTasksMetrics(0);
    }

    /**
     * Check active and max compute tasks per connection metrics when compute is disabled for thin clients.
     */
    @Test
    public void testComputeTasksMetricsComputeDisabled() throws Exception {
        startGrid(0);

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            checkComputeTasksMetrics(0, 0);

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> client.compute().execute(TestComputeTask.class.getName(), null),
                ClientException.class,
                "Compute grid functionality is disabled for thin clients"
            );

            checkComputeTasksMetrics(0, 0);
        }
    }

    /** Get configuration with enabled compute on clients. */
    private IgniteConfiguration getConfigurationWithCompute() throws Exception {
        return getConfiguration(getTestIgniteInstanceName(0))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setThinClientConfiguration(new ThinClientConfiguration()
                    .setMaxActiveComputeTasksPerConnection(MAX_TASKS)));
    }

    /**
     * Starts tasks via client and checks compute tasks metrics.
     *
     * @param client Ignite client.
     * @param cnt Tasks count.
     * @param expActive Expected active compute tasks on the busiest connection.
     * @return Task futures.
     */
    private List<IgniteInternalFuture<Object>> startTasksAndCheckMetrics(
        IgniteClient client,
        int cnt,
        int expActive) throws Exception {
        int expTasks = TestComputeTask.TASKS.size() + cnt;

        List<IgniteInternalFuture<Object>> futs = IntStream.range(0, cnt)
            .mapToObj(i -> runAsync(() -> client.compute().execute(TestComputeTask.class.getName(), null)))
            .toList();

        // Task instances are created after the active tasks counter is incremented, so wait for them to keep the order.
        assertTrue(waitForCondition(() -> TestComputeTask.TASKS.size() == expTasks, getTestTimeout()));

        checkComputeTasksMetrics(expActive);

        return futs;
    }

    /**
     * Check compute tasks metrics via the metric registry and JMX, when compute is enabled for thin clients.
     *
     * @param expActive Expected active compute tasks on the busiest connection.
     */
    private void checkComputeTasksMetrics(int expActive) throws Exception {
        checkComputeTasksMetrics(expActive, MAX_TASKS);
    }

    /**
     * Check compute tasks metrics via the metric registry and JMX.
     *
     * @param expActive Expected active compute tasks on the busiest connection.
     * @param expMax Expected configured limit of active compute tasks per connection.
     */
    private void checkComputeTasksMetrics(int expActive, int expMax) throws Exception {
        MetricRegistry mreg = grid(0).context().metric().registry(CLIENT_CONNECTOR_METRICS);

        waitForMetricValue(mreg, METRIC_ACTIVE_COMPUTE_TASKS, expActive, getTestTimeout());

        assertEquals(expActive, mreg.<IntMetric>findMetric(METRIC_ACTIVE_COMPUTE_TASKS).value());
        assertEquals(expMax, mreg.<IntMetric>findMetric(METRIC_MAX_COMPUTE_TASKS).value());

        DynamicMBean mbean = metricRegistry(grid(0).name(), "client", "connector");

        assertEquals(expActive, mbean.getAttribute(METRIC_ACTIVE_COMPUTE_TASKS));
        assertEquals(expMax, mbean.getAttribute(METRIC_MAX_COMPUTE_TASKS));
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
        waitForCondition(new GridAbsPredicate() {
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

    /** Task that waits for the latch on reduce. */
    public static class TestComputeTask extends ComputeTaskAdapter<Object, Object> {
        /** Tasks instances. */
        private static final List<TestComputeTask> TASKS = new CopyOnWriteArrayList<>();

        /** Finish latch. */
        private final CountDownLatch finishLatch = new CountDownLatch(1);

        /** Default constructor. */
        public TestComputeTask() {
            TASKS.add(this);
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) {
            return F.asMap(new NoopJob(), subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public @Nullable Object reduce(List<ComputeJobResult> results) {
            try {
                finishLatch.await();
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }

            return null;
        }

        /**
         * Finishes task with a specified index.
         *
         * @param idx Index of a task.
         */
        public static void finish(int idx) {
            TASKS.get(idx).finishLatch.countDown();
        }

        /** Clears static state. */
        public static void reset() {
            TASKS.forEach(t -> t.finishLatch.countDown());

            TASKS.clear();
        }
    }

    /** No-op job. */
    private static class NoopJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() {
            return null;
        }
    }
}
