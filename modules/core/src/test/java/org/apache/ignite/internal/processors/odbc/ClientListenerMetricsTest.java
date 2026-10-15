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
import java.util.stream.StreamSupport;
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
import org.apache.ignite.internal.processors.metric.impl.MaxValueMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLIENT_CONNECTOR_METRICS;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_ACEPTED;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_AUTHENTICATION;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_TIMEOUT;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_TOTAL;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.METRIC_ACTIVE;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.METRIC_PEAK_ACTIVE_COMPUTE_TASKS;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_STORE_TYPE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Client listener metrics tests.
 */
public class ClientListenerMetricsTest extends GridCommonAbstractTest {
    /** Maximum active tasks. */
    private static final int MAX_TASKS = 3;

    /** Metric time interval long enough for the peak not to expire during a test. Expiration is emulated by a reset. */
    private static final long LONG_INTERVAL = 10 * 60_000L;

    /** Metric time interval to check the peak expiration without a long wait. */
    private static final long SHORT_INTERVAL = 2_000L;

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

    /**
     * Check that the active compute tasks metric reports the peak of the busiest connection (not the sum), keeps it
     * during the time interval and drops it after the time interval.
     */
    @Test
    public void testComputeTasksMetrics() throws Exception {
        startGridWithCompute();

        List<IgniteInternalFuture<Object>> futs = new ArrayList<>();

        try (
            IgniteClient client1 = Ignition.startClient(getClientConfiguration());
            IgniteClient client2 = Ignition.startClient(getClientConfiguration())
        ) {
            checkComputeTasks(0);

            // Tasks 0, 1.
            futs.addAll(startTasks(client1, 2));
            checkComputeTasks(2);

            // Tasks 2, 3. The sum (4) exceeds the limit, but the metric reports the busiest connection.
            futs.addAll(startTasks(client2, 2));
            checkComputeTasks(2);

            // Task 4.
            futs.addAll(startTasks(client2, 1));
            checkComputeTasks(MAX_TASKS);

            assertComputeTaskRejected(client2);

            // A rejected task counts as the limit.
            checkComputeTasks(MAX_TASKS);

            // Finished tasks are released by the connections, but the metric keeps the peak during the time interval.
            finishTasks(2);
            checkComputeTasks(MAX_TASKS);

            finishTasks(0, 1);
            checkComputeTasks(MAX_TASKS);

            finishTasks(3, 4);
            checkComputeTasks(MAX_TASKS);

            // Clear the peak, as if it expired. No tasks are active, so the metric reports zero.
            computeTasksMetric().reset();
            assertEquals(0, computeTasksMetric().value());

            // Shorten the time interval to check the peak expiration without a long wait.
            computeTasksMetric().reset(SHORT_INTERVAL);

            // Task 5.
            futs.addAll(startTasks(client1, 1));
            checkComputeTasks(1);

            // The peak expires after the time interval.
            finishTasks(5);
            checkComputeTasks(0);

            // Wait for all results before clients are closed.
            for (IgniteInternalFuture<Object> fut : futs)
                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Check that the metric is not less than the current value, so it reports long-running tasks after their peak has
     * expired.
     */
    @Test
    public void testComputeTasksMetricOnLongRunningTasks() throws Exception {
        startGridWithCompute();

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            List<IgniteInternalFuture<Object>> futs = startTasks(client, MAX_TASKS);

            checkComputeTasks(MAX_TASKS);

            // Clear the peak, as if it expired while the tasks are active. The metric reports the current value.
            computeTasksMetric().reset();
            assertEquals(MAX_TASKS, computeTasksMetric().value());

            finishTasks(IntStream.range(0, MAX_TASKS).toArray());

            // Finished tasks do not update the peak, so the metric drops to the current value at once.
            assertEquals(0, computeTasksMetric().value());

            // Wait for all results before the client is closed.
            for (IgniteInternalFuture<Object> fut : futs)
                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Check that a task rejected by the limit updates the metric with the limit, even if the active tasks were started
     * before the time interval.
     */
    @Test
    public void testComputeTasksMetricOnRejectedTask() throws Exception {
        startGridWithCompute();

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            List<IgniteInternalFuture<Object>> futs = startTasks(client, MAX_TASKS);

            checkComputeTasks(MAX_TASKS);

            // Clear the peak, as if it expired while the tasks are active.
            computeTasksMetric().reset();

            assertComputeTaskRejected(client);

            // The metric keeps the limit from the rejected task, though the current value is less.
            finishTasks(0);
            checkComputeTasks(MAX_TASKS);

            finishTasks(IntStream.range(1, MAX_TASKS).toArray());

            // Wait for all results before the client is closed.
            for (IgniteInternalFuture<Object> fut : futs)
                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    /** Check that closing the connection with unfinished tasks releases its tasks. */
    @Test
    public void testComputeTasksMetricsAfterClientDisconnect() throws Exception {
        startGridWithCompute();

        try (IgniteClient client1 = Ignition.startClient(getClientConfiguration())) {
            List<IgniteInternalFuture<Object>> futs1 = startTasks(client1, 1);

            List<IgniteInternalFuture<Object>> futs2;

            ClientConnectionView connView2;

            try (IgniteClient client2 = Ignition.startClient(getClientConfiguration())) {
                futs2 = startTasks(client2, 2);

                checkComputeTasks(2);

                connView2 = thinConnections().stream().filter(v -> v.activeComputeTasks() == 2)
                    .findFirst().orElseThrow();
            }

            // Tasks of the closed connection are released on disconnect.
            assertTrue(waitForCondition(() -> connView2.activeComputeTasks() == 0, getTestTimeout()));

            // The metric keeps the peak during the time interval.
            checkComputeTasks(2);

            // Unfinished tasks fail on the client side. The error depends on whether the client received the execute
            // response before the close.
            for (IgniteInternalFuture<Object> fut : futs2)
                assertThrowsWithCause(() -> fut.get(getTestTimeout(), TimeUnit.MILLISECONDS), ClientException.class);

            // Last task of 'client1'.
            finishTasks(0);
            checkComputeTasks(2);

            // Wait for the result before the client is closed.
            for (IgniteInternalFuture<Object> fut : futs1)
                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    /** Check the active compute tasks metric when compute is disabled for thin clients. */
    @Test
    public void testComputeTasksMetricsComputeDisabled() throws Exception {
        startGrid(0);

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            checkComputeTasks(0);

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> client.compute().execute(TestComputeTask.class.getName(), null),
                ClientException.class,
                "Compute grid functionality is disabled for thin clients"
            );

            checkComputeTasks(0);
        }
    }

    /**
     * Starts a node with compute enabled for thin clients and sets the metric time interval to {@link #LONG_INTERVAL}.
     */
    private void startGridWithCompute() throws Exception {
        startGrid(getConfiguration(getTestIgniteInstanceName(0))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setThinClientConfiguration(new ThinClientConfiguration()
                    .setMaxActiveComputeTasksPerConnection(MAX_TASKS))));

        computeTasksMetric().reset(LONG_INTERVAL);
    }

    /**
     * Checks that a task is rejected because of the limit.
     *
     * @param client Ignite client.
     */
    private void assertComputeTaskRejected(IgniteClient client) {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> client.compute().execute(TestComputeTask.class.getName(), null),
            ClientException.class,
            "Active compute tasks per connection limit (" + MAX_TASKS + ") exceeded"
        );
    }

    /**
     * Starts tasks via client and waits for them to be started on the node.
     *
     * @param client Ignite client.
     * @param cnt Tasks count.
     * @return Task futures.
     */
    private List<IgniteInternalFuture<Object>> startTasks(IgniteClient client, int cnt) throws Exception {
        int expTasks = TestComputeTask.TASKS.size() + cnt;

        List<IgniteInternalFuture<Object>> futs = IntStream.range(0, cnt)
            .mapToObj(i -> runAsync(() -> client.compute().execute(TestComputeTask.class.getName(), null)))
            .toList();

        // A task instance is created after the counter is incremented. Wait for instances to keep the task order.
        assertTrue(waitForCondition(() -> TestComputeTask.TASKS.size() == expTasks, getTestTimeout()));

        return futs;
    }

    /**
     * Finishes tasks and waits for them to be released by the connections.
     *
     * @param idxs Indexes of the tasks.
     */
    private void finishTasks(int... idxs) throws Exception {
        int expTasks = activeComputeTasks() - idxs.length;

        for (int idx : idxs)
            TestComputeTask.finish(idx);

        // Counter is decremented after the result is sent to the client.
        assertTrue(waitForCondition(() -> activeComputeTasks() == expTasks, getTestTimeout()));
    }

    /** @return Total active compute tasks of the thin client connections from the system view. */
    private int activeComputeTasks() {
        return thinConnections().stream().mapToInt(ClientConnectionView::activeComputeTasks).sum();
    }

    /**
     * Waits for the expected value of the active compute tasks metric.
     *
     * @param expMetric Expected value of the metric.
     */
    private void checkComputeTasks(long expMetric) throws Exception {
        assertTrue(waitForCondition(() -> computeTasksMetric().value() == expMetric, getTestTimeout()));
    }

    /** @return Active compute tasks metric. */
    private MaxValueMetric computeTasksMetric() {
        return grid(0).context().metric().registry(CLIENT_CONNECTOR_METRICS).findMetric(METRIC_PEAK_ACTIVE_COMPUTE_TASKS);
    }

    /** @return Thin client connections from the system view. */
    private List<ClientConnectionView> thinConnections() {
        SystemView<ClientConnectionView> view = grid(0).context().systemView().view(CLI_CONN_VIEW);

        return StreamSupport.stream(view.spliterator(), false).filter(v -> "THIN".equals(v.type())).toList();
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

        /** Finishes all tasks and clears the task list. */
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
