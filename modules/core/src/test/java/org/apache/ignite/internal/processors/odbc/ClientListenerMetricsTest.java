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
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
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
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.metric.impl.MaxValueMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
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
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.METRIC_MAX_RUNNING_COMPUTE_TASKS;
import static org.apache.ignite.internal.util.GridTestClockTimer.DFLT_TIME_SUPPLIER;
import static org.apache.ignite.internal.util.GridTestClockTimer.timeSupplier;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_STORE_TYPE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Client listener metrics tests.
 */
public class ClientListenerMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final int MAX_TASKS = 5;

    /** Metric time interval. Expiration is emulated by a time shift. */
    private static final long LONG_INTERVAL = 10 * 60_000L;

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
     * Check that the running compute tasks metric reports the maximum among all connections and keeps it
     * during the time interval.
     */
    @Test
    public void testComputeTasksMetrics() throws Exception {
        startGridWithCompute();

        List<IgniteClientFuture<?>> futs = new ArrayList<>();

        try (
            IgniteClient client1 = Ignition.startClient(getClientConfiguration());
            IgniteClient client2 = Ignition.startClient(getClientConfiguration())
        ) {
            checkMaxComputeTasks(0);
            assertEquals(0, activeComputeTasksCount());

            int startCnt = 2;

            // Run tasks on the first connection.
            futs.addAll(startTasks(client1, startCnt));
            checkMaxComputeTasks(startCnt);
            assertEquals(startCnt, activeComputeTasksCount());

            // The same number of tasks on the second connection. Max metric should not change.
            futs.addAll(startTasks(client2, startCnt));
            checkMaxComputeTasks(startCnt);
            assertEquals(startCnt * 2, activeComputeTasksCount());

            // Run tasks on the second connection up to the limit.
            futs.addAll(startTasks(client2, MAX_TASKS - startCnt));
            checkMaxComputeTasks(MAX_TASKS);

            int allTasksCnt = MAX_TASKS + startCnt;
            assertEquals(allTasksCnt, activeComputeTasksCount());

            // Max metric should not exceed limit after a rejected task.
            assertComputeTaskRejected(client2);
            checkMaxComputeTasks(MAX_TASKS);
            assertEquals(allTasksCnt, activeComputeTasksCount());

            // Finished tasks should not reset the maximum.
            finishTasks(IntStream.range(0, allTasksCnt).toArray());
            checkMaxComputeTasks(MAX_TASKS);

            // "Wait" for expiration.
            shiftTime(LONG_INTERVAL);
            assertEquals(0, computeTasksMetric().value());

            // Run one more task.
            futs.addAll(startTasks(client1, 1));
            checkMaxComputeTasks(1);

            finishTasks(allTasksCnt);

            shiftTime(2 * LONG_INTERVAL);
            checkMaxComputeTasks(0);

            for (IgniteClientFuture<?> fut : futs)
                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        finally {
            timeSupplier(DFLT_TIME_SUPPLIER);
        }
    }

    /** Check that the metric is not less than the current maximum, when metric has expired. */
    @Test
    public void testComputeTasksMetricOnLongRunningTasks() throws Exception {
        startGridWithCompute();

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            List<IgniteClientFuture<?>> futs = startTasks(client, MAX_TASKS);

            checkMaxComputeTasks(MAX_TASKS);

            // "Wait" for expiration.
            shiftTime(LONG_INTERVAL);
            assertEquals(MAX_TASKS, computeTasksMetric().value());

            finishTasks(IntStream.range(0, MAX_TASKS).toArray());

            assertEquals(0, computeTasksMetric().value());

            for (IgniteClientFuture<?> fut : futs)
                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        finally {
            timeSupplier(DFLT_TIME_SUPPLIER);
        }
    }

    /** */
    @Test
    public void testComputeTasksMetricOnRejectedTask() throws Exception {
        startGridWithCompute();

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            List<IgniteClientFuture<?>> futs = startTasks(client, MAX_TASKS);

            checkMaxComputeTasks(MAX_TASKS);

            // "Wait" for expiration.
            shiftTime(LONG_INTERVAL);

            assertComputeTaskRejected(client);

            finishTasks(0);

            // The metric keeps the max limit, though the current value is less.
            checkMaxComputeTasks(MAX_TASKS);

            finishTasks(IntStream.range(1, MAX_TASKS).toArray());

            for (IgniteClientFuture<?> fut : futs)
                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        finally {
            timeSupplier(DFLT_TIME_SUPPLIER);
        }
    }

    /** */
    @Test
    public void testComputeTasksMetricsAfterClientDisconnect() throws Exception {
        startGridWithCompute();

        try (IgniteClient client1 = Ignition.startClient(getClientConfiguration())) {
            List<IgniteClientFuture<?>> futs1 = startTasks(client1, 1);

            assertEquals(1, activeComputeTasksCount());

            List<IgniteClientFuture<?>> futs2;

            try (IgniteClient client2 = Ignition.startClient(getClientConfiguration())) {
                futs2 = startTasks(client2, 2);

                checkMaxComputeTasks(2);

                assertEquals(3, activeComputeTasksCount());
            }

            // Tasks of the closed client should not be counted in system views.
            assertTrue(waitForCondition(() -> activeComputeTasksCount() == 1, getTestTimeout()));

            // Finish tasks of the closed client.
            TestComputeTask.finish(1);
            TestComputeTask.finish(2);

            // Unfinished tasks of client2 fail.
            for (IgniteClientFuture<?> fut : futs2)
                assertThrowsWithCause(() -> fut.get(getTestTimeout(), TimeUnit.MILLISECONDS), ClientException.class);

            // Finish task of alive client.
            finishTasks(0);

            // The metric keeps the maximum during the time interval.
            checkMaxComputeTasks(2);

            for (IgniteClientFuture<?> fut : futs1)
                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    /** */
    @Test
    public void testComputeTasksMetricsComputeDisabled() throws Exception {
        startGrid(0);

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            checkMaxComputeTasks(0);

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> client.compute().execute(TestComputeTask.class.getName(), null),
                ClientException.class,
                "Compute grid functionality is disabled for thin clients"
            );

            checkMaxComputeTasks(0);
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

    /** */
    private void assertComputeTaskRejected(IgniteClient client) {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> client.compute().execute(TestComputeTask.class.getName(), null),
            ClientException.class,
            "Active compute tasks per connection limit (" + MAX_TASKS + ") exceeded"
        );
    }

    /** */
    private List<IgniteClientFuture<?>> startTasks(IgniteClient client, int cnt) throws Exception {
        int expTasks = TestComputeTask.TASKS.size() + cnt;

        List<IgniteClientFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < cnt; i++)
            futs.add(client.compute().executeAsync2(TestComputeTask.class.getName(), null));

        // Waits for tasks to be started on the nodes.
        assertTrue(waitForCondition(() -> TestComputeTask.TASKS.size() == expTasks, getTestTimeout()));

        return futs;
    }

    /** Finishes tasks and waits for them to be released by the connections. */
    private void finishTasks(int... idxs) throws Exception {
        int expTasks = activeComputeTasksCount() - idxs.length;

        for (int idx : idxs)
            TestComputeTask.finish(idx);

        // Counter is decremented after the result is sent to the client.
        assertTrue(waitForCondition(() -> activeComputeTasksCount() == expTasks, getTestTimeout()));
    }

    /** @return Total active compute tasks of the thin client connections. */
    private int activeComputeTasksCount() {
        return thinConnections().stream().mapToInt(ClientConnectionView::activeComputeTasks).sum();
    }

    /** */
    private void checkMaxComputeTasks(long expMetric) throws Exception {
        assertTrue(waitForCondition(() -> computeTasksMetric().value() == expMetric, getTestTimeout()));
    }

    /** */
    private MaxValueMetric computeTasksMetric() {
        return grid(0).context().metric().registry(CLIENT_CONNECTOR_METRICS)
            .findMetric(METRIC_MAX_RUNNING_COMPUTE_TASKS);
    }

    /** Shifts the time returned by {@link U#currentTimeMillis()} relative to the system time. */
    private static void shiftTime(long offset) {
        timeSupplier(() -> System.currentTimeMillis() + offset);
    }

    /** */
    private List<ClientConnectionView> thinConnections() {
        SystemView<ClientConnectionView> view = grid(0).context().systemView().view(CLI_CONN_VIEW);

        return U.arrayList(view.iterator(), view.size(), v -> "THIN".equals(v.type()));
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
        waitForCondition(() -> mreg.<IntMetric>findMetric(metric).value() == value, timeout);

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
        /** */
        private static final List<TestComputeTask> TASKS = new CopyOnWriteArrayList<>();

        /** */
        private final CountDownLatch finishLatch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) {
            TASKS.add(this);

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

        /** @param idx Index of a task. */
        public static void finish(int idx) {
            TASKS.get(idx).finishLatch.countDown();
        }

        /** Finishes all tasks and clears the task list. */
        public static void reset() {
            TASKS.forEach(t -> t.finishLatch.countDown());

            TASKS.clear();
        }
    }

    /** */
    private static class NoopJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() {
            return null;
        }
    }
}
