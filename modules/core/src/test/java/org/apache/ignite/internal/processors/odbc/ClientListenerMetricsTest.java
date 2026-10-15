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
import org.apache.ignite.internal.management.api.CommandMBean;
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

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLIENT_CONNECTOR_METRICS;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.DFLT_PEAK_ACTIVE_COMPUTE_TASKS_INTERVAL;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_ACEPTED;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_PEAK_ACTIVE_COMPUTE_TASKS;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_AUTHENTICATION;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_TIMEOUT;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.METRIC_REJECTED_TOTAL;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.METRIC_ACTIVE;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_STORE_TYPE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Client listener metrics tests.
 */
public class ClientListenerMetricsTest extends GridCommonAbstractTest {
    /** Maximum active tasks. */
    private static final int MAX_TASKS = 3;

    /** Time interval of the active compute tasks metric, so the peaks do not expire during the test. */
    private static final long LONG_INTERVAL = 10 * 60_000L;

    /** Time interval of the active compute tasks metric to check that the peak expires, long enough to check the peak. */
    private static final long SHORT_INTERVAL = 10_000L;

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
     * Check that the active compute tasks metric reports the peak of the busiest connection (not the sum) and keeps it
     * during the time interval, while the {@link ClientListenerProcessor#CLI_CONN_VIEW} system view reports current
     * values.
     */
    @Test
    public void testComputeTasksMetrics() throws Exception {
        startGridWithCompute();

        List<IgniteInternalFuture<Object>> futs = new ArrayList<>();

        try (
            IgniteClient client1 = Ignition.startClient(getClientConfiguration());
            IgniteClient client2 = Ignition.startClient(getClientConfiguration())
        ) {
            checkComputeTasks(0, 0, 0);

            // Tasks 0, 1.
            futs.addAll(startTasks(client1, 2));
            checkComputeTasks(2, 2, 0);

            // Tasks 2, 3. The sum of the connections (4) exceeds even the limit, the metric reports the busiest one.
            futs.addAll(startTasks(client2, 2));
            checkComputeTasks(2, 2, 2);

            // Task 4.
            futs.addAll(startTasks(client2, 1));
            checkComputeTasks(MAX_TASKS, 2, MAX_TASKS);

            assertComputeTaskRejected(client2);

            // Rejected task is not counted in the view.
            checkComputeTasks(MAX_TASKS, 2, MAX_TASKS);

            // Finished tasks are not counted in the view, but the metric keeps the peak during the time interval.
            TestComputeTask.finish(2);
            checkComputeTasks(MAX_TASKS, 2, MAX_TASKS - 1);

            TestComputeTask.finish(0);
            TestComputeTask.finish(1);
            checkComputeTasks(MAX_TASKS, 0, MAX_TASKS - 1);

            TestComputeTask.finish(3);
            TestComputeTask.finish(4);
            checkComputeTasks(MAX_TASKS, 0, 0);

            // Shorten the time interval to check that the peak expires. Reconfiguration also clears the peak.
            configureComputeTasksInterval(SHORT_INTERVAL);
            checkComputeTasks(0, 0, 0);

            // Task 5.
            futs.addAll(startTasks(client1, 1));
            checkComputeTasks(1, 1, 0);

            // The peak expires after the time interval.
            TestComputeTask.finish(5);
            checkComputeTasks(0, 0, 0);

            // Wait for all results before clients are closed.
            for (IgniteInternalFuture<Object> fut : futs)
                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Check that a task rejected because of the limit updates the metric, so it reports the saturated connection even
     * if its tasks were started before the current time interval.
     */
    @Test
    public void testComputeTasksMetricOnRejectedTask() throws Exception {
        startGridWithCompute();

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            List<IgniteInternalFuture<Object>> futs = startTasks(client, MAX_TASKS);

            checkComputeTasks(MAX_TASKS, MAX_TASKS);

            // Clear the accumulated peak, as if it expired while the tasks are still active.
            computeTasksMetric().reset();
            checkComputeTasks(0, MAX_TASKS);

            assertComputeTaskRejected(client);

            checkComputeTasks(MAX_TASKS, MAX_TASKS);

            for (int i = 0; i < MAX_TASKS; i++)
                TestComputeTask.finish(i);

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

                checkComputeTasks(2, 1, 2);

                connView2 = thinConnections().stream().filter(v -> v.activeComputeTasks() == 2).findFirst().orElseThrow();
            }

            // Task resources are released on disconnect, so the counter of the closed connection is decremented.
            assertTrue(waitForCondition(() -> connView2.activeComputeTasks() == 0, getTestTimeout()));

            // Closed connection is not shown, the metric keeps the peak during the time interval.
            checkComputeTasks(2, 1);

            // Unfinished task execution fails on the client side, since the connection is closed. The error depends on
            // whether the client has received the execute response before the close.
            for (IgniteInternalFuture<Object> fut : futs2)
                GridTestUtils.assertThrowsWithCause(() -> fut.get(getTestTimeout(), TimeUnit.MILLISECONDS), ClientException.class);

            // Last task of 'client1'.
            TestComputeTask.finish(0);
            checkComputeTasks(2, 0);

            // Wait for the result before the client is closed.
            for (IgniteInternalFuture<Object> fut : futs1)
                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    /** Check the active compute tasks metric and system view when compute is disabled for thin clients. */
    @Test
    public void testComputeTasksMetricsComputeDisabled() throws Exception {
        startGrid(0);

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            checkComputeTasks(0, 0);

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> client.compute().execute(TestComputeTask.class.getName(), null),
                ClientException.class,
                "Compute grid functionality is disabled for thin clients"
            );

            checkComputeTasks(0, 0);
        }
    }

    /**
     * Starts a node with enabled compute for thin clients. The time interval of the active compute tasks metric is
     * changed to {@link #LONG_INTERVAL}, so the peaks do not expire during the test.
     */
    private void startGridWithCompute() throws Exception {
        startGrid(getConfiguration(getTestIgniteInstanceName(0))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setThinClientConfiguration(new ThinClientConfiguration()
                    .setMaxActiveComputeTasksPerConnection(MAX_TASKS))));

        assertEquals(DFLT_PEAK_ACTIVE_COMPUTE_TASKS_INTERVAL, computeTasksMetric().timeInterval());

        configureComputeTasksInterval(LONG_INTERVAL);
    }

    /**
     * Changes the time interval of the active compute tasks metric the same way as
     * {@code control.sh --metric --configure-max-value} does.
     *
     * @param interval Time interval in milliseconds.
     */
    private void configureComputeTasksInterval(long interval) throws Exception {
        DynamicMBean mbean = getMxBean(grid(0).name(), "management", singletonList("Metric"), "ConfigureMaxValue",
            DynamicMBean.class);

        mbean.invoke(
            CommandMBean.INVOKE,
            new Object[] {MetricUtils.metricName(CLIENT_CONNECTOR_METRICS, METRIC_PEAK_ACTIVE_COMPUTE_TASKS),
                Long.toString(interval), null},
            new String[] {String.class.getName(), String.class.getName(), String.class.getName()}
        );

        assertTrue(waitForCondition(() -> computeTasksMetric().timeInterval() == interval, getTestTimeout()));
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

        // Task instances are created after the active tasks counter is incremented, so wait for them to keep the order.
        assertTrue(waitForCondition(() -> TestComputeTask.TASKS.size() == expTasks, getTestTimeout()));

        return futs;
    }

    /**
     * Checks the active compute tasks metric via the metric registry and JMX, and the current active compute tasks of
     * the thin client connections via the system view.
     *
     * @param expMetric Expected value of the metric.
     * @param expConnTasks Expected active compute tasks of the thin client connections, in any order.
     */
    private void checkComputeTasks(long expMetric, int... expConnTasks) throws Exception {
        List<Integer> exp = IntStream.of(expConnTasks).sorted().boxed().toList();

        // Counter is decremented after the result is sent to the client, so wait for the values.
        assertTrue(waitForCondition(() -> exp.equals(thinConnectionsComputeTasks()), getTestTimeout()));

        assertTrue(waitForCondition(() -> computeTasksMetric().value() == expMetric, getTestTimeout()));

        assertTrue(waitForCondition(() -> jmxComputeTasksMetric() == expMetric, getTestTimeout()));
    }

    /** @return Active compute tasks metric. */
    private MaxValueMetric computeTasksMetric() {
        return grid(0).context().metric().registry(CLIENT_CONNECTOR_METRICS).findMetric(METRIC_PEAK_ACTIVE_COMPUTE_TASKS);
    }

    /** @return Active compute tasks metric value read via JMX. */
    private long jmxComputeTasksMetric() {
        DynamicMBean mbean = metricRegistry(grid(0).name(), "client", "connector");

        try {
            return (Long)mbean.getAttribute(METRIC_PEAK_ACTIVE_COMPUTE_TASKS);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** @return Sorted active compute tasks of the thin client connections from the system view. */
    private List<Integer> thinConnectionsComputeTasks() {
        return thinConnections().stream().map(ClientConnectionView::activeComputeTasks).sorted().toList();
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
