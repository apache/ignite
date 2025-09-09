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

package org.apache.ignite.thread;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.IgnitionEx.gridx;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.THREAD_POOLS;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests that thread pool metrics are available before the start of all Ignite components happened.
 */
public class ThreadPoolMetricsTest extends GridCommonAbstractTest {
    /** Custom executor name. */
    private static final String CUSTOM_EXEC_NAME = "user-pool";

    /** Names of the system views for the thread pools. */
    private static final Collection<String> THREAD_POOL_VIEWS = Arrays.asList(
        SYS_POOL_QUEUE_VIEW,
        STREAM_POOL_QUEUE_VIEW
    );

    /** Mapping of the metric group name to the thread pool instance. */
    private static final Map<String, Function<PoolProcessor, ExecutorService>> THREAD_POOL_METRICS = new HashMap<>();

    static {
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridUtilityCacheExecutor"), PoolProcessor::utilityCachePool);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridExecutionExecutor"), PoolProcessor::getExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridServicesExecutor"), PoolProcessor::getServiceExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridSystemExecutor"), PoolProcessor::getSystemExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridClassLoadingExecutor"), PoolProcessor::getPeerClassLoadingExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridManagementExecutor"), PoolProcessor::getManagementExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridAffinityExecutor"), PoolProcessor::getAffinityExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridCallbackExecutor"), PoolProcessor::asyncCallbackPool);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridQueryExecutor"), PoolProcessor::getQueryExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridSchemaExecutor"), PoolProcessor::getSchemaExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridRebalanceExecutor"), PoolProcessor::getRebalanceExecutorService);
        THREAD_POOL_METRICS.put(
            metricName(THREAD_POOLS, "GridRebalanceStripedExecutor"),
            PoolProcessor::getStripedRebalanceExecutorService
        );
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridThinClientExecutor"), PoolProcessor::getThinClientExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridDataStreamExecutor"), PoolProcessor::getDataStreamerExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "StripedExecutor"), PoolProcessor::getStripedExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridRestExecutor"), PoolProcessor::getRestExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridSnapshotExecutor"), PoolProcessor::getSnapshotExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, "GridReencryptionExecutor"), PoolProcessor::getReencryptionExecutorService);
        THREAD_POOL_METRICS.put(metricName(THREAD_POOLS, CUSTOM_EXEC_NAME), proc -> (ExecutorService)proc.customExecutor(CUSTOM_EXEC_NAME));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true))
            )
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setExecutorConfiguration(new ExecutorConfiguration().setName(CUSTOM_EXEC_NAME));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests that thread pool metrics are available before the start of all Ignite components happened.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testThreadPoolMetricsRegistry() throws Exception {
        // Latch that indicates whether the start of Ignite components was launched.
        CountDownLatch startLaunchedLatch = new CountDownLatch(1);

        // Latch that indicates whether the start of Ignite components was unblocked.
        CountDownLatch startUnblockedLatch = new CountDownLatch(1);

        IgniteInternalFuture<IgniteEx> startFut = runAsync(() -> startGrid(
            getConfiguration(getTestIgniteInstanceName()).setPluginProviders(new AbstractTestPluginProvider() {
                @Override public String name() {
                    return "test-stuck-plugin";
                }

                @Override public void onIgniteStart() {
                    startLaunchedLatch.countDown();

                    try {
                        startUnblockedLatch.await(getTestTimeout(), MILLISECONDS);
                    }
                    catch (InterruptedException e) {
                        throw new IgniteException(e);
                    }
                }
            })));

        try {
            assertTrue(startLaunchedLatch.await(getTestTimeout(), MILLISECONDS));

            IgniteEx srv = gridx(getTestIgniteInstanceName());

            assertTrue(StreamSupport.stream(srv.context().metric().spliterator(), false)
                .map(ReadOnlyMetricRegistry::name)
                .collect(Collectors.toSet())
                .containsAll(THREAD_POOL_METRICS.keySet()));

            assertTrue(StreamSupport.stream(srv.context().systemView().spliterator(), false)
                .map(SystemView::name)
                .collect(Collectors.toSet())
                .containsAll(THREAD_POOL_VIEWS));
        }
        finally {
            startUnblockedLatch.countDown();
        }

        startFut.get(getTestTimeout(), MILLISECONDS);
    }

    /**
     * Tests basic thread pool metrics.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testThreadPoolMetrics() throws Exception {
        IgniteEx ignite = startGrid(getTestIgniteInstanceName());
        CountDownLatch longTaskLatch = new CountDownLatch(1);
        PoolProcessor poolProc = ignite.context().pools();
        List<Runnable> tasks = new ArrayList<>();
        AtomicInteger cntr = new AtomicInteger();
        int taskCnt = 10;

        for (int i = 0; i < taskCnt; i++) {
            tasks.add(new RunnableX() {
                @Override public void runx() throws Exception {
                    U.sleep(1);

                    cntr.decrementAndGet();
                }
            });
        }

        // Add a long task to check the metric of active tasks.
        tasks.add(new RunnableX() {
            @Override public void runx() throws Exception {
                U.await(longTaskLatch, getTestTimeout(), MILLISECONDS);
            }
        });

        for (Map.Entry<String, Function<PoolProcessor, ExecutorService>> entry : THREAD_POOL_METRICS.entrySet()) {
            String metricsName = entry.getKey();
            ExecutorService execSvc = entry.getValue().apply(poolProc);
            MetricRegistry mreg = ignite.context().metric().registry(metricsName);
            HistogramMetric execTimeMetric = mreg.findMetric(PoolProcessor.TASK_EXEC_TIME);
            boolean stripedExecutor = execSvc instanceof StripedExecutor;

            // Ensure that the execution time histogram can be reset.
            execTimeMetric.reset();

            cntr.set(taskCnt);

            for (int i = 0; i < tasks.size(); i++) {
                Runnable task = tasks.get(i);

                if (execSvc instanceof IgniteStripedThreadPoolExecutor)
                    ((IgniteStripedThreadPoolExecutor)execSvc).execute(task, i);
                else
                    execSvc.execute(task);
            }

            String errMsg = "pool=" + mreg.name();

            assertTrue(GridTestUtils.waitForCondition(() -> cntr.get() == 0, getTestTimeout()));
            assertFalse(errMsg, ((BooleanMetric)mreg.findMetric("Shutdown")).value());
            assertFalse(errMsg, ((BooleanMetric)mreg.findMetric("Terminated")).value());
            assertTrue(errMsg, ((IntMetric)mreg.findMetric("ActiveCount")).value() > 0);
            assertTrue(errMsg, stream(execTimeMetric.value()).sum() >= taskCnt);

            if (stripedExecutor) {
                assertTrue(errMsg, ((IntMetric)mreg.findMetric("StripesCount")).value() > 0);
                assertTrue(errMsg, ((LongMetric)mreg.findMetric("TotalCompletedTasksCount")).value() >= taskCnt);

                continue;
            }

            assertTrue(errMsg, ((LongMetric)mreg.findMetric("CompletedTaskCount")).value() >= taskCnt);
            assertFalse(errMsg, F.isEmpty(mreg.findMetric("ThreadFactoryClass").getAsString()));
            assertFalse(errMsg, F.isEmpty(mreg.findMetric("RejectedExecutionHandlerClass").getAsString()));
            assertTrue(errMsg, ((IntMetric)mreg.findMetric("CorePoolSize")).value() > 0);
            assertTrue(errMsg, ((IntMetric)mreg.findMetric("LargestPoolSize")).value() > 0);
            assertTrue(errMsg, ((IntMetric)mreg.findMetric("MaximumPoolSize")).value() > 0);
        }

        longTaskLatch.countDown();
    }
}
