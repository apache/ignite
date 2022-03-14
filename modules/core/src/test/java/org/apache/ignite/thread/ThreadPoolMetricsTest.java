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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.IgnitionEx.gridx;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.THREAD_POOLS;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.thread.ExecutorServiceMetricsAware.TASK_EXEC_TIME_NAME;

/**
 * Tests that thread pool metrics are available before the start of all Ignite components happened.
 */
public class ThreadPoolMetricsTest extends GridCommonAbstractTest {
    /** Names of the general thread pool metrics. */
    private static final Collection<String> THREAD_POOL_METRICS = Arrays.asList(
        metricName(THREAD_POOLS, "GridUtilityCacheExecutor"),
        metricName(THREAD_POOLS, "GridExecutionExecutor"),
        metricName(THREAD_POOLS, "GridServicesExecutor"),
        metricName(THREAD_POOLS, "GridSystemExecutor"),
        metricName(THREAD_POOLS, "GridClassLoadingExecutor"),
        metricName(THREAD_POOLS, "GridManagementExecutor"),
        metricName(THREAD_POOLS, "GridAffinityExecutor"),
        metricName(THREAD_POOLS, "GridCallbackExecutor"),
        metricName(THREAD_POOLS, "GridQueryExecutor"),
        metricName(THREAD_POOLS, "GridSchemaExecutor"),
        metricName(THREAD_POOLS, "GridRebalanceExecutor"),
        metricName(THREAD_POOLS, "GridThinClientExecutor"),
        metricName(THREAD_POOLS, "GridRebalanceStripedExecutor"),
        metricName(THREAD_POOLS, "GridDataStreamExecutor")
    );

    /** Names of the system views for the thread pools. */
    private static final Collection<String> THREAD_POOL_VIEWS = Arrays.asList(
        SYS_POOL_QUEUE_VIEW,
        STREAM_POOL_QUEUE_VIEW
    );

    /**
     * Tests that thread pool metrics are available before the start of all Ignite components happened.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testThreadPoolMetrics() throws Exception {
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
                .containsAll(THREAD_POOL_METRICS));

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
     * @throws Exception If failed.
     */
    @Test
    public void testThreadPoolTaskTimeMetric() throws Exception {
        IgniteEx ignite = startGrid(getConfiguration(getTestIgniteInstanceName())
            .setConnectorConfiguration(new ConnectorConfiguration()));

        PoolProcessor poolProc = ignite.context().pools();

        int taskCnt = 10;

        List<Callable<Integer>> tasks = new ArrayList<>();
        AtomicInteger cntr = new AtomicInteger();

        for (int i = 0; i < taskCnt; i++) {
            tasks.add(() -> {
                U.sleep(1);

                return cntr.decrementAndGet();
            });
        }

        for (Field field : poolProc.getClass().getDeclaredFields()) {
            cntr.set(taskCnt);

            if (ExecutorService.class.isAssignableFrom(field.getType())) {
                ExecutorService execSvc = U.field(poolProc, field.getName());

                if (execSvc == null || execSvc instanceof IgniteStripedThreadPoolExecutor)
                    continue;

                if (execSvc instanceof StripedExecutor) {
                    for (int i = 0; i < taskCnt; i++) {
                        Callable<?> call = tasks.get(i);
                        Runnable r = () -> {
                            try {
                                call.call();
                            }
                            catch (Exception e) {
                                throw F.wrap(e);
                            }
                        };

                        if (execSvc instanceof IgniteStripedThreadPoolExecutor)
                            ((IgniteStripedThreadPoolExecutor)execSvc).execute(r, taskCnt - i - 1);
                        else
                            execSvc.execute(r);
                    }

                    waitForCondition(() -> cntr.get() == 0, 5_000);
                }
                else
                    execSvc.invokeAll(tasks, getTestTimeout(), MILLISECONDS);

                assertEquals(field.getName(), 0, cntr.get());
            }
        }

        String metricPrefix = THREAD_POOLS + MetricUtils.SEPARATOR;

        for (ReadOnlyMetricRegistry mreg : ignite.context().metric()) {
            if (!mreg.name().startsWith(metricPrefix))
                continue;

            HistogramMetric histogram = mreg.findMetric(TASK_EXEC_TIME_NAME);

            String poolName = "pool=" + mreg.name();

            assertNotNull(poolName, histogram);

            long[] res = histogram.value();

            assertTrue(poolName + ": exp=" + taskCnt + ", actual=" + res[0], res[0] >= taskCnt);
        }
    }
}
