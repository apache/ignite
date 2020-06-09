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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.spi.metric.log.LogExporterSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.IgnitionEx.gridx;
import static org.apache.ignite.internal.managers.systemview.GridSystemViewManager.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.managers.systemview.GridSystemViewManager.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests that thread pool metrics are available before the start of all Ignite components happened.
 */
public class ThreadPoolMetricsTest extends GridCommonAbstractTest {
    /** Names of the general thread pools. */
    private static final Collection<String> THREAD_POOL_NAMES = Arrays.asList(
        "GridUtilityCacheExecutor",
        "GridExecutionExecutor",
        "GridServicesExecutor",
        "GridSystemExecutor",
        "GridClassLoadingExecutor",
        "GridManagementExecutor",
        "GridIgfsExecutor",
        "GridAffinityExecutor",
        "GridCallbackExecutor",
        "GridQueryExecutor",
        "GridSchemaExecutor",
        "GridRebalanceExecutor",
        "GridRebalanceStripedExecutor",
        "GridDataStreamExecutor"
    );

    /** Names of the system views for the thread pools. */
    private static final Collection<String> THREAD_POOL_VIEWS = Arrays.asList(
        SYS_POOL_QUEUE_VIEW,
        STREAM_POOL_QUEUE_VIEW
    );

    /** Latch that indicates whether the start of Ignite components was launched. */
    public final CountDownLatch startLaunchedLatch = new CountDownLatch(1);

    /** Latch that indicates whether the start of Ignite components was unblocked. */
    public final CountDownLatch startUnblockedLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setMetricExporterSpi(new LogExporterSpi())
            .setPluginProviders(new AbstractTestPluginProvider() {
                /** {@inheritDoc} */
                @Override public String name() {
                    return "test-stuck-plugin";
                }

                /** {@inheritDoc} */
                @Override public void onIgniteStart() {
                    startLaunchedLatch.countDown();

                    try {
                        startUnblockedLatch.await(getTestTimeout(), MILLISECONDS);
                    }
                    catch (InterruptedException e) {
                        throw new IgniteException(e);
                    }
                }
            });
    }

    /**
     * Tests that thread pool metrics are available before the start of all Ignite components happened.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("Convert2MethodRef")
    public void testThreadPoolMetrics() throws Exception {
        try {
            runAsync(() -> startGrid());

            assertTrue(startLaunchedLatch.await(getTestTimeout(), MILLISECONDS));

            IgniteEx srv = gridx(getTestIgniteInstanceName());

            List<String> metrics = new ArrayList<>();

            srv.context().metric().forEach(metric -> metrics.add(metric.name()));

            assertTrue(metrics.containsAll(THREAD_POOL_NAMES.stream()
                .map(name -> metricName(GridMetricManager.THREAD_POOLS, name))
                .collect(Collectors.toList()))
            );

            assertTrue(metrics.contains(GridMetricManager.IGNITE_METRICS));

            List<String> views = new ArrayList<>();

            srv.context().systemView().forEach(view -> views.add(view.name()));

            assertTrue(views.containsAll(THREAD_POOL_VIEWS));
        }
        finally {
            startUnblockedLatch.countDown();
        }
    }
}
