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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.IgnitionEx.gridx;
import static org.apache.ignite.internal.managers.systemview.GridSystemViewManager.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.managers.systemview.GridSystemViewManager.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.THREAD_POOLS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

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
        metricName(THREAD_POOLS, "GridRebalanceStripedExecutor"),
        metricName(THREAD_POOLS, "GridDataStreamExecutor")
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
        IgniteInternalFuture<IgniteEx> startFut = runAsync(() -> startGrid());

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
}
