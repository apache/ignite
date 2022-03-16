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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
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
import org.apache.ignite.plugin.AbstractTestPluginProvider;
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

/**
 * Tests that thread pool metrics are available before the start of all Ignite components happened.
 */
public class ThreadPoolMetricsTest extends GridCommonAbstractTest {
    /**  Custom executor name. */
    private static final String CUSTOM_EXEC_NAME = "user-pool";

    /** Names of the system views for the thread pools. */
    private static final Collection<String> THREAD_POOL_VIEWS = Arrays.asList(
        SYS_POOL_QUEUE_VIEW,
        STREAM_POOL_QUEUE_VIEW
    );

    /** Mapping of the metric group name to the thread pool instance. */
    private static final Map<String, Function<PoolProcessor, ExecutorService>> THREAD_POOL_METRICS =
        new HashMap<String, Function<PoolProcessor, ExecutorService>>() {{
            put(metricName(THREAD_POOLS, "GridUtilityCacheExecutor"), PoolProcessor::utilityCachePool);
            put(metricName(THREAD_POOLS, "GridExecutionExecutor"), PoolProcessor::getExecutorService);
            put(metricName(THREAD_POOLS, "GridServicesExecutor"), PoolProcessor::getServiceExecutorService);
            put(metricName(THREAD_POOLS, "GridSystemExecutor"), PoolProcessor::getSystemExecutorService);
            put(metricName(THREAD_POOLS, "GridClassLoadingExecutor"), PoolProcessor::getPeerClassLoadingExecutorService);
            put(metricName(THREAD_POOLS, "GridManagementExecutor"), PoolProcessor::getManagementExecutorService);
            put(metricName(THREAD_POOLS, "GridAffinityExecutor"), PoolProcessor::getAffinityExecutorService);
            put(metricName(THREAD_POOLS, "GridCallbackExecutor"), PoolProcessor::asyncCallbackPool);
            put(metricName(THREAD_POOLS, "GridQueryExecutor"), PoolProcessor::getQueryExecutorService);
            put(metricName(THREAD_POOLS, "GridSchemaExecutor"), PoolProcessor::getSchemaExecutorService);
            put(metricName(THREAD_POOLS, "GridRebalanceExecutor"), PoolProcessor::getRebalanceExecutorService);
            put(metricName(THREAD_POOLS, "GridRebalanceStripedExecutor"), PoolProcessor::getStripedRebalanceExecutorService);
            put(metricName(THREAD_POOLS, "GridThinClientExecutor"), PoolProcessor::getThinClientExecutorService);
            put(metricName(THREAD_POOLS, "GridDataStreamExecutor"), PoolProcessor::getDataStreamerExecutorService);
            put(metricName(THREAD_POOLS, "StripedExecutor"), PoolProcessor::getStripedExecutorService);
            put(metricName(THREAD_POOLS, "GridRestExecutor"), PoolProcessor::getRestExecutorService);
            put(metricName(THREAD_POOLS, "GridSnapshotExecutor"), PoolProcessor::getSnapshotExecutorService);
            put(metricName(THREAD_POOLS, "GridReencryptionExecutor"), PoolProcessor::getReencryptionExecutorService);
            put(metricName(THREAD_POOLS, CUSTOM_EXEC_NAME), proc -> (ExecutorService)proc.customExecutor(CUSTOM_EXEC_NAME));
        }};

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
}
