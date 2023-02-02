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

package org.apache.ignite.internal.processors.service;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.Iterables;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.service.inner.MyService;
import org.apache.ignite.internal.processors.service.inner.MyServiceFactory;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.serviceMetricRegistryName;

/**
 * Tests metrics of service invocations.
 */
public class GridServiceMetricsTest extends GridCommonAbstractTest {
    /** Number of service invocations. */
    private static final int INVOKE_CNT = 50;

    /** Service name used in the tests. */
    private static final String SRVC_NAME = "TestService";

    /** Error message of created metrics. */
    private static final String METRICS_MUST_NOT_BE_CREATED = "Service metric registry must not be created.";

    /** Utility holder of current grid number. */
    private int gridNum;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** Checks service metrics are enabled / disabled properly. */
    @Test
    public void testServiceMetricsEnabledDisabled() throws Exception {
        IgniteEx ignite = startGrid();

        ServiceConfiguration srvcCfg = serviceCfg(MyServiceFactory.create(), 0, 1);

        srvcCfg.setStatisticsEnabled(false);

        ignite.services().deploy(srvcCfg);

        assertNull(METRICS_MUST_NOT_BE_CREATED, findMetricRegistry(ignite.context().metric(), SRVC_NAME));

        ignite.services().cancel(SRVC_NAME);

        srvcCfg.setStatisticsEnabled(true);

        ignite.services().deploy(srvcCfg);

        assertNotNull("Service metric registry must be created.",
            findMetricRegistry(ignite.context().metric(), SRVC_NAME));
    }

    /** Checks metric are created when service is deployed and removed when service is undeployed. */
    @Test
    public void testMetricsOnServiceDeployAndCancel() throws Exception {
        List<IgniteEx> grids = startGrids(3, false);

        // 2 services per node.
        grids.get(0).services().deploy(serviceCfg(MyServiceFactory.create(), grids.size() * 2, 2));

        awaitPartitionMapExchange();

        int expectedCnt = Arrays.stream(MyService.class.getDeclaredMethods()).map(Method::getName).collect(
            Collectors.toSet()).size();

        // Make sure metrics are registered.
        for (IgniteEx ignite : grids)
            assertEquals(metricsCnt(ignite), expectedCnt);

        grids.get(0).services().cancel(SRVC_NAME);

        awaitPartitionMapExchange();

        for (IgniteEx ignite : grids)
            assertEquals(metricsCnt(ignite), 0);
    }

    /** Tests service metrics migrates correclty with the service redeployment. */
    @Test
    public void testRedeploy() throws Exception {
        List<IgniteEx> grids = startGrids(3, false);

        // 2 services per node.
        grid(0).services().deploy(serviceCfg(MyServiceFactory.create(), 1, 0));

        awaitPartitionMapExchange();

        // Only same method metric count must persist across the cluster for the singleton.
        int expectedCnt = Arrays.stream(MyService.class.getDeclaredMethods()).map(Method::getName).collect(
            Collectors.toSet()).size();

        // Only same method metric count must persist across the cluster for the singleton.
        assertEquals("Only one metric registry can persist for one service instance", expectedCnt,
            grids.stream().map(GridServiceMetricsTest::metricsCnt).mapToInt(Integer::intValue).sum());

        for (int i = 0; i < grids.size(); ++i) {
            if (metricsCnt(grid(i)) > 0) {
                stopGrid(i);

                awaitPartitionMapExchange();

                break;
            }
        }

        // Only same method metric count must persist across the cluster for the singleton.
        assertEquals("Only one metric registry can persist for one service instance", expectedCnt,
            G.allGrids().stream().map(grid -> metricsCnt((IgniteEx)grid)).mapToInt(Integer::intValue).sum());
    }

    /** Tests service metrics for single service instance. */
    @Test
    public void testServiceMetricsSingle() throws Throwable {
        testServiceMetrics(1, 1, 1, 1);
    }

    /** Tests service metrics for multy service instance: one per server. */
    @Test
    public void testServiceMetricsMulty() throws Throwable {
        testServiceMetrics(3, 3, 3, 1);
    }

    /** Tests service metrics for multy service instance: fewer that servers and clients. */
    @Test
    public void testServiceMetricsMultyFew() throws Throwable {
        testServiceMetrics(4, 3, 2, 1);
    }

    /** Tests service metrics for multy service instance: serveral instances per node. */
    @Test
    public void testServiceMetricsMultyDuplicated() throws Throwable {
        testServiceMetrics(3, 2, 3, 3);
    }

    /** Tests service metrics for multy service instance: serveral instances per node, total fewer that servers. */
    @Test
    public void testServiceMetricsMultyFewDuplicated() throws Throwable {
        testServiceMetrics(5, 4, 3, 2);
    }

    /**
     * Invokes service in various ways: from clients, servers, etc. Checks these calls reflect in the metrics.
     *
     * @param serverCnt Number of server nodes.
     * @param clientCnt Number of client nodes.
     * @param perClusterCnt Number of service instances per cluster.
     * @param perNodeCnt Number of service instances per node.
     */
    private void testServiceMetrics(int serverCnt, int clientCnt, int perClusterCnt, int perNodeCnt) throws Throwable {
        List<IgniteEx> servers = startGrids(serverCnt, false);

        List<IgniteEx> clients = startGrids(clientCnt, true);

        servers.get(0).services().deploy(serviceCfg(MyServiceFactory.create(), perClusterCnt, perNodeCnt));

        awaitPartitionMapExchange();

        List<MyService> serverStickyProxies = servers.stream()
            .map(ignite -> (MyService)ignite.services().serviceProxy(SRVC_NAME, MyService.class, true))
            .collect(Collectors.toList());

        List<MyService> clientStickyProxies = clients.stream()
            .map(ignite -> (MyService)ignite.services().serviceProxy(SRVC_NAME, MyService.class, true))
            .collect(Collectors.toList());

        long invokeCollector = 0;

        // Call service through the server proxies.
        for (int i = 0; i < INVOKE_CNT; ++i) {
            // Call from server.
            IgniteEx ignite = servers.get(i % servers.size());

            callService4Times(ignite, serverStickyProxies.get(i % serverStickyProxies.size()));

            // Call from client.
            ignite = clients.get(i % clients.size());

            callService4Times(ignite, clientStickyProxies.get(i % clientStickyProxies.size()));

            invokeCollector += 8;
        }

        long invokesInMetrics = 0;

        // Calculate and check invocations within the metrics.
        for (IgniteEx ignite : servers) {
            ReadOnlyMetricRegistry metrics = findMetricRegistry(ignite.context().metric(), SRVC_NAME);

            // Metrics may not be deployed on this server node.
            if (metrics == null)
                continue;

            for (Metric metric : metrics) {
                if (metric instanceof HistogramMetric)
                    invokesInMetrics += sumHistogramEntries((HistogramMetric)metric);
            }
        }

        // Compare calls number and metrics number.
        assertEquals("Calculated wrong service invocation number.", invokesInMetrics, invokeCollector);
    }

    /** Expose ignite-references of the nodes as list. */
    private List<IgniteEx> startGrids(int cnt, boolean client) throws Exception {
        List<IgniteEx> grids = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; ++i)
            grids.add(client ? startClientGrid(gridNum++) : startGrid(gridNum++));

        return grids;
    }

    /**
     * Executes 2 calls for {@link MyService} though unsticky proxy and 2 calls to {@code extraSrvc}. Total 4 are
     * suposed to present in the metrics.
     *
     * @param ignite Server or client node.
     * @param extraSrvc Extra service instance or proxy to call.
     */
    private static void callService4Times(IgniteEx ignite, MyService extraSrvc) {
        MyService srvc = ignite.services().serviceProxy(SRVC_NAME, MyService.class, false);

        srvc.hello();

        srvc.hello(1);

        extraSrvc.hello();

        extraSrvc.hello(2);
    }

    /** Provides test service configuration. */
    private static ServiceConfiguration serviceCfg(Service srvc, int perClusterCnt, int perNodeCnt) {
        ServiceConfiguration svcCfg = new ServiceConfiguration();

        svcCfg.setName(SRVC_NAME);
        svcCfg.setService(srvc);
        svcCfg.setMaxPerNodeCount(perNodeCnt);
        svcCfg.setTotalCount(perClusterCnt);
        svcCfg.setStatisticsEnabled(true);

        return svcCfg;
    }

    /** @return Number of metrics contained in metric registry of the test service. */
    private static int metricsCnt(IgniteEx ignite) {
        return Iterables.size(ignite.context().metric().registry(serviceMetricRegistryName(SRVC_NAME)));
    }

    /**
     * Count total of histogram values.
     *
     * @param histogram Histogram to traverse.
     * @return Sum of all entries of {@code histogram} buckets.
     */
    public static long sumHistogramEntries(HistogramMetric histogram) {
        if (histogram == null)
            return 0;

        long sum = 0;

        for (int i = 0; i < histogram.value().length; ++i)
            sum += histogram.value()[i];

        return sum;
    }

    /** @return Metric registry if it is found in {@code metricMgr} by name {@code srvcName}. Null otherwise. */
    private static ReadOnlyMetricRegistry findMetricRegistry(GridMetricManager metricMgr, String srvcName) {
        for (ReadOnlyMetricRegistry registry : metricMgr) {
            if (registry.name().equals(serviceMetricRegistryName(srvcName)))
                return registry;
        }

        return null;
    }
}
