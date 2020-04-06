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

import com.google.common.collect.Iterables;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.service.inner.MyService;
import org.apache.ignite.internal.processors.service.inner.MyServiceFactory;
import org.apache.ignite.internal.processors.service.inner.NamingService;
import org.apache.ignite.internal.processors.service.inner.NamingServiceImpl;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.MAX_ABBREVIATE_NAME_LVL;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.methodMetricName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.serviceMetricRegistryName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.sumHistogramEntries;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SERVICE_METRIC_REGISTRY;

/**
 * Tests metrics of service invocations.
 */
public class GridServiceMetricsTest extends GridCommonAbstractTest {
    /** Number of service invocations. */
    private static final int INVOKE_CNT = 50;

    /** Service name used in the tests. */
    private static final String SRVC_NAME = GridServiceMetricsTest.class.getSimpleName()+"_service";

    /** Error message of created metrics. */
    private static final String METRICS_MUST_NOT_BE_CREATED = "Service metric registry must not be created.";

    /** Utility holder of current grid number. */
    private int gridNum;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // JMX metrics exposition to see actual namings and placement of the metrics.
        cfg.setMetricExporterSpi(new JmxMetricExporterSpi());

        return cfg;
    }

    /** Checks service metrics are enabled / disabled properly. */
    @Test
    public void testServiceMetricsEnabledDisabled() throws Exception {
        IgniteEx ignite = startGrid();

        ServiceConfiguration srvcCfg = serviceCfg(SRVC_NAME, 0, 1);

        srvcCfg.setStatisticsEnabled(false);

        ignite.services().deploy(srvcCfg);

        assertNull(METRICS_MUST_NOT_BE_CREATED, findMetricRegistry(ignite.context().metric(), SRVC_NAME));

        ignite.services().cancel(SRVC_NAME);

        srvcCfg.setStatisticsEnabled(true);

        ignite.services().deploy(srvcCfg);

        assertNotNull("Service metric registry must be created.",
            findMetricRegistry(ignite.context().metric(), SRVC_NAME));
    }

    /** Checks metric behaviour when launched several service instances. */
    @Test
    public void testMultipleDeployment() throws Throwable {
        List<IgniteEx> servers = new ArrayList<>();

        servers.add(startGrid(gridNum++));

        IgniteEx server = servers.get(0);

        IgniteEx client = startClientGrid(gridNum++);

        assertNull(METRICS_MUST_NOT_BE_CREATED, findMetricRegistry(server.context().metric(), SERVICE_METRIC_REGISTRY));

        int totalInstance = 2;

        int perNode = 2;

        ServiceConfiguration srvcCfg = server.services().multipleConfiguration(SRVC_NAME, MyServiceFactory.create(),
            totalInstance, perNode);

        srvcCfg.setStatisticsEnabled(true);

        server.services().deploy(srvcCfg);

        awaitPartitionMapExchange();

        // Call proxies on the clients.
        Stream.generate(() -> client.services().serviceProxy(SRVC_NAME, MyService.class, true))
            .limit(totalInstance).forEach(srvc -> ((MyService)srvc).hello());

        ReadOnlyMetricRegistry metrics = findMetricRegistry(server.context().metric(), SRVC_NAME);

        // Total service calls number.
        int callsCnt = 0;

        for (Metric m : metrics) {
            if (m instanceof HistogramMetric)
                callsCnt += sumHistogramEntries((HistogramMetric)m);
        }

        assertEquals(callsCnt, totalInstance);

        // Add servers more than service instances.
        servers.add(startGrid(gridNum++));

        servers.add(startGrid(gridNum++));

        awaitPartitionMapExchange();

        int deployedCnt = 0;

        int metricsCnt = 0;

        for (IgniteEx ignite : servers) {
            if (ignite.services().service(SRVC_NAME) != null)
                deployedCnt++;

            if (findMetricRegistry(ignite.context().metric(), SRVC_NAME) != null)
                metricsCnt++;
        }

        assertEquals(deployedCnt, metricsCnt);

        assertEquals(metricsCnt, totalInstance);
    }

    /** Checks metric are created when service is deployed and removed when service is undeployed. */
    @Test
    public void testMetricDeplotmentUndeployment() throws Exception {
        List<IgniteEx> servers = startGrids(3, false);

        // 2 services per node.
        servers.get(0).services().deploy(serviceCfg(SRVC_NAME, servers.size(), 2));

        awaitPartitionMapExchange();

        // Make sure metrics are registered.
        for (IgniteEx ignite : servers)
            assertEquals(metricsCnt(ignite, SRVC_NAME), MyService.class.getDeclaredMethods().length);

        servers.get(0).services().cancel(SRVC_NAME);

        awaitPartitionMapExchange();

        for (IgniteEx ignite : servers)
            assertEquals(metricsCnt(ignite, SRVC_NAME), 0);
    }

    /** Checks two different histograms are created for the same short names of service methods. */
    @Test
    public void testMetricNaming() throws Exception {
        IgniteEx ignite = startGrid(1);

        ignite.services().deploy(ignite.services().nodeSingletonConfiguration(SRVC_NAME, new NamingServiceImpl())
            .setStatisticsEnabled(true));

        MetricRegistry registry = ignite.context().metric().registry(serviceMetricRegistryName(SRVC_NAME));

        List<Metric> metricsFound = new ArrayList<>();

        for (Method mtd : NamingService.class.getDeclaredMethods()) {
            Metric curMetric = null;

            for (int i = 0; i <= MAX_ABBREVIATE_NAME_LVL; ++i) {
                String metricName = methodMetricName(mtd, i);

                if ((curMetric = registry.findMetric(metricName)) instanceof HistogramMetric
                    && !metricsFound.contains(curMetric)) {
                    metricsFound.add(curMetric);

                    break;
                }
            }

            assertNotNull("No metric found for method " + mtd, curMetric);
        }
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

    /** Tests service metrics for multy service instance: serveral instances per node */
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

        servers.get(0).services().deploy(serviceCfg(SRVC_NAME, perClusterCnt, perNodeCnt));

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

        extraSrvc.hello(1);
    }

    /** Provides test service configuration. */
    private static ServiceConfiguration serviceCfg(String svcName, int perClusterCnt, int perNodeCnt) {
        ServiceConfiguration svcCfg = new ServiceConfiguration();

        svcCfg.setName(svcName);
        svcCfg.setService(MyServiceFactory.create());
        svcCfg.setMaxPerNodeCount(perNodeCnt);
        svcCfg.setTotalCount(perClusterCnt);
        svcCfg.setStatisticsEnabled(true);

        return svcCfg;
    }

    /** @return Number of metrics contained in metric registry for {@code srvcName}. */
    private static int metricsCnt(IgniteEx ignite, String srvcName) {
        return Iterables.size(ignite.context().metric().registry(serviceMetricRegistryName(srvcName)));
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
