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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import com.google.common.collect.Iterables;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.sumHistogramEntries;
import static org.apache.ignite.internal.processors.service.ServiceContextImpl.SERVICE_METRIC_REGISTRY;

/**
 * Tests metrics of service invocations.
 */
public class GridServiceMetricsTest extends GridCommonAbstractTest {
    /** Number of service invocations. */
    private static final int INVOKE_CNT = 100;

    /** Service name used in the tests. */
    private static final String SRVC_NAME = "TestService";

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

        ServiceConfiguration srvcCfg = serviceCfg(new TheService(), 0, 1);

        srvcCfg.setStatisticsEnabled(false);

        ignite.services().deploy(srvcCfg);

        assertNull("Service metric registry must not be created yet.", findMetricRegistry(ignite.context().metric(), SRVC_NAME));

        ignite.services().cancel(SRVC_NAME);

        srvcCfg.setStatisticsEnabled(true);

        ignite.services().deploy(srvcCfg);

        ignite.services().serviceProxy(SRVC_NAME, ExtendedTestService.class, false).foo();

        assertNotNull("Service metric registry must be created.", findMetricRegistry(ignite.context().metric(), SRVC_NAME));

        ignite.services().cancel(SRVC_NAME);

        assertNull("Service metric registry must be destroyed already.", findMetricRegistry(ignite.context().metric(), SRVC_NAME));
    }

    /** Makes sure service metrics aren't lost with service redeployment. */
    @Test
    public void testRedeploy() throws Exception {
        IgniteEx ig = startGrid();

        ig.services().deploy(serviceCfg(new TheService(), 2, 2));

        int invoked = callService(ig);

        ReadOnlyMetricRegistry metrics = findMetricRegistry(ig.context().metric(), SRVC_NAME);

        assertNotNull(metrics);

        int metricCnt = 0;

        for (Metric metric : metrics) {
            if (metric instanceof HistogramMetric)
                metricCnt += sumHistogramEntries((HistogramMetric)metric);
        }

        assertEquals(invoked, metricCnt);

        startGrid(1);

        metricCnt = 0;
        metrics = findMetricRegistry(ig.context().metric(), SRVC_NAME);

        assertNotNull(metrics);

        for (Metric metric : metrics) {
            if (metric instanceof HistogramMetric)
                metricCnt += sumHistogramEntries((HistogramMetric)metric);
        }

        assertEquals(invoked, metricCnt);
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

        servers.get(0).services().deploy(serviceCfg(new TheService(), perClusterCnt, perNodeCnt));

        awaitPartitionMapExchange();

        long invokeCollector = 0;

        // Call service through the server proxies.
        for (int i = 0; i < INVOKE_CNT; ++i) {
            invokeCollector += callService(servers.get(i % servers.size()));

            // Call from client.
            invokeCollector += callService(clients.get(i % clients.size()));
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
     * Executes the service several times.
     *
     * @param ignite Server or client node.
     * @return Number of invocations done.
     */
    private static int callService(Ignite ignite) {
        ExtendedTestService srvc = ignite.services().serviceProxy(SRVC_NAME, ExtendedTestService.class, true);

        srvc.foo("a");
        srvc.foo();
        srvc.bar(1);
        srvc.bar();

        srvc = ignite.services().serviceProxy(SRVC_NAME, ExtendedTestService.class, false);

        srvc.foo("a");
        srvc.foo();
        srvc.bar(1);
        srvc.bar();

        return 8;
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

    /** @return Number of metrics contained in metric registry for {@code srvcName}. */
    private static int metricsCnt(IgniteEx ignite, String srvcName) {
        return Iterables.size(ignite.context().metric().registry(metricName(SERVICE_METRIC_REGISTRY, srvcName)));
    }

    /** @return Metric registry if it is found in {@code metricMgr} by name {@code srvcName}. Null otherwise. */
    private static ReadOnlyMetricRegistry findMetricRegistry(GridMetricManager metricMgr, String srvcName) {
        for (ReadOnlyMetricRegistry registry : metricMgr) {
            if (registry.name().equals(metricName(SERVICE_METRIC_REGISTRY, srvcName)))
                return registry;
        }

        return null;
    }

    /** */
    public static interface TestService {
        /** */
        void foo();

        /** */
        int bar();
    }

    /** */
    public static interface ExtendedTestService extends TestService, Externalizable {
        /** */
        String foo(String p);

        /** */
        int bar(int p);
    }

    /** Simple implementation of the test service. */
    public static class TheService implements Service, ExtendedTestService {
        /** */
        private final Random rnd;

        /** */
        public TheService(){
            rnd = new Random();
        }

        /** {@inheritDoc} */
        @Override public String foo(String p) {
            sleep();

            return p;
        }

        /** {@inheritDoc} */
        @Override public int bar(int p) {
            sleep();

            return p + rnd.nextInt(p);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            sleep();
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            sleep();
        }

        /** {@inheritDoc} */
        @Override public void foo() {
            sleep();
        }

        /** {@inheritDoc} */
        @Override public int bar() {
            sleep();

            return rnd.nextInt();
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            sleep();
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            sleep();
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            sleep();
        }

        /** */
        private void sleep() {
            try {
                Thread.sleep(rnd.nextInt(30));
            }
            catch (InterruptedException ignored) {
                // No-op;
            }
        }
    }
}
