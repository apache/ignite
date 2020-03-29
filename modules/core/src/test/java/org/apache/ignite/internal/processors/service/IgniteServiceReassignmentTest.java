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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.Test;

/**
 *
 */
public class IgniteServiceReassignmentTest extends GridCommonAbstractTest {
    /** */
    private ServiceConfiguration srvcCfg;

    /** */
    private boolean useStrLog;

    /** */
    private List<IgniteLogger> strLoggers = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (srvcCfg != null)
            cfg.setServiceConfiguration(srvcCfg);

        if (useStrLog) {
            GridStringLogger strLog = new GridStringLogger(false, cfg.getGridLogger());

            strLog.logLength(100 * 1024);

            cfg.setGridLogger(strLog);

            strLoggers.add(strLog);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRestart1() throws Exception {
        srvcCfg = serviceConfiguration();

        IgniteEx node1 = startGrid(1);

        waitForService(node1);

        assertEquals(42, serviceProxy(node1).foo());

        srvcCfg = serviceConfiguration();

        IgniteEx node2 = startGrid(2);

        node1.close();

        waitForService(node2);

        assertEquals(42, serviceProxy(node2).foo());

        srvcCfg = serviceConfiguration();

        IgniteEx node3 = startGrid(3);

        waitForService(node3);

        assertEquals(42, serviceProxy(node3).foo());

        srvcCfg = serviceConfiguration();

        node1 = startGrid(1);

        waitForService(node1);
        waitForService(node2);
        waitForService(node3);

        assertEquals(42, serviceProxy(node1).foo());
        assertEquals(42, serviceProxy(node2).foo());
        assertEquals(42, serviceProxy(node3).foo());

        node2.close();

        waitForService(node1);
        waitForService(node3);

        assertEquals(42, serviceProxy(node1).foo());
        assertEquals(42, serviceProxy(node3).foo());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRestart2() throws Exception {
        startGrids(3);

        ServiceConfiguration svcCfg = new ServiceConfiguration();

        svcCfg.setName("DummyService");
        svcCfg.setTotalCount(10);
        svcCfg.setMaxPerNodeCount(1);
        svcCfg.setService(new DummyService());

        ignite(0).services().deploy(svcCfg);

        for (int i = 0; i < 3; i++)
            assertEquals(42, serviceProxy(ignite(i)).foo());

        for (int i = 0; i < 3; i++)
            startGrid(i + 3);

        for (int i = 0; i < 3; i++)
            stopGrid(i);

        for (int i = 0; i < 3; i++)
            assertEquals(42, serviceProxy(ignite(i + 3)).foo());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRestartRandom() throws Exception {
        final int NODES = 5;

        Ignite ignite = startGridsMultiThreaded(NODES);

        ignite.services().deploy(serviceConfiguration());

        for (int i = 0; i < 30; i++) {
            log.info("Iteration: " + i);

            int stopIdx = ThreadLocalRandom.current().nextInt(NODES);

            stopGrid(stopIdx);

            for (int nodeIdx = 0; nodeIdx < NODES; nodeIdx++) {
                if (nodeIdx == stopIdx)
                    continue;

                waitForService(grid(nodeIdx));

                assertEquals(42, serviceProxy(ignite(nodeIdx)).foo());
            }

            startGrid(stopIdx);

            for (int nodeIdx = 0; nodeIdx < NODES; nodeIdx++)
                assertEquals(42, serviceProxy(ignite(nodeIdx)).foo());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testZombieAssignmentsCleanup() throws Exception {
        Assume.assumeTrue(!isEventDrivenServiceProcessorEnabled());

        useStrLog = true;

        final int nodesCnt = 2;
        final int maxSvc = 30;

        try {
            startGridsMultiThreaded(nodesCnt);

            IgniteEx ignite = grid(0);

            IgniteInternalCache<GridServiceAssignmentsKey, Object> sysCache = ignite.utilityCache();

            List<GridServiceAssignmentsKey> zombieAssignmentsKeys = new ArrayList<>(maxSvc);

            // Adding some assignments without deployments.
            for (int i = 0; i < maxSvc; i++) {
                String name = "svc-" + i;

                ServiceConfiguration svcCfg = new ServiceConfiguration();

                svcCfg.setName(name);

                GridServiceAssignmentsKey key = new GridServiceAssignmentsKey(name);

                UUID nodeId = grid(i % nodesCnt).localNode().id();

                sysCache.put(key, new GridServiceAssignments(svcCfg, nodeId, ignite.cluster().topologyVersion()));

                zombieAssignmentsKeys.add(key);
            }

            // Simulate exchange with merge.
            GridTestUtils.runAsync(() -> startGrid(nodesCnt));
            GridTestUtils.runAsync(() -> startGrid(nodesCnt + 1));
            startGrid(nodesCnt + 2);

            awaitPartitionMapExchange();

            // Checking that all our assignments was removed.
            for (GridServiceAssignmentsKey key : zombieAssignmentsKeys)
                assertNull("Found assignment for undeployed service " + key.name(), sysCache.get(key));

            for (IgniteLogger logger : strLoggers)
                assertFalse(logger.toString().contains("Getting affinity for topology version earlier than affinity is " +
                    "calculated"));
        } finally {
            useStrLog = false;

            strLoggers.clear();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeStopWhileThereAreCacheActivitiesInServiceProcessor() throws Exception {
        Assume.assumeTrue(!isEventDrivenServiceProcessorEnabled());

        final int nodesCnt = 2;
        final int maxSvc = 1024;

        startGridsMultiThreaded(nodesCnt);

        IgniteEx ignite = grid(0);

        IgniteInternalCache<GridServiceAssignmentsKey, Object> sysCache = ignite.utilityCache();

        // Adding some assignments without deployments.
        for (int i = 0; i < maxSvc; i++) {
            String name = "svc-" + i;

            ServiceConfiguration svcCfg = new ServiceConfiguration();

            svcCfg.setName(name);

            GridServiceAssignmentsKey key = new GridServiceAssignmentsKey(name);

            UUID nodeId = grid(i % nodesCnt).localNode().id();

            sysCache.put(key, new GridServiceAssignments(svcCfg, nodeId, ignite.cluster().topologyVersion()));
        }

        // Simulate exchange with merge.
        GridTestUtils.runAsync(() -> startGrid(nodesCnt));
        GridTestUtils.runAsync(() -> startGrid(nodesCnt + 1));
        startGrid(nodesCnt + 2);

        Thread.sleep((int)(1000 * ThreadLocalRandom.current().nextDouble()));

        stopAllGrids();
    }

    /**
     * @param node Node.
     * @throws Exception If failed.
     */
    private void waitForService(final IgniteEx node) throws Exception {
        if (node.context().service() instanceof IgniteServiceProcessor)
            waitForServicesReadyTopology(node, node.context().discovery().topologyVersionEx());
        else {
            assertTrue(GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    try {
                        serviceProxy(node).foo();

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            }, 5000));
        }
    }

    /**
     * @param node Node.
     * @return Service proxy.
     */
    private static MyService serviceProxy(Ignite node) {
        return node.services().serviceProxy("DummyService", MyService.class, true);
    }

    /**
     * @return Service configuration.
     */
    private ServiceConfiguration serviceConfiguration() {
        ServiceConfiguration svc = new ServiceConfiguration();

        svc.setName("DummyService");
        svc.setTotalCount(1);
        svc.setService(new DummyService());

        return svc;
    }

    /**
     *
     */
    public interface MyService {
        /**
         * @return Dummy result.
         */
        int foo();
    }

    /**
     *
     */
    static class DummyService implements MyService, Service {
        /** */
        @IgniteInstanceResource
        private Ignite locNode;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            locNode.log().info("Service cancelled [execId=" + ctx.executionId() +
                ", node=" + locNode.cluster().localNode() + ']');
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) {
            locNode.log().info("Service initialized [execId=" + ctx.executionId() +
                ", node=" + locNode.cluster().localNode() + ']');
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) {
            locNode.log().info("Service started [execId=" + ctx.executionId() +
                ", node=" + locNode.cluster().localNode() + ']');
        }

        /** {@inheritDoc} */
        @Override public int foo() {
            locNode.log().info("Service called.");

            return 42;
        }
    }
}
