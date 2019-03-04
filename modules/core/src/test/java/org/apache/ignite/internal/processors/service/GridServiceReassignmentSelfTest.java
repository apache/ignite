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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests service reassignment.
 */
public class GridServiceReassignmentSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** */
    private static final String SERVICE_NAME = "testService";

    /** */
    private static final long SERVICE_TOP_WAIT_TIMEOUT = 2_000L;

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterSingleton() throws Exception {
        checkReassigns(1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeSingleton() throws Exception {
        checkReassigns(0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimited1() throws Exception {
        checkReassigns(5, 2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimited2() throws Exception {
        checkReassigns(7, 3);
    }

    /**
     * @throws Exception If failed.
     */
    private CounterService proxy(Ignite g) throws Exception {
        return g.services().serviceProxy(SERVICE_NAME, CounterService.class, false);
    }

    /**
     * @param total Total number of services.
     * @param maxPerNode Maximum number of services per node.
     * @throws IgniteCheckedException If failed.
     */
    private void checkReassigns(int total, int maxPerNode) throws Exception {
        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.exeLatch(SERVICE_NAME, latch);

        grid(0).services().deployMultiple(SERVICE_NAME, new CounterServiceImpl(), total, maxPerNode);

        for (int i = 0; i < 10; i++)
            proxy(randomGrid()).increment();

        Collection<Integer> startedGrids = new HashSet<>();

        try {
            startedGrids.add(0);

            int maxTopSize = 5;

            boolean grow = true;

            Random rnd = new Random();

            for (int i = 0; i < 20; i++) {
                if (grow) {
                    assert startedGrids.size() < maxTopSize;

                    startRandomNodesMultithreaded(maxTopSize, rnd, startedGrids);

                    if (startedGrids.size() == maxTopSize)
                        grow = false;
                }
                else {
                    assert startedGrids.size() > 1;

                    int gridIdx = nextRandomIdx(startedGrids, rnd);

                    stopGrid(gridIdx);

                    startedGrids.remove(gridIdx);

                    if (startedGrids.size() == 1)
                        grow = true;
                }

                for (int attempt = 0; attempt <= 10; ++attempt) {
                    U.sleep(500);

                    if (checkServices(total, maxPerNode, F.first(startedGrids), attempt == 10))
                        break;
                }
            }
        }
        finally {
            grid(F.first(startedGrids)).services().cancel(SERVICE_NAME);

            stopAllGrids();

            startGrid(0);
        }
    }

    /**
     * Checks services assignments.
     *
     * @param total Total number of services.
     * @param maxPerNode Maximum number of services per node.
     * @param gridIdx Grid index to check.
     * @param lastTry Last try flag.
     * @throws Exception If failed.
     * @return {@code True} if check passed.
     */
    private boolean checkServices(int total, int maxPerNode, int gridIdx, boolean lastTry) throws Exception {
        IgniteEx grid = grid(gridIdx);

        waitForServicesReadyTopology(grid, grid.context().discovery().topologyVersionEx());

        Map<UUID, Integer> srvcTop = grid.context().service().serviceTopology(SERVICE_NAME, SERVICE_TOP_WAIT_TIMEOUT);

        Collection<UUID> nodes = F.viewReadOnly(grid.context().discovery().aliveServerNodes(), F.node2id());

        assertNotNull("Grid assignments object is null", srvcTop);

        int sum = 0;

        for (Map.Entry<UUID, Integer> entry : srvcTop.entrySet()) {
            UUID nodeId = entry.getKey();

            if (!lastTry && !nodes.contains(nodeId))
                return false;

            assertTrue("Dead node is in assignments: " + nodeId, nodes.contains(nodeId));

            Integer nodeCnt = entry.getValue();

            if (maxPerNode > 0)
                assertTrue("Max per node limit exceeded [nodeId=" + nodeId + ", max=" + maxPerNode +
                    ", actual=" + nodeCnt, nodeCnt <= maxPerNode);

            sum += nodeCnt;
        }

        if (total > 0)
            assertTrue("Total number of services limit exceeded [sum=" + sum +
                ", assigns=" + srvcTop + ']', sum <= total);
        else
            assertEquals("Reassign per node failed.", nodes.size(), srvcTop.size());

        if (!lastTry && proxy(grid).get() != 10)
            return false;

        assertEquals(10, proxy(grid).get());

        return true;
    }

    /**
     * Start 1, 2 or 3 random nodes simultaneously.
     *
     * @param limit Cluster size limit.
     * @param rnd Randmo generator.
     * @param grids Collection with indexes of running nodes.
     * @throws Exception If failed.
     */
    private void startRandomNodesMultithreaded(int limit, Random rnd, Collection<Integer> grids) throws Exception {
        int cnt = rnd.nextInt(Math.min(limit - grids.size(), 3)) + 1;

        for (int i = 1; i <= cnt; i++) {
            int gridIdx = nextAvailableIdx(grids, limit, rnd);

            if (i == cnt)
                startGrid(gridIdx);
            else
                GridTestUtils.runAsync(() -> startGrid(gridIdx));

            grids.add(gridIdx);
        }
    }

    /**
     * Gets next available index.
     *
     * @param startedGrids Indexes for started grids.
     * @param maxTopSize Max topology size.
     * @return Next available index.
     */
    private int nextAvailableIdx(Collection<Integer> startedGrids, int maxTopSize, Random rnd) {
        while (true) {
            int idx = rnd.nextInt(maxTopSize);

            if (!startedGrids.contains(idx))
                return idx;
        }
    }

    /**
     * @param startedGrids Started grids.
     * @param rnd Random numbers generator.
     * @return Randomly chosen started grid.
     */
    private int nextRandomIdx(Iterable<Integer> startedGrids, Random rnd) {
        while (true) {
            for (Integer idx : startedGrids) {
                if (rnd.nextBoolean())
                    return idx;
            }
        }
    }
}
