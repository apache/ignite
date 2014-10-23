/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Tests service reassignment.
 */
public class GridServiceReassignmentSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClusterSingleton() throws Exception {
        checkReassigns(1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeSingleton() throws Exception {
        checkReassigns(0, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLimited1() throws Exception {
        checkReassigns(5, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLimited2() throws Exception {
        checkReassigns(7, 3);
    }

    /**
     * @param total Total number of services.
     * @param maxPerNode Maximum number of services per node.
     * @throws GridException If failed.
     */
    private void checkReassigns(int total, int maxPerNode) throws Exception {
        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.exeLatch("testService", latch);

        grid(0).services().deployMultiple("testService", new DummyService(), total, maxPerNode);

        Collection<Integer> startedGrids = new HashSet<>();

        try {
            startedGrids.add(0);

            int maxTopSize = 5;

            boolean grow = true;

            Random rnd = new Random();

            for (int i = 0; i < 20; i++) {
                if (grow) {
                    assert startedGrids.size() < maxTopSize;

                    int gridIdx = nextAvailableIdx(startedGrids, maxTopSize, rnd);

                    startGrid(gridIdx);

                    startedGrids.add(gridIdx);

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

                U.sleep(500);

                checkServices(total, maxPerNode, F.first(startedGrids));
            }
        }
        finally {
            grid(F.first(startedGrids)).services().cancel("testService");

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
     * @throws Exception If failed.
     */
    private void checkServices(int total, int maxPerNode, int gridIdx) throws Exception {
        GridEx grid = grid(gridIdx);

        GridCacheProjectionEx<GridServiceAssignmentsKey, GridServiceAssignments> cache = grid.
            utilityCache(GridServiceAssignmentsKey.class, GridServiceAssignments.class);

        GridServiceAssignments assignments = cache.get(new GridServiceAssignmentsKey("testService"));

        Collection<UUID> nodes = F.viewReadOnly(grid.nodes(), F.node2id());

        int sum = 0;

        for (Map.Entry<UUID, Integer> entry : assignments.assigns().entrySet()) {
            UUID nodeId = entry.getKey();

            assertTrue("Dead node is in assignments: " + nodeId, nodes.contains(nodeId));

            Integer nodeCnt = entry.getValue();

            if (maxPerNode > 0)
                assertTrue("Max per node limit exceeded [nodeId=" + nodeId + ", max=" + maxPerNode +
                    ", actual=" + nodeCnt, nodeCnt <= maxPerNode);

            sum += nodeCnt;
        }

        if (total > 0)
            assertTrue("Total number of services limit exceeded [sum=" + sum +
                ", assigns=" + assignments.assigns() + ']', sum <= total);
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

    private int nextRandomIdx(Iterable<Integer> startedGrids, Random rnd) {
        while (true) {
            for (Integer idx : startedGrids) {
                if (rnd.nextBoolean())
                    return idx;
            }
        }
    }

}
