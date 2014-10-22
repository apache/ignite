/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import junit.framework.*;
import org.gridgain.grid.*;
import org.gridgain.grid.service.*;

import java.util.concurrent.*;

/**
 * Single node services test.
 */
public class GridServiceProcessorMultiNodeSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 4;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiNodeProxy() throws Exception {
        Grid grid = randomGrid();

        int extras = 3;

        startExtraNodes(extras);

        String name = "testMultiNodeProxy";

        grid.services().deployNodeSingleton(name, new CounterServiceImpl()).get();

        CounterService svc = grid.services().serviceProxy(name, CounterService.class, false);

        for (int i = 0; i < extras; i++) {
            svc.increment();

            stopGrid(nodeCount() + i);
        }

        assertEquals(extras, svc.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeSingletonRemoteNotStickyProxy() throws Exception {
        String name = "testNodeSingletonRemoteNotStickyProxy";

        Grid grid = randomGrid();

        // Deploy only on remote nodes.
        grid.forRemotes().services().deployNodeSingleton(name, new CounterServiceImpl()).get();

        info("Deployed service: " + name);

        // Get local proxy.
        CounterService svc = grid.services().serviceProxy(name, CounterService.class, false);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());

        int total = 0;

        for (GridNode n : grid.forRemotes().nodes()) {
            CounterService rmtSvc = grid.forNode(n).services().serviceProxy(name, CounterService.class, false);

            int cnt = rmtSvc.localIncrements();

            // Since deployment is not stick, count on each node must be less than 10.
            assertTrue("Invalid local increments: " + cnt, cnt != 10);

            total += cnt;
        }

        assertEquals(10, total);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeSingletonRemoteStickyProxy() throws Exception {
        String name = "testNodeSingletonRemoteStickyProxy";

        Grid grid = randomGrid();

        // Deploy only on remote nodes.
        grid.forRemotes().services().deployNodeSingleton(name, new CounterServiceImpl()).get();

        // Get local proxy.
        CounterService svc = grid.services().serviceProxy(name, CounterService.class, true);

        for (int i = 0; i < 10; i++)
            svc.increment();

        assertEquals(10, svc.get());

        int total = 0;

        for (GridNode n : grid.forRemotes().nodes()) {
            CounterService rmtSvc = grid.forNode(n).services().serviceProxy(name, CounterService.class, false);

            int cnt = rmtSvc.localIncrements();

            assertTrue("Invalid local increments: " + cnt, cnt == 10 || cnt == 0);

            total += rmtSvc.localIncrements();
        }

        assertEquals(10, total);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingletonUpdateTopology() throws Exception {
        String name = "serviceSingletonUpdateTopology";

        Grid g = randomGrid();

        CountDownLatch latch = new CountDownLatch(1);

        org.gridgain.grid.kernal.processors.service.DummyService.exeLatch(name, latch);

        GridFuture<?> fut = g.services().deployClusterSingleton(name, new org.gridgain.grid.kernal.processors.service.DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        TestCase.assertEquals(name, 1, org.gridgain.grid.kernal.processors.service.DummyService.started(name));
        TestCase.assertEquals(name, 0, org.gridgain.grid.kernal.processors.service.DummyService.cancelled(name));

        int nodeCnt = 2;

        startExtraNodes(nodeCnt);

        try {
            TestCase.assertEquals(name, 1, org.gridgain.grid.kernal.processors.service.DummyService.started(name));
            TestCase.assertEquals(name, 0, org.gridgain.grid.kernal.processors.service.DummyService.cancelled(name));

            info(">>> Passed checks.");

            checkCount(name, g.services().deployedServices(), 1);
        }
        finally {
            stopExtraNodes(nodeCnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityDeployUpdateTopology() throws Exception {
        Grid g = randomGrid();

        final Integer affKey = 1;

        // Store a cache key.
        g.cache(CACHE_NAME).put(affKey, affKey.toString());

        String name = "serviceAffinityUpdateTopology";

        GridFuture<?> fut = g.services().deployKeyAffinitySingleton(name, new AffinityService(affKey),
            CACHE_NAME, affKey);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        checkCount(name, g.services().deployedServices(), 1);

        int nodeCnt = 2;

        startExtraNodes(nodeCnt);

        try {
            checkCount(name, g.services().deployedServices(), 1);
        }
        finally {
            stopExtraNodes(nodeCnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOnEachNodeUpdateTopology() throws Exception {
        String name = "serviceOnEachNodeUpdateTopology";

        Grid g = randomGrid();

        CountDownLatch latch = new CountDownLatch(nodeCount());

        org.gridgain.grid.kernal.processors.service.DummyService.exeLatch(name, latch);

        GridFuture<?> fut = g.services().deployNodeSingleton(name, new org.gridgain.grid.kernal.processors.service.DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        TestCase.assertEquals(name, nodeCount(), org.gridgain.grid.kernal.processors.service.DummyService.started(name));
        TestCase.assertEquals(name, 0, org.gridgain.grid.kernal.processors.service.DummyService.cancelled(name));

        int newNodes = 2;

        latch = new CountDownLatch(newNodes);

        org.gridgain.grid.kernal.processors.service.DummyService.exeLatch(name, latch);

        startExtraNodes(newNodes);

        try {
            latch.await();

            TestCase.assertEquals(name, nodeCount() + newNodes, org.gridgain.grid.kernal.processors.service.DummyService.started(name));
            TestCase.assertEquals(name, 0, org.gridgain.grid.kernal.processors.service.DummyService.cancelled(name));

            checkCount(name, g.services().deployedServices(), nodeCount() + newNodes);
        }
        finally {
            stopExtraNodes(newNodes);
        }
    }

    /**
     * Dummy interface for testing purposes.
     */
    private interface DummyService extends GridService {
        /**
         * @return Some integer value.
         */
        int getInt();
    }
}