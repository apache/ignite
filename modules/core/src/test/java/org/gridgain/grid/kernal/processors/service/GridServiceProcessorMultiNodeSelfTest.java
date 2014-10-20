/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

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
    public void testSingletonUpdateTopology() throws Exception {
        String name = "serviceSingletonUpdateTopology";

        Grid g = randomGrid();

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        GridFuture<?> fut = g.services().deployClusterSingleton(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, 1, DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        int nodeCnt = 2;

        startExtraNodes(nodeCnt);

        try {
            assertEquals(name, 1, DummyService.started(name));
            assertEquals(name, 0, DummyService.cancelled(name));

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

        CountDownLatch latch = new CountDownLatch(1);

        String name = "serviceAffinityUpdateTopology";
        GridFuture<?> fut = g.services().deployKeyAffinitySingleton(name, new AffinityService(latch, affKey),
            CACHE_NAME, affKey);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

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

        DummyService.exeLatch(name, latch);

        GridFuture<?> fut = g.services().deployNodeSingleton(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(name, nodeCount(), DummyService.started(name));
        assertEquals(name, 0, DummyService.cancelled(name));

        int newNodes = 2;

        latch = new CountDownLatch(newNodes);

        DummyService.exeLatch(name, latch);

        startExtraNodes(newNodes);

        try {
            latch.await();

            assertEquals(name, nodeCount() + newNodes, DummyService.started(name));
            assertEquals(name, 0, DummyService.cancelled(name));

            checkCount(name, g.services().deployedServices(), nodeCount() + newNodes);
        }
        finally {
            stopExtraNodes(newNodes);
        }
    }

    /**
     * @return Instance of {@code GridDummyService} for testing purposes.
     */
    private static GridDummyService getDummyService() {
        return new GridDummyService() {
            private DummyService ds = new DummyService();

            @Override public int getInt() {
                return 239;
            }

            @Override public void cancel(GridServiceContext ctx) {
                ds.cancel(ctx);
            }

            @Override public void execute(GridServiceContext ctx) throws Exception {
                ds.execute(ctx);
            }
        };
    }

    /**
     * @throws Exception If failed.
     */
    public void testClusterSingletonServiceProxy() throws Exception {
        String name = "remoteService";

        Grid gLoc = startGrid(getTestGridName() + "local");

        Grid gRemote = startGrid(getTestGridName() + "remote");

        GridFuture<?> fut = gRemote.services().deployClusterSingleton(name, getDummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        GridDummyService proxy = gLoc.services().serviceProxy(name, GridDummyService.class, true);

        assertEquals("Proxy service was not executed", 239, proxy.getInt());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeSingletonServiceProxy() throws Exception {
        Grid g = randomGrid();

        startExtraNodes(2);

        String name = "dummyService";

        GridFuture<?> fut = g.services().deployNodeSingleton(name, getDummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        GridDummyService proxy = g.services().serviceProxy(name, GridDummyService.class, true);

        assertEquals("Proxy service was not executed", 239, proxy.getInt());
    }

    /**
     * Dummy interface for testing purposes.
     */
    private interface GridDummyService extends GridService {
        /**
         * @return Some integer value.
         */
        int getInt();
    }
}