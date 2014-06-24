// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.*;

import java.util.concurrent.*;

/**
 * Single node services test.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridServiceProcessorMultiNodeSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    private void startExtraNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++)
            startGrid(nodeCount() + i);
    }

    /**
     * @throws Exception If failed.
     */
    private void stopExtraNodes(int cnt) throws Exception {
        for (int i = 0; i < cnt; i++)
            stopGrid(nodeCount() + i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingletonUpdateTopology() throws Exception {
        Grid g = randomGrid();

        String name = "serviceSingletonUpdateTopology";

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.latch(latch);

        GridFuture<?> fut = g.services().deploySingleton(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(1, DummyService.started());
        assertFalse(DummyService.isCancelled());

        int nodeCnt = 1;

        startExtraNodes(nodeCnt);

        try {
            assertEquals(1, DummyService.started());
            assertFalse(DummyService.isCancelled());

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
        Grid g = randomGrid();

        String name = "serviceOnEachNodeUpdateTopology";

        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.latch(latch);

        GridFuture<?> fut = g.services().deployOnEachNode(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(nodeCount(), DummyService.started());
        assertFalse(DummyService.isCancelled());

        int newNodes = 1;

        latch = new CountDownLatch(newNodes);

        DummyService.latch(latch);

        startExtraNodes(newNodes);

        try {
            latch.await();

            assertEquals(nodeCount() + newNodes, DummyService.started());
            assertFalse(DummyService.isCancelled());

            checkCount(name, g.services().deployedServices(), nodeCount() + newNodes);
        }
        finally {
            stopExtraNodes(newNodes);
        }
    }
}
