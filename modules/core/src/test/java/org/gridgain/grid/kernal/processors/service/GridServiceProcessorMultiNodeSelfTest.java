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
        return 4;
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

        int newNodes = 2;

        for (int i = 0; i < newNodes; i++)
            startGrid(nodeCount() + i);

        assertEquals(1, DummyService.started());
        assertFalse(DummyService.isCancelled());

        checkCount(name, g.services().deployedServices(), 1);
                                              }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOnEachNode() throws Exception {
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

        int newNodes = 2;

        latch = new CountDownLatch(newNodes);

        DummyService.latch(latch);

        for (int i = 0; i < newNodes; i++)
            startGrid(nodeCount() + i);

        latch.await();

        assertEquals(nodeCount() + newNodes, DummyService.started());
        assertFalse(DummyService.isCancelled());

        checkCount(name, g.services().deployedServices(), nodeCount() + newNodes);
    }
}
