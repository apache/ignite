// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.*;

/**
 * Single node services test.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridServiceProcessorSingleNodeSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOnEachNode() throws Exception {
        Grid g = grid(0);

        String name = "serviceOnEachNode";

        GridFuture<?> fut = g.services().deployOnEachNode(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        assertTrue(DummyService.isStarted());
        assertFalse(DummyService.isCancelled());
    }
}
