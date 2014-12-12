/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.apache.ignite.*;
import org.gridgain.grid.*;

/**
 * Single node services test.
 */
public class GridServiceProcessorSingleNodeSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }


    /**
     * @throws Exception If failed.
     */
    public void testNodeSingletonNotDeployedProxy() throws Exception {
        String name = "testNodeSingletonNotDeployedProxy";

        Ignite ignite = randomGrid();

        // Deploy only on remote nodes.
        ignite.managed(ignite.cluster().forRemotes()).deployNodeSingleton(name, new CounterServiceImpl());

        info("Deployed service: " + name);

        // Get local proxy.
        CounterService svc = ignite.managed().serviceProxy(name, CounterService.class, false);

        try {
            svc.increment();

            fail("Should never reach here.");
        }
        catch (IgniteException e) {
            info("Got expected exception: " + e.getMessage());
        }
    }
}
