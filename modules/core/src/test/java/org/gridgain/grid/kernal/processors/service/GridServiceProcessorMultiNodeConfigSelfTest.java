// @java.file.header

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
 *
 * @author @java.author
 * @version @java.version
 */
public class GridServiceProcessorMultiNodeConfigSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** Cluster singleton name. */
    private static final String CLUSTER_SINGLE = "serviceConfigSingleton";

    /** Node singleton name. */
    private static final String NODE_SINGLE = "serviceConfigEachNode";

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected GridServiceConfiguration[] services() {
        GridServiceConfiguration[] arr = new GridServiceConfiguration[2];

        GridServiceConfiguration cfg = new GridServiceConfiguration();

        cfg.setName(CLUSTER_SINGLE);
        cfg.setMaxPerNodeCount(1);
        cfg.setTotalCount(1);
        cfg.setService(new DummyService());

        arr[0] = cfg;

        cfg = new GridServiceConfiguration();

        cfg.setName(NODE_SINGLE);
        cfg.setMaxPerNodeCount(1);
        cfg.setService(new DummyService());

        arr[1] = cfg;

        return arr;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        assertEquals(CLUSTER_SINGLE, 1, DummyService.started(CLUSTER_SINGLE));
        assertEquals(CLUSTER_SINGLE, 0, DummyService.cancelled(CLUSTER_SINGLE));

        assertEquals(NODE_SINGLE, nodeCount(), DummyService.started(NODE_SINGLE));
        assertEquals(CLUSTER_SINGLE, 0, DummyService.cancelled(NODE_SINGLE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingletonUpdateTopology() throws Exception {
        checkSingletonUpdateTopology(CLUSTER_SINGLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOnEachNodeUpdateTopology() throws Exception {
        checkDeployOnEachNodeUpdateTopology(NODE_SINGLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAll() throws Exception {
        checkSingletonUpdateTopology(CLUSTER_SINGLE);

        DummyService.reset();

        checkDeployOnEachNodeUpdateTopology(NODE_SINGLE);

        DummyService.reset();
    }

    /**
     * @param name Name.
     * @throws Exception If failed.
     */
    private void checkSingletonUpdateTopology(String name) throws Exception {
        Grid g = randomGrid();

        int nodeCnt = 2;

        startExtraNodes(nodeCnt);

        try {
            assertEquals(name, 0, DummyService.started(name));
            assertEquals(name, 0, DummyService.cancelled(name));

            info(">>> Passed checks.");

            checkCount(name, g.services().deployedServices(), 1);
        }
        finally {
            stopExtraNodes(nodeCnt);
        }
    }

    /**
     * @param name Name.
     * @throws Exception If failed.
     */
    private void checkDeployOnEachNodeUpdateTopology(String name) throws Exception {
        Grid g = randomGrid();

        int newNodes = 2;

        CountDownLatch latch = new CountDownLatch(newNodes);

        DummyService.exeLatch(name, latch);

        startExtraNodes(newNodes);

        try {
            latch.await();

            assertEquals(name, newNodes, DummyService.started(name));
            assertEquals(name, 0, DummyService.cancelled(name));

            checkCount(name, g.services().deployedServices(), nodeCount() + newNodes);
        }
        finally {
            stopExtraNodes(newNodes);
        }
    }
}
