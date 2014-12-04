/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.apache.ignite.*;
import org.apache.ignite.managed.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;

import java.util.concurrent.*;

/**
 * Single node services test.
 */
public class GridServiceProcessorMultiNodeConfigSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** Cluster singleton name. */
    private static final String CLUSTER_SINGLE = "serviceConfigSingleton";

    /** Node singleton name. */
    private static final String NODE_SINGLE = "serviceConfigEachNode";

    /** Affinity service name. */
    private static final String AFFINITY = "serviceConfigAffinity";

    /** Affinity key. */
    private static final Integer AFFINITY_KEY = 1;

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected GridServiceConfiguration[] services() {
        GridServiceConfiguration[] arr = new GridServiceConfiguration[3];

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

        cfg = new GridServiceConfiguration();

        cfg.setName(AFFINITY);
        cfg.setCacheName(CACHE_NAME);
        cfg.setAffinityKey(AFFINITY_KEY);
        cfg.setMaxPerNodeCount(1);
        cfg.setTotalCount(1);
        cfg.setService(new AffinityService(AFFINITY_KEY));

        arr[2] = cfg;

        return arr;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        GridTestUtils.waitForCondition(
            new GridAbsPredicateX() {
                @Override public boolean applyx() {
                    return
                        DummyService.started(CLUSTER_SINGLE) == 1 &&
                        DummyService.cancelled(CLUSTER_SINGLE) == 0 &&
                        DummyService.started(NODE_SINGLE) == nodeCount() &&
                        DummyService.cancelled(NODE_SINGLE) == 0 &&
                        actualCount(AFFINITY, randomGrid().managed().deployedServices()) == 1;
                }
            },
            2000
        );
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
     * @throws Exception If failed.
     */
    public void testAffinityUpdateTopology() throws Exception {
        Ignite g = randomGrid();

        checkCount(AFFINITY, g.managed().deployedServices(), 1);

        int nodeCnt = 2;

        startExtraNodes(nodeCnt);

        try {
            checkCount(AFFINITY, g.managed().deployedServices(), 1);
        }
        finally {
            stopExtraNodes(nodeCnt);
        }
    }

    /**
     * @param name Name.
     * @throws Exception If failed.
     */
    private void checkSingletonUpdateTopology(String name) throws Exception {
        Ignite g = randomGrid();

        int nodeCnt = 2;

        startExtraNodes(nodeCnt);

        try {
            assertEquals(name, 0, DummyService.started(name));
            assertEquals(name, 0, DummyService.cancelled(name));

            info(">>> Passed checks.");

            checkCount(name, g.managed().deployedServices(), 1);
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
        Ignite g = randomGrid();

        int newNodes = 2;

        CountDownLatch latch = new CountDownLatch(newNodes);

        DummyService.exeLatch(name, latch);

        startExtraNodes(newNodes);

        try {
            latch.await();

            assertEquals(name, newNodes, DummyService.started(name));
            assertEquals(name, 0, DummyService.cancelled(name));

            checkCount(name, g.managed().deployedServices(), nodeCount() + newNodes);
        }
        finally {
            stopExtraNodes(newNodes);
        }
    }
}
