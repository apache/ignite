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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Single node services test.
 */
public class GridServiceProcessorMultiNodeConfigSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** Cluster singleton name. */
    private static final String CLUSTER_SINGLE = "serviceConfigSingleton";

    /** Node singleton name. */
    private static final String NODE_SINGLE = "serviceConfigEachNode";

    /** Node singleton name. */
    private static final String NODE_SINGLE_BUT_CLIENT = "serviceConfigEachNodeButClient";

    /** Node singleton name. */
    private static final String NODE_SINGLE_WITH_LIMIT = "serviceConfigWithLimit";

    /** Affinity service name. */
    private static final String AFFINITY = "serviceConfigAffinity";

    /** Affinity key. */
    private static final Integer AFFINITY_KEY = 1;

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected ServiceConfiguration[] services() {
        List<ServiceConfiguration> cfgs = new ArrayList<>();

        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(CLUSTER_SINGLE);
        cfg.setMaxPerNodeCount(1);
        cfg.setTotalCount(1);
        cfg.setService(new DummyService());

        cfgs.add(cfg);

        cfg = new ServiceConfiguration();

        cfg.setName(NODE_SINGLE_BUT_CLIENT);
        cfg.setMaxPerNodeCount(1);
        cfg.setService(new DummyService());

        cfgs.add(cfg);

        cfg = new ServiceConfiguration();

        cfg.setName(AFFINITY);
        cfg.setCacheName(CACHE_NAME);
        cfg.setAffinityKey(AFFINITY_KEY);
        cfg.setMaxPerNodeCount(1);
        cfg.setTotalCount(1);
        cfg.setService(new AffinityService(AFFINITY_KEY));

        cfgs.add(cfg);

        cfg = new ServiceConfiguration();

        cfg.setName(NODE_SINGLE);
        cfg.setMaxPerNodeCount(1);
        cfg.setNodeFilter(new CacheConfiguration.IgniteAllNodesPredicate());
        cfg.setService(new DummyService());

        cfgs.add(cfg);

        cfg = new ServiceConfiguration();

        cfg.setName(NODE_SINGLE_WITH_LIMIT);
        cfg.setMaxPerNodeCount(1);
        cfg.setTotalCount(nodeCount() + 1);
        cfg.setService(new DummyService());

        cfgs.add(cfg);

        return cfgs.toArray(new ServiceConfiguration[cfgs.size()]);
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
                        DummyService.started(NODE_SINGLE_BUT_CLIENT) == nodeCount() &&
                        DummyService.cancelled(NODE_SINGLE_BUT_CLIENT) == 0 &&
                        DummyService.started(NODE_SINGLE_WITH_LIMIT) >= nodeCount() &&
                        DummyService.cancelled(NODE_SINGLE_WITH_LIMIT) == 0 &&
                        actualCount(AFFINITY, randomGrid().services().serviceDescriptors()) == 1;
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
    public void testDeployOnEachNodeButClientUpdateTopology() throws Exception {
        checkDeployOnEachNodeButClientUpdateTopology(NODE_SINGLE_BUT_CLIENT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAll() throws Exception {
        checkSingletonUpdateTopology(CLUSTER_SINGLE);

        DummyService.reset();

        checkDeployOnEachNodeButClientUpdateTopology(NODE_SINGLE_BUT_CLIENT);

        DummyService.reset();

        checkDeployOnEachNodeUpdateTopology(NODE_SINGLE);

        DummyService.reset();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityUpdateTopology() throws Exception {
        Ignite g = randomGrid();

        checkCount(AFFINITY, g.services().serviceDescriptors(), 1);

        int nodeCnt = 2;

        startExtraNodes(nodeCnt);

        try {
            checkCount(AFFINITY, g.services().serviceDescriptors(), 1);
        }
        finally {
            stopExtraNodes(nodeCnt);
        }

        checkCount(AFFINITY, g.services().serviceDescriptors(), 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployLimits() throws Exception {
        final Ignite g = randomGrid();

        final String name = NODE_SINGLE_WITH_LIMIT;

        waitForDeployment(name, nodeCount());

        checkCount(name, g.services().serviceDescriptors(), nodeCount());

        int extraNodes = 2;

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        startExtraNodes(extraNodes);

        try {
            latch.await();

            checkCount(name, g.services().serviceDescriptors(), nodeCount() + 1);
        }
        finally {
            stopExtraNodes(extraNodes);
        }

        assertEquals(name, 1, DummyService.cancelled(name));

        waitForDeployment(name, nodeCount());

        checkCount(name, g.services().serviceDescriptors(), nodeCount());
    }

    /**
     * @param srvcName Service name
     * @param expectedDeps Expected number of service deployments
     *
     */
    private boolean waitForDeployment(final String srvcName, final int expectedDeps) throws IgniteInterruptedCheckedException {
        final Ignite g = randomGrid();

        return GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() {
                return actualCount(srvcName, g.services().serviceDescriptors())  == expectedDeps;
            }
        }, 1500);
    }

    /**
     * @param name Name.
     * @throws Exception If failed.
     */
    private void checkSingletonUpdateTopology(String name) throws Exception {
        Ignite g = randomGrid();

        startExtraNodes(2, 2);

        try {
            assertEquals(name, 0, DummyService.started(name));
            assertEquals(name, 0, DummyService.cancelled(name));

            info(">>> Passed checks.");

            checkCount(name, g.services().serviceDescriptors(), 1);
        }
        finally {
            stopExtraNodes(4);
        }
    }

    /**
     * @param name Name.
     * @throws Exception If failed.
     */
    private void checkDeployOnEachNodeUpdateTopology(String name) throws Exception {
        Ignite g = randomGrid();

        int newNodes = 4;

        CountDownLatch latch = new CountDownLatch(newNodes);

        DummyService.exeLatch(name, latch);

        startExtraNodes(2, 2);

        try {
            latch.await();

            assertEquals(name, newNodes, DummyService.started(name));
            assertEquals(name, 0, DummyService.cancelled(name));

            checkCount(name, g.services().serviceDescriptors(), nodeCount() + newNodes);
        }
        finally {
            stopExtraNodes(newNodes);
        }

        waitForDeployment(name, nodeCount());

        checkCount(name, g.services().serviceDescriptors(), nodeCount());
    }

    /**
     * @param name Name.
     * @throws Exception If failed.
     */
    private void checkDeployOnEachNodeButClientUpdateTopology(String name) throws Exception {
        Ignite g = randomGrid();

        int servers = 2;
        int clients = 2;

        CountDownLatch latch = new CountDownLatch(servers);

        DummyService.exeLatch(name, latch);

        startExtraNodes(servers, clients);

        try {
            latch.await();

            assertEquals(name, servers, DummyService.started(name));
            assertEquals(name, 0, DummyService.cancelled(name));

            checkCount(name, g.services().serviceDescriptors(), nodeCount() + servers);
        }
        finally {
            stopExtraNodes(servers + clients);
        }

        waitForDeployment(name, nodeCount());

        checkCount(name, g.services().serviceDescriptors(), nodeCount());
    }
}