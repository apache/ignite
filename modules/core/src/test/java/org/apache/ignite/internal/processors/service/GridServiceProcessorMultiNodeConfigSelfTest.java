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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.managed.*;
import org.apache.ignite.testframework.*;

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
    @Override protected ManagedServiceConfiguration[] services() {
        ManagedServiceConfiguration[] arr = new ManagedServiceConfiguration[3];

        ManagedServiceConfiguration cfg = new ManagedServiceConfiguration();

        cfg.setName(CLUSTER_SINGLE);
        cfg.setMaxPerNodeCount(1);
        cfg.setTotalCount(1);
        cfg.setService(new DummyService());

        arr[0] = cfg;

        cfg = new ManagedServiceConfiguration();

        cfg.setName(NODE_SINGLE);
        cfg.setMaxPerNodeCount(1);
        cfg.setService(new DummyService());

        arr[1] = cfg;

        cfg = new ManagedServiceConfiguration();

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
