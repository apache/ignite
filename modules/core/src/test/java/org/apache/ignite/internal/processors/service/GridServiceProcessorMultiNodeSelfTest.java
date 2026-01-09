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

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
    @Test
    public void testSingletonUpdateTopology() throws Exception {
        String name = "serviceSingletonUpdateTopology";

        IgniteEx g = randomGrid();

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        IgniteServices svcs = g.services();

        IgniteFuture<?> fut = svcs.deployClusterSingletonAsync(name, new DummyService());

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(1, DummyService.started(name), name);
        assertEquals(0, DummyService.cancelled(name), name);

        int nodeCnt = 2;

        startExtraNodes(nodeCnt);

        try {
            assertEquals(1, DummyService.started(name), name);
            assertEquals(0, DummyService.cancelled(name), name);

            info(">>> Passed checks.");

            checkCount(name, g, 1);
        }
        finally {
            stopExtraNodes(nodeCnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityDeployUpdateTopology() throws Exception {
        IgniteEx g = randomGrid();

        final Integer affKey = 1;

        // Store a cache key.
        g.cache(CACHE_NAME).put(affKey, affKey.toString());

        final String name = "serviceAffinityUpdateTopology";

        IgniteServices svcs = g.services();

        IgniteFuture<?> fut = svcs.deployKeyAffinitySingletonAsync(name, new AffinityService(affKey),
            CACHE_NAME, affKey);

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        checkCount(name, g.services().serviceDescriptors(), 1);

        int nodeCnt = 2;

        startExtraNodes(nodeCnt);

        try {
            checkCount(name, g, 1);
        }
        finally {
            stopExtraNodes(nodeCnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployOnEachNodeButClientUpdateTopology() throws Exception {
        // Prestart client node.
        Ignite client = startClientGrid("client", getConfiguration("client"));

        try {
            final String name = "serviceOnEachNodeButClientUpdateTopology";

            IgniteEx g = randomGrid();

            CountDownLatch latch = new CountDownLatch(nodeCount());

            DummyService.exeLatch(name, latch);

            IgniteServices svcs = g.services();

            IgniteFuture<?> fut = svcs.deployNodeSingletonAsync(name, new DummyService());

            info("Deployed service: " + name);

            fut.get();

            info("Finished waiting for service future: " + name);

            latch.await();

            // Ensure service is deployed
            assertNotNull(client.services().serviceProxy(name, Service.class, false, 2000));

            assertEquals(nodeCount(), DummyService.started(name), name);
            assertEquals(0, DummyService.cancelled(name), name);

            int servers = 2;

            latch = new CountDownLatch(servers);

            DummyService.exeLatch(name, latch);

            int clients = 2;

            startExtraNodes(servers, clients);

            try {
                latch.await();

                waitForDeployment(name, servers);

                // Since we start extra nodes, there may be extra start and cancel events,
                // so we check only the difference between start and cancel and
                // not start and cancel events individually.
                assertEquals(nodeCount() + servers, DummyService.started(name) - DummyService.cancelled(name), name);

                checkCount(name, g, nodeCount() + servers);
            }
            finally {
                stopExtraNodes(servers + clients);
            }
        }
        finally {
            stopGrid("client");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployOnEachProjectionNodeUpdateTopology() throws Exception {
        // Prestart client node.
        Ignite client = startClientGrid("client", getConfiguration("client"));

        try {
            final String name = "serviceOnEachProjectionNodeUpdateTopology";

            IgniteEx g = randomGrid();

            int prestartedSrvcs = 1;

            CountDownLatch latch = new CountDownLatch(prestartedSrvcs);

            DummyService.exeLatch(name, latch);

            IgniteServices svcs = g.services(g.cluster().forClients());

            IgniteFuture<?> fut = svcs.deployNodeSingletonAsync(name, new DummyService());

            info("Deployed service: " + name);

            fut.get();

            info("Finished waiting for service future: " + name);

            latch.await();

            // Ensure service is deployed
            assertNotNull(client.services().serviceProxy(name, Service.class, false, 2000));

            assertEquals(prestartedSrvcs, DummyService.started(name), name);
            assertEquals(0, DummyService.cancelled(name), name);

            int servers = 2;

            int clients = 2;

            latch = new CountDownLatch(clients);

            DummyService.exeLatch(name, latch);

            startExtraNodes(servers, clients);

            try {
                latch.await();

                waitForDeployment(name, clients);

                // Since we start extra nodes, there may be extra start and cancel events,
                // so we check only the difference between start and cancel and
                // not start and cancel events individually.
                assertEquals(clients + prestartedSrvcs, DummyService.started(name) - DummyService.cancelled(name), name);

                checkCount(name, g, clients + prestartedSrvcs);
            }
            finally {
                stopExtraNodes(servers + clients);
            }
        }
        finally {
            stopGrid("client");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeployOnEachNodeUpdateTopology() throws Exception {
        // Prestart client node.
        Ignite client = startClientGrid("client", getConfiguration("client"));

        try {
            final String name = "serviceOnEachNodeUpdateTopology";

            IgniteEx g = randomGrid();

            final int prestartedNodes = nodeCount() + 1;

            CountDownLatch latch = new CountDownLatch(prestartedNodes);

            DummyService.exeLatch(name, latch);

            ServiceConfiguration srvcCfg = new ServiceConfiguration();

            srvcCfg.setNodeFilter(new CacheConfiguration.IgniteAllNodesPredicate());
            srvcCfg.setName(name);
            srvcCfg.setMaxPerNodeCount(1);
            srvcCfg.setService(new DummyService());

            IgniteServices svcs = g.services();

            IgniteFuture<?> fut = svcs.deployAsync(srvcCfg);

            info("Deployed service: " + name);

            fut.get();

            info("Finished waiting for service future: " + name);

            latch.await();

            // Ensure service is deployed
            assertNotNull(client.services().serviceProxy(name, Service.class, false, 2000));

            assertEquals(prestartedNodes, DummyService.started(name), name);
            assertEquals(0, DummyService.cancelled(name), name);

            int servers = 2;
            int clients = 2;

            int extraNodes = servers + clients;

            latch = new CountDownLatch(extraNodes);

            DummyService.exeLatch(name, latch);

            startExtraNodes(servers, clients);

            try {
                latch.await();

                waitForDeployment(name, prestartedNodes + extraNodes);

                // Since we start extra nodes, there may be extra start and cancel events,
                // so we check only the difference between start and cancel and
                // not start and cancel events individually.
                assertEquals(prestartedNodes + extraNodes,
                    DummyService.started(name) - DummyService.cancelled(name), name);

                checkCount(name, g, prestartedNodes + extraNodes);
            }
            finally {
                stopExtraNodes(extraNodes);
            }
        }
        finally {
            stopGrid("client");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testDeployLimits() throws Exception {
        final String name = "serviceWithLimitsUpdateTopology";

        IgniteEx g = randomGrid();

        final int totalInstances = nodeCount() + 1;

        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.exeLatch(name, latch);

        ServiceConfiguration srvcCfg = new ServiceConfiguration();

        srvcCfg.setName(name);
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setTotalCount(totalInstances);
        srvcCfg.setService(new DummyService());

        IgniteServices svcs = g.services().withAsync();

        svcs.deploy(srvcCfg);

        IgniteFuture<?> fut = svcs.future();

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        assertEquals(nodeCount(), DummyService.started(name), name);
        assertEquals(0, DummyService.cancelled(name), name);

        checkCount(name, g.services().serviceDescriptors(), nodeCount());

        latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        int extraNodes = 2;

        startExtraNodes(extraNodes);

        try {
            latch.await();

            waitForDeployment(name, totalInstances);

            // Since we start extra nodes, there may be extra start and cancel events,
            // so we check only the difference between start and cancel and
            // not start and cancel events individually.
            assertEquals(totalInstances, DummyService.started(name) - DummyService.cancelled(name), name);

            checkCount(name, g, totalInstances);
        }
        finally {
            stopExtraNodes(extraNodes);
        }
    }
}
