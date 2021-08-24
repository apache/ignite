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
package org.apache.ignite.network.scalecube;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.LocalPortRangeNodeFinder;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.TestMessageSerializationRegistryImpl;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests if a topology size is correct after some nodes are restarted in quick succession.
 */
class ITNodeRestartsTest {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ITNodeRestartsTest.class);

    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry = new TestMessageSerializationRegistryImpl();

    /** Network factory. */
    private final ClusterServiceFactory networkFactory = new TestScaleCubeClusterServiceFactory();

    /** Created {@link ClusterService}s. Needed for resource management. */
    private List<ClusterService> services;

    /** Tear down method. */
    @AfterEach
    void tearDown() {
        for (ClusterService service : services)
            service.stop();
    }

    /**
     * Tests that restarting nodes get discovered in an established topology.
     */
    @Test
    public void testRestarts() {
        final int initPort = 3344;

        var nodeFinder = new LocalPortRangeNodeFinder(initPort, initPort + 5);

        services = nodeFinder.findNodes().stream()
            .map(addr -> startNetwork(addr, nodeFinder))
            .collect(Collectors.toCollection(ArrayList::new)); // ensure mutability

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, 5, 5_000), service.topologyService().localMember().toString()
                + ", topSize=" + service.topologyService().allMembers().size());
        }

        int idx0 = 0;
        int idx1 = 2;

        List<NetworkAddress> addresses = nodeFinder.findNodes();

        LOG.info("Shutdown {}", addresses.get(idx0));
        services.get(idx0).stop();

        LOG.info("Shutdown {}", addresses.get(idx1));
        services.get(idx1).stop();

        LOG.info("Starting {}", addresses.get(idx0));
        ClusterService svc0 = startNetwork(addresses.get(idx0), nodeFinder);
        services.set(idx0, svc0);

        LOG.info("Starting {}", addresses.get(idx1));
        ClusterService svc2 = startNetwork(addresses.get(idx1), nodeFinder);
        services.set(idx1, svc2);

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, 5, 10_000), service.topologyService().localMember().toString()
                + ", topSize=" + service.topologyService().allMembers().size());
        }

        LOG.info("Reached stable state");
    }

    /**
     * Creates a {@link ClusterService} using the given local address and the node finder.
     *
     * @param addr Node address.
     * @param nodeFinder Node finder.
     * @return Created Cluster Service.
     */
    private ClusterService startNetwork(NetworkAddress addr, NodeFinder nodeFinder) {
        ClusterService clusterService = ClusterServiceTestUtils.clusterService(
            addr.toString(),
            addr.port(),
            nodeFinder,
            serializationRegistry,
            networkFactory
        );

        clusterService.start();

        return clusterService;
    }

    /**
     * Blocks until the given topology reaches {@code expected} amount of members.
     *
     * @param service  The service.
     * @param expected Expected count.
     * @param timeout  The timeout.
     * @return Wait status.
     */
    @SuppressWarnings("BusyWait")
    private static boolean waitForTopology(ClusterService service, int expected, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (service.topologyService().allMembers().size() == expected)
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }
}
