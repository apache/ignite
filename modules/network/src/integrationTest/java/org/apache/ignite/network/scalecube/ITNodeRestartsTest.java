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
import java.util.stream.IntStream;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests if a topology size is correct after some nodes are restarted in quick succession.
 */
class ITNodeRestartsTest {
    /** */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ITNodeRestartsTest.class);

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistry();

    /** */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** */
    private List<ClusterService> services;

    /** */
    @AfterEach
    void after() {
        for (ClusterService service : services)
            service.shutdown();
    }

    /** */
    @Test
    public void testRestarts() throws InterruptedException {
        final int initPort = 3344;

        String addr = "localhost";
        List<String> addresses = IntStream.range(0, 5).mapToObj(i -> addr + ":" + (initPort + i)).collect(toList());

        services = new ArrayList<>(addresses.size());

        for (int i = 0; i < addresses.size(); i++) {
            String address = addresses.get(i);

            ClusterService svc = startNetwork(address, initPort + i, addresses);

            services.add(svc);
        }

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, 5, 5_000), service.topologyService().localMember().toString()
                + ", topSize=" + service.topologyService().allMembers().size());
        }

        int idx0 = 0;
        int idx1 = 2;

        LOG.info("Shutdown " + addresses.get(idx0));
        services.get(idx0).shutdown();

        LOG.info("Shutdown " + addresses.get(idx1));
        services.get(idx1).shutdown();

        LOG.info("Starting " + addresses.get(idx0));
        ClusterService svc0 = startNetwork(addresses.get(idx0), initPort + idx0, addresses);
        services.set(idx0, svc0);

        LOG.info("Starting " + addresses.get(idx1));
        ClusterService svc2 = startNetwork(addresses.get(idx1), initPort + idx1, addresses);
        services.set(idx1, svc2);

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, 5, 10_000), service.topologyService().localMember().toString()
                + ", topSize=" + service.topologyService().allMembers().size());
        }

        LOG.info("Reached stable state");
    }

    /** */
    private ClusterService startNetwork(String name, int port, List<String> addresses) {
        var context = new ClusterLocalConfiguration(name, port, addresses, SERIALIZATION_REGISTRY);

        ClusterService clusterService = NETWORK_FACTORY.createClusterService(context);

        clusterService.start();

        return clusterService;
    }

    /**
     * @param service  The service.
     * @param expected Expected count.
     * @param timeout  The timeout.
     * @return Wait status.
     */
    @SuppressWarnings("BusyWait")
    protected boolean waitForTopology(ClusterService service, int expected, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < stop) {
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
