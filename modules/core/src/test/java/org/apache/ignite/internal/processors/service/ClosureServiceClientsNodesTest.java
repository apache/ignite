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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test that compute and service run only on server nodes by default.
 */
public class ClosureServiceClientsNodesTest extends GridCommonAbstractTest {
    /** Number of grids started for tests. */
    private static final int NODES_CNT = 4;

    /** Test singleton service name. */
    private static final String SINGLETON_NAME = "testSingleton";

    /** IP finder. */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new OptimizedMarshaller(false));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder).setForceServerMode(true));

        cfg.setCacheConfiguration();

        if (gridName.equals(getTestGridName(0)))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultClosure() throws Exception {
        Set<String> srvNames = new HashSet<>(NODES_CNT - 1);

        for (int i = 1; i < NODES_CNT; ++i)
            srvNames.add(getTestGridName(i));

        for (int i = 0 ; i < NODES_CNT; i++) {
            log.info("Iteration: " + i);

            Ignite ignite = grid(i);

            Collection<String> res = ignite.compute().broadcast(new IgniteCallable<String>() {
                @IgniteInstanceResource
                Ignite ignite;

                @Override public String call() throws Exception {
                    assertFalse(ignite.configuration().isClientMode());

                    return ignite.name();
                }
            });

            assertEquals(res.size(), NODES_CNT - 1);

            for (String name : res)
                assertTrue(srvNames.contains(name));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientClosure() throws Exception {
        for (int i = 0 ; i < NODES_CNT; i++) {
            log.info("Iteration: " + i);

            Ignite ignite = grid(i);

            Collection<String> res = ignite.compute(ignite.cluster().forClients()).
                broadcast(new IgniteCallable<String>() {
                    @IgniteInstanceResource
                    Ignite ignite;

                    @Override public String call() throws Exception {
                        assertTrue(ignite.configuration().isClientMode());

                        return ignite.name();
                    }
                });

            assertEquals(1, res.size());

            assertEquals(getTestGridName(0), F.first(res));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomClosure() throws Exception {
        for (int i = 0 ; i < NODES_CNT; i++) {
            log.info("Iteration: " + i);

            Ignite ignite = grid(i);

            Collection<String> res = ignite.compute(ignite.cluster().forPredicate(F.<ClusterNode>alwaysTrue())).
                broadcast(new IgniteCallable<String>() {
                    @IgniteInstanceResource
                    Ignite ignite;

                    @Override public String call() throws Exception {
                        return ignite.name();
                    }
                });

            assertEquals(NODES_CNT, res.size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultService() throws Exception {
        UUID clientNodeId = grid(0).cluster().localNode().id();

        for (int i = 0 ; i < NODES_CNT; i++) {
            log.info("Iteration: " + i);

            Ignite ignite = grid(i);

            ignite.services().deployNodeSingleton(SINGLETON_NAME, new TestService());

            ClusterGroup grp = ignite.cluster();

            assertEquals(NODES_CNT, grp.nodes().size());

            Collection<ServiceDescriptor> srvDscs = ignite.services(grp).serviceDescriptors();

            assertEquals(1, srvDscs.size());

            Map<UUID, Integer> nodesMap = F.first(srvDscs).topologySnapshot();

            assertEquals(NODES_CNT - 1, nodesMap.size());

            for (Map.Entry<UUID, Integer> nodeInfo : nodesMap.entrySet()) {
                assertFalse(clientNodeId.equals(nodeInfo.getKey()));

                assertEquals(1, nodeInfo.getValue().intValue());
            }

            ignite.services().cancelAll();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientService() throws Exception {
        UUID clientNodeId = grid(0).cluster().localNode().id();

        for (int i = 0 ; i < NODES_CNT; i++) {
            log.info("Iteration: " + i);

            Ignite ignite = grid(i);

            ignite.services(ignite.cluster().forClients()).deployNodeSingleton(SINGLETON_NAME, new TestService());

            ClusterGroup grp = ignite.cluster();

            assertEquals(NODES_CNT, grp.nodes().size());

            Collection<ServiceDescriptor> srvDscs = ignite.services(grp).serviceDescriptors();

            assertEquals(1, srvDscs.size());

            Map<UUID, Integer> nodesMap = F.first(srvDscs).topologySnapshot();

            assertEquals(1, nodesMap.size());

            for (Map.Entry<UUID, Integer> nodeInfo : nodesMap.entrySet()) {
                assertEquals(clientNodeId, nodeInfo.getKey());

                assertEquals(1, nodeInfo.getValue().intValue());
            }

            ignite.services().cancelAll();
        }
    }

    /**
     * Test service.
     */
    private static class TestService implements Service {
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
          //No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            //No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            log.info("Executing test service.");
        }
    }
}