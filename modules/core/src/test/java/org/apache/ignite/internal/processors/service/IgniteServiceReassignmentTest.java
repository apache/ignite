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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteServiceReassignmentTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private ServiceConfiguration srvcCfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (srvcCfg != null)
            cfg.setServiceConfiguration(srvcCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeRestart1() throws Exception {
        srvcCfg = serviceConfiguration();

        Ignite node1 = startGrid(1);

        assertEquals(42, serviceProxy(node1).foo());

        srvcCfg = serviceConfiguration();

        Ignite node2 = startGrid(2);

        node1.close();

        waitForService(node2);

        assertEquals(42, serviceProxy(node2).foo());

        srvcCfg = serviceConfiguration();

        Ignite node3 = startGrid(3);

        assertEquals(42, serviceProxy(node3).foo());

        srvcCfg = serviceConfiguration();

        node1 = startGrid(1);

        assertEquals(42, serviceProxy(node1).foo());
        assertEquals(42, serviceProxy(node2).foo());
        assertEquals(42, serviceProxy(node3).foo());

        node2.close();

        waitForService(node1);

        assertEquals(42, serviceProxy(node1).foo());
        assertEquals(42, serviceProxy(node3).foo());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeRestart2() throws Exception {
        startGrids(3);

        ServiceConfiguration svcCfg = new ServiceConfiguration();

        svcCfg.setName("DummyService");
        svcCfg.setTotalCount(10);
        svcCfg.setMaxPerNodeCount(1);
        svcCfg.setService(new DummyService());

        ignite(0).services().deploy(svcCfg);

        for (int i = 0; i < 3; i++)
            assertEquals(42, serviceProxy(ignite(i)).foo());

        for (int i = 0; i < 3; i++)
            startGrid(i + 3);

        for (int i = 0; i < 3; i++)
            stopGrid(i);

        for (int i = 0; i < 3; i++)
            assertEquals(42, serviceProxy(ignite(i + 3)).foo());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeRestartRandom() throws Exception {
        final int NODES = 5;

        Ignite ignite = startGridsMultiThreaded(NODES);

        ignite.services().deploy(serviceConfiguration());

        for (int i = 0; i < 30; i++) {
            log.info("Iteration: " + i);

            int stopIdx = ThreadLocalRandom.current().nextInt(NODES);

            stopGrid(stopIdx);

            for (int nodeIdx = 0; nodeIdx < NODES; nodeIdx++) {
                if (nodeIdx == stopIdx)
                    continue;

                waitForService(ignite(nodeIdx));

                assertEquals(42, serviceProxy(ignite(nodeIdx)).foo());
            }

            startGrid(stopIdx);

            for (int nodeIdx = 0; nodeIdx < NODES; nodeIdx++)
                assertEquals(42, serviceProxy(ignite(nodeIdx)).foo());
        }
    }

    /**
     * @param node Node.
     * @throws Exception If failed.
     */
    private void waitForService(final Ignite node) throws Exception {
        assertTrue(GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                try {
                    serviceProxy(node).foo();

                    return true;
                }
                catch (IgniteException ignored) {
                    return false;
                }
            }
        }, 5000));
    }

    /**
     * @param node Node.
     * @return Service proxy.
     */
    private static MyService serviceProxy(Ignite node) {
        return node.services().serviceProxy("DummyService", MyService.class, true);
    }

    /**
     * @return Service configuration.
     */
    private ServiceConfiguration serviceConfiguration() {
        ServiceConfiguration svc = new ServiceConfiguration();

        svc.setName("DummyService");
        svc.setTotalCount(1);
        svc.setService(new DummyService());

        return svc;
    }

    /**
     *
     */
    public interface MyService {
        /**
         * @return Dummy result.
         */
        int foo();
    }

    /**
     *
     */
    static class DummyService implements MyService, Service {
        /** */
        @IgniteInstanceResource
        private Ignite locNode;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            locNode.log().info("Service cancelled [execId=" + ctx.executionId() +
                ", node=" + locNode.cluster().localNode() + ']');
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) {
            locNode.log().info("Service initialized [execId=" + ctx.executionId() +
                ", node=" + locNode.cluster().localNode() + ']');
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) {
            locNode.log().info("Service started [execId=" + ctx.executionId() +
                ", node=" + locNode.cluster().localNode() + ']');
        }

        /** {@inheritDoc} */
        @Override public int foo() {
            locNode.log().info("Service called.");

            return 42;
        }
    }
}
