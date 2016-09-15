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

package org.apache.ignite.internal.managers.communication;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.log4j.helpers.ThreadLocalMap;

/**
 *
 */
public class IgniteCommunicationBalanceTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpCommunicationSpi commSpi = ((TcpCommunicationSpi)cfg.getCommunicationSpi());

        commSpi.setSharedMemoryPort(-1);
        commSpi.setSelectorsCount(4);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

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
    public void testBalance() throws Exception {
        startGrid(0);

        client = true;

        Ignite client = startGrid(4);

        startGridsMultiThreaded(1, 3);

        for (int i = 0; i < 4; i++) {
            ClusterNode node = client.cluster().node(ignite(i).cluster().localNode().id());

            client.compute(client.cluster().forNode(node)).run(new DummyRunnable());
        }

//        ThreadLocalRandom rnd = ThreadLocalRandom.current();
//
//        for (int iter = 0; iter < 10; iter++) {
//            log.info("Iteration: " + iter);
//
//            int nodeIdx = rnd.nextInt(4);
//
//            ClusterNode node = client.cluster().node(ignite(nodeIdx).cluster().localNode().id());
//
//            for (int i = 0; i < 10_000; i++)
//                client.compute(client.cluster().forNode(node)).run(new DummyRunnable());
//
//            U.sleep(5000);
//        }

        while (true) {
            ((IgniteKernal) client).dumpDebugInfo();

            Thread.sleep(5000);
        }

        //Thread.sleep(Long.MAX_VALUE);
    }

    /**
     *
     */
    static class DummyRunnable implements IgniteRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            // No-op.
        }
    }
}
