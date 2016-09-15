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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.util.nio.GridNioServer.Balancer;

/**
 *
 */
public class IgniteCommunicationBalanceTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private int selectors;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpCommunicationSpi commSpi = ((TcpCommunicationSpi)cfg.getCommunicationSpi());

        commSpi.setSharedMemoryPort(-1);

        if (selectors > 0)
            commSpi.setSelectorsCount(selectors);

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
        selectors = 4;

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
     * @throws Exception If failed.
     */
    public void testRandomBalance() throws Exception {
        System.setProperty(GridNioServer.IGNITE_NIO_SES_BALANCER_CLASS_NAME, TestBalancer.class.getName());

        final int NODES = 10;

        startGridsMultiThreaded(NODES);

        final long stopTime = System.currentTimeMillis() + 60_000;

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (System.currentTimeMillis() < stopTime)
                    ignite(rnd.nextInt(NODES)).compute().broadcast(new DummyRunnable());

                return null;
            }
        }, 20, "test-thread");
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

    /**
     *
     */
    @SuppressWarnings("unchecked")
    public static class TestBalancer implements Balancer {
        /** */
        private final GridNioServer srv;

        /**
         * @param srv Server.
         */
        public TestBalancer(GridNioServer srv) {
            this.srv = srv;
        }

        /** {@inheritDoc} */
        @Override public void balance() {
            List<GridNioServer.AbstractNioClientWorker> clientWorkers = srv.workers();

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            int w1 = rnd.nextInt(clientWorkers.size());

            if (clientWorkers.get(w1).sessions().isEmpty())
                return;

            int w2 = rnd.nextInt(clientWorkers.size());

            while (w2 == w1)
                w2 = rnd.nextInt(clientWorkers.size());

            if (clientWorkers.get(w2).sessions().isEmpty())
                return;

            GridNioSession ses = randomSession(clientWorkers.get(w1));

            if (ses != null) {
                System.out.println("[" + Thread.currentThread().getName() + "] Move session " +
                    "[w1=" + w1 + ", w2=" + w2 + ", ses=" + ses + ']');

                srv.moveSession(ses, w1, w2);
            }
        }

        /**
         * @param worker Worker.
         * @return NIO session.
         */
        private GridNioSession randomSession(GridNioServer.AbstractNioClientWorker worker) {
            Collection<GridNioSession> sessions = worker.sessions();

            int size = sessions.size();

            if (size == 0)
                return null;

            int idx = ThreadLocalRandom.current().nextInt(size);

            Iterator<GridNioSession> it = sessions.iterator();

            int cnt = 0;

            while (it.hasNext()) {
                GridNioSession ses = it.next();

                if (cnt == idx)
                    return ses;
            }

            return null;
        }

    }
}
