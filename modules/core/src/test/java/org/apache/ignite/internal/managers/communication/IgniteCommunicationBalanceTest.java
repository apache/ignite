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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
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
        commSpi.setConnectionsPerNode(1);

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
    public void testBalance1() throws Exception {
        System.setProperty(GridNioServer.IGNITE_NIO_SES_BALANCER_BALANCE_PERIOD, "500");

        try {
            selectors = 4;

            startGridsMultiThreaded(4);

            client = true;

            Ignite client = startGrid(4);

            for (int i = 0; i < 4; i++) {
                ClusterNode node = client.cluster().node(ignite(i).cluster().localNode().id());

                client.compute(client.cluster().forNode(node)).run(new DummyRunnable(null));
            }

            waitNioBalanceStop(client, 30_000);

            final GridNioServer srv = GridTestUtils.getFieldValue(client.configuration().getCommunicationSpi(), "nioSrvr");

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            long readMoveCnt1 = srv.readerMoveCount();
            long writeMoveCnt1 = srv.writerMoveCount();

            for (int iter = 0; iter < 10; iter++) {
                log.info("Iteration: " + iter);

                int nodeIdx = rnd.nextInt(4);

                ClusterNode node = client.cluster().node(ignite(nodeIdx).cluster().localNode().id());

                IgniteCompute compute = client.compute(client.cluster().forNode(node));

                for (int i = 0; i < 10_000; i++)
                    compute.run(new DummyRunnable(null));

                final long readMoveCnt = readMoveCnt1;
                final long writeMoveCnt = writeMoveCnt1;

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return srv.readerMoveCount() > readMoveCnt && srv.writerMoveCount() > writeMoveCnt;
                    }
                }, 10_000);

                waitNioBalanceStop(client, 30_000);

                long readMoveCnt2 = srv.readerMoveCount();
                long writeMoveCnt2 = srv.writerMoveCount();

                assertTrue(readMoveCnt2 > readMoveCnt1);
                assertTrue(writeMoveCnt2 > writeMoveCnt1);

                readMoveCnt1 = readMoveCnt2;
                writeMoveCnt1 = writeMoveCnt2;
            }

            for (Ignite node : G.allGrids())
                waitNioBalanceStop(node, 10_000);
        }
        finally {
            System.setProperty(GridNioServer.IGNITE_NIO_SES_BALANCER_BALANCE_PERIOD, "");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBalance2() throws Exception {
        System.setProperty(GridNioServer.IGNITE_NIO_SES_BALANCER_BALANCE_PERIOD, "500");

        try {
            startGridsMultiThreaded(5);

            client = true;

            startGridsMultiThreaded(5, 5);

            for (int i = 0; i < 20; i++) {
                log.info("Iteration: " + i);

                final AtomicInteger idx = new AtomicInteger();

                GridTestUtils.runMultiThreaded(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Ignite node = ignite(idx.incrementAndGet() % 10);

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        int msgs = rnd.nextInt(1000);

                        for (int i = 0; i < msgs; i++) {
                            int sndTo = rnd.nextInt(10);

                            ClusterNode sntToNode = node.cluster().node(ignite(sndTo).cluster().localNode().id());

                            IgniteCompute compute = node.compute(node.cluster().forNode(sntToNode));

                            compute.run(new DummyRunnable(new byte[rnd.nextInt(1024)]));
                        }

                        return null;
                    }
                }, 30, "test-thread");

                for (Ignite node : G.allGrids())
                    waitNioBalanceStop(node, 10_000);
            }
        }
        finally {
            System.setProperty(GridNioServer.IGNITE_NIO_SES_BALANCER_BALANCE_PERIOD, "");
        }
    }

    /**
     * @param node Node.
     * @param timeout Timeout.
     * @throws Exception If failed.
     */
    private void waitNioBalanceStop(Ignite node, long timeout) throws Exception {
        TcpCommunicationSpi spi = (TcpCommunicationSpi)node.configuration().getCommunicationSpi();

        final GridNioServer srv = GridTestUtils.getFieldValue(spi, "nioSrvr");

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                long readerMovCnt1 = srv.readerMoveCount();
                long writerMovCnt1 = srv.writerMoveCount();

                U.sleep(2000);

                long readerMovCnt2 = srv.readerMoveCount();
                long writerMovCnt2 = srv.writerMoveCount();

                if (readerMovCnt1 != readerMovCnt2) {
                    log.info("Readers balance is in progress [cnt1=" + readerMovCnt1 + ", cnt2=" + readerMovCnt2 + ']');

                    return false;
                }
                if (writerMovCnt1 != writerMovCnt2) {
                    log.info("Writers balance is in progress [cnt1=" + writerMovCnt1 + ", cnt2=" + writerMovCnt2 + ']');

                    return false;
                }

                return true;
            }
        }, timeout));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomBalance() throws Exception {
        System.setProperty(GridNioServer.IGNITE_NIO_SES_BALANCER_CLASS_NAME, TestBalancer.class.getName());

        try {
            final int NODES = 10;

            startGridsMultiThreaded(NODES);

            final long stopTime = System.currentTimeMillis() + 60_000;

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (System.currentTimeMillis() < stopTime)
                        ignite(rnd.nextInt(NODES)).compute().broadcast(new DummyRunnable(null));

                    return null;
                }
            }, 20, "test-thread");
        }
        finally {
            System.setProperty(GridNioServer.IGNITE_NIO_SES_BALANCER_CLASS_NAME, null);
        }
    }

    /**
     *
     */
    private static class DummyRunnable implements IgniteRunnable {
        /** */
        private byte[] data;

        /**
         * @param data Data.
         */
        public DummyRunnable(byte[] data) {
            this.data = data;
        }

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
