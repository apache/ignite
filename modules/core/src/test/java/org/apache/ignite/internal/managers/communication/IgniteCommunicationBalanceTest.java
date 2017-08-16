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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

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
        commSpi.setConnectionsPerNode(connectionsPerNode());
        commSpi.setUsePairedConnections(usePairedConnections());

        if (selectors > 0)
            commSpi.setSelectorsCount(selectors);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Value for {@link TcpCommunicationSpi#setUsePairedConnections(boolean)}.
     */
    protected boolean usePairedConnections() {
        return false;
    }

    /**
     * @return Connections per node.
     */
    protected int connectionsPerNode() {
        return 1;
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
        System.setProperty(IgniteSystemProperties.IGNITE_IO_BALANCE_PERIOD, "5000");

        try {
            selectors = 4;

            final int SRVS = 6;

            startGridsMultiThreaded(SRVS);

            client = true;

            final Ignite client = startGrid(SRVS);

            for (int i = 0; i < SRVS; i++) {
                ClusterNode node = client.cluster().node(ignite(i).cluster().localNode().id());

                client.compute(client.cluster().forNode(node)).call(new DummyCallable(null));
            }

            waitNioBalanceStop(Collections.singletonList(client), 10_000);

            final GridNioServer srv = GridTestUtils.getFieldValue(client.configuration().getCommunicationSpi(), "nioSrvr");

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            long readMoveCnt1 = srv.readerMoveCount();
            long writeMoveCnt1 = srv.writerMoveCount();

            int prevNodeIdx = -1;

            for (int iter = 0; iter < 10; iter++) {
                int nodeIdx = rnd.nextInt(SRVS);

                while (prevNodeIdx == nodeIdx)
                    nodeIdx = rnd.nextInt(SRVS);

                prevNodeIdx = nodeIdx;

                log.info("Iteration [iter=" + iter + ", node=" + nodeIdx + ']');

                final long readMoveCnt = readMoveCnt1;
                final long writeMoveCnt = writeMoveCnt1;

                final int nodeIdx0 = nodeIdx;

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        byte[] data = new byte[100_000];

                        for (int j = 0; j < 10; j++) {
                            for (int i = 0; i < SRVS; i++) {
                                ClusterNode node = client.cluster().node(ignite(i).cluster().localNode().id());

                                IgniteCompute compute = client.compute(client.cluster().forNode(node));

                                compute.call(new DummyCallable(i == nodeIdx0 ? data : null));
                            }
                        }

                        if (usePairedConnections())
                            return srv.readerMoveCount() > readMoveCnt && srv.writerMoveCount() > writeMoveCnt;
                        else
                            return srv.readerMoveCount() > readMoveCnt || srv.writerMoveCount() > writeMoveCnt;
                    }
                }, 30_000);

                waitNioBalanceStop(Collections.singletonList(client), 30_000);

                long readMoveCnt2 = srv.readerMoveCount();
                long writeMoveCnt2 = srv.writerMoveCount();

                log.info("Move counts [rc1=" + readMoveCnt1 +
                    ", wc1=" + writeMoveCnt1 +
                    ", rc2=" + readMoveCnt2 +
                    ", wc2=" + writeMoveCnt2 + ']');

                if (usePairedConnections()) {
                    assertTrue(readMoveCnt2 > readMoveCnt1);
                    assertTrue(writeMoveCnt2 > writeMoveCnt1);
                }
                else
                    assertTrue(readMoveCnt2 > readMoveCnt1 || writeMoveCnt2 > writeMoveCnt1);

                readMoveCnt1 = readMoveCnt2;
                writeMoveCnt1 = writeMoveCnt2;
            }

            waitNioBalanceStop(G.allGrids(), 10_000);
        }
        finally {
            System.setProperty(IgniteSystemProperties.IGNITE_IO_BALANCE_PERIOD, "");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBalance2() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_IO_BALANCE_PERIOD, "1000");

        try {
            startGridsMultiThreaded(5);

            client = true;

            startGridsMultiThreaded(5, 5);

            for (int i = 0; i < 5; i++) {
                log.info("Iteration: " + i);

                final AtomicInteger idx = new AtomicInteger();

                GridTestUtils.runMultiThreaded(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Ignite node = ignite(idx.incrementAndGet() % 10);

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        int msgs = rnd.nextInt(500, 600);

                        for (int i = 0; i < msgs; i++) {
                            int sndTo = rnd.nextInt(10);

                            ClusterNode sntToNode = node.cluster().node(ignite(sndTo).cluster().localNode().id());

                            IgniteCompute compute = node.compute(node.cluster().forNode(sntToNode));

                            compute.call(new DummyCallable(new byte[rnd.nextInt(rnd.nextInt(256, 1024))]));
                        }

                        return null;
                    }
                }, 30, "test-thread");

                waitNioBalanceStop(G.allGrids(), 10_000);
            }
        }
        finally {
            System.setProperty(IgniteSystemProperties.IGNITE_IO_BALANCE_PERIOD, "");
        }
    }

    /**
     * @param nodes Node.
     * @param timeout Timeout.
     * @throws Exception If failed.
     */
    private void waitNioBalanceStop(List<Ignite> nodes, long timeout) throws Exception {
        final List<GridNioServer> srvs = new ArrayList<>();

        for (Ignite node : nodes) {
            TcpCommunicationSpi spi = (TcpCommunicationSpi) node.configuration().getCommunicationSpi();

            GridNioServer srv = GridTestUtils.getFieldValue(spi, "nioSrvr");

            srvs.add(srv);
        }

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                List<Long> rCnts = new ArrayList<>();
                List<Long> wCnts = new ArrayList<>();

                for (GridNioServer srv : srvs) {
                    long readerMovCnt1 = srv.readerMoveCount();
                    long writerMovCnt1 = srv.writerMoveCount();

                    rCnts.add(readerMovCnt1);
                    wCnts.add(writerMovCnt1);
                }

                U.sleep(2000);

                for (int i = 0; i < srvs.size(); i++) {
                    GridNioServer srv = srvs.get(i);

                    long readerMovCnt1 = rCnts.get(i);
                    long writerMovCnt1 = wCnts.get(i);

                    long readerMovCnt2 = srv.readerMoveCount();
                    long writerMovCnt2 = srv.writerMoveCount();

                    if (readerMovCnt1 != readerMovCnt2) {
                        log.info("Readers balance is in progress [node=" + i + ", cnt1=" + readerMovCnt1 +
                            ", cnt2=" + readerMovCnt2 + ']');

                        return false;
                    }
                    if (writerMovCnt1 != writerMovCnt2) {
                        log.info("Writers balance is in progress [node=" + i + ", cnt1=" + writerMovCnt1 +
                            ", cnt2=" + writerMovCnt2 + ']');

                        return false;
                    }
                }

                return true;
            }
        }, timeout));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomBalance() throws Exception {
        System.setProperty(GridNioServer.IGNITE_IO_BALANCE_RANDOM_BALANCE, "true");
        System.setProperty(IgniteSystemProperties.IGNITE_IO_BALANCE_PERIOD, "500");

        try {
            final int NODES = 10;

            startGridsMultiThreaded(NODES);

            final long stopTime = System.currentTimeMillis() + 60_000;

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (System.currentTimeMillis() < stopTime)
                        ignite(rnd.nextInt(NODES)).compute().broadcast(new DummyCallable(null));

                    return null;
                }
            }, 20, "test-thread");
        }
        finally {
            System.setProperty(GridNioServer.IGNITE_IO_BALANCE_RANDOM_BALANCE, "");
            System.setProperty(IgniteSystemProperties.IGNITE_IO_BALANCE_PERIOD, "");
        }
    }

    /**
     *
     */
    private static class DummyCallable implements IgniteCallable<Object> {
        /** */
        private byte[] data;

        /**
         * @param data Data.
         */
        DummyCallable(byte[] data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return data;
        }
    }
}
