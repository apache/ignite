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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.clock.GridClockDeltaSnapshotMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class GridCacheReplicatedInvalidateSelfTest extends GridCommonAbstractTest {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** */
    private static final Integer KEY = 1;

    /** */
    private static final String VAL = "test";

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * Don't start grid by default.
     */
    public GridCacheReplicatedInvalidateSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setCommunicationSpi(new TestCommunicationSpi());

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setRebalanceMode(NONE);
        cc.setCacheMode(REPLICATED);
        cc.setWriteSynchronizationMode(FULL_SYNC);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTest() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-601");

        for (int i = 0; i < GRID_CNT; i++)
            ioSpi(i).clearCounts();
    }

    /**
     * @param i Index.
     * @return IO SPI.
     */
    private TestCommunicationSpi ioSpi(int i) {
        return (TestCommunicationSpi)grid(i).configuration().getCommunicationSpi();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticReadCommitted() throws Throwable {
        checkCommit(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticRepeatableRead() throws Throwable {
        checkCommit(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticSerializable() throws Throwable {
        checkCommit(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Throwable If check failed.
     */
    private void checkCommit(TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Throwable {
        int idx = RAND.nextInt(GRID_CNT);

        IgniteCache<Integer, String> cache = jcache(idx);

        Transaction tx = grid(idx).transactions().txStart(concurrency, isolation, 0, 0);

        try {
            cache.put(KEY, VAL);

            tx.commit();
        }
        catch (Throwable e) {
            error("Transaction failed (will rollback): " + tx, e);

            tx.rollback();

            throw e;
        }

        TestCommunicationSpi ioSpi = ioSpi(idx);

        int checkIdx = RAND.nextInt(GRID_CNT);

        while (checkIdx == idx)
            checkIdx = RAND.nextInt(GRID_CNT);

        Ignite checkIgnite = grid(checkIdx);

        int msgCnt = ioSpi.getMessagesCount(checkIgnite.cluster().localNode().id());

        info("Checked node: " + checkIgnite.cluster().localNode().id());

        assertEquals("Invalid message count for grid: " + checkIgnite.cluster().localNode().id(), 2, msgCnt);
    }

    /**
     *
     */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private final Map<UUID, Integer> msgCntMap = new HashMap<>();

        /**
         * @param destNodeId Node id to check.
         * @return Number of messages that was sent to node.
         */
        public int getMessagesCount(UUID destNodeId) {
            synchronized (msgCntMap) {
                Integer cnt = msgCntMap.get(destNodeId);

                return cnt == null ? 0 : cnt;
            }
        }

        /**
         *  Clear message counts.
         */
        public void clearCounts() {
            synchronized (msgCntMap) {
                msgCntMap.clear();
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode destNode, Message msg,
            IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            Object msg0 = ((GridIoMessage)msg).message();

            if (!(msg0 instanceof GridClockDeltaSnapshotMessage)) {
                info("Sending message [locNodeId=" + ignite.cluster().localNode().id() +
                    ", destNodeId= " + destNode.id()
                    + ", msg=" + msg + ']');

                synchronized (msgCntMap) {
                    Integer cnt = msgCntMap.get(destNode.id());

                    msgCntMap.put(destNode.id(), cnt == null ? 1 : cnt + 1);
                }
            }

            super.sendMessage(destNode, msg, ackClosure);
        }
    }
}