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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.T1;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class TxOptimisticOnPartitionExchangeTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Tx size. */
    private static final int TX_SIZE = 20 * NODES_CNT;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Logger started. */
    private static volatile boolean msgInterception;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES_CNT);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestCommunicationSpi(log()));

        cfg.setCacheConfiguration(defaultCacheConfiguration()
            .setName(CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setCacheMode(PARTITIONED)
            .setBackups(1));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConsistencyOnPartitionExchange() throws Exception {
        doTest(SERIALIZABLE, true);
        doTest(READ_COMMITTED, true);
        doTest(SERIALIZABLE, false);
        doTest(READ_COMMITTED, false);
    }

    /**
     * @param isolation {@link TransactionIsolation}.
     * @param txInitiatorPrimary False If the transaction does not use the keys of the node that initiated it.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void doTest(final TransactionIsolation isolation, boolean txInitiatorPrimary) throws Exception {
        final CountDownLatch txStarted = new CountDownLatch(1);

        final IgniteCache cache = ignite(0).cache(CACHE_NAME);

        final Map<Integer, Integer> txValues = new TreeMap<>();

        ClusterNode node = ignite(0).cluster().node();

        GridCacheAffinityManager affinity = ((IgniteCacheProxy)cache).context().affinity();

        for (int i = 0; txValues.size() < TX_SIZE; i++) {
            if (!txInitiatorPrimary && node.equals(affinity.primaryByKey(i, NONE)))
                continue;

            txValues.put(i, i);
        }

        TestCommunicationSpi.init();

        msgInterception = true;

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() {
                try (Transaction tx = ignite(0).transactions().txStart(OPTIMISTIC, isolation)) {
                    info(">>> TX started.");

                    txStarted.countDown();

                    cache.putAll(txValues);

                    tx.commit();

                    info(">>> TX committed.");
                }

                return null;
            }
        });

        txStarted.await();

        try {
            info(">>> Grid starting.");

            IgniteEx ignite = startGrid(NODES_CNT);

            info(">>> Grid started.");

            fut.get();

            awaitPartitionMapExchange();

            msgInterception = false;

            IgniteCache<Object, Object> cacheStartedNode = ignite.cache(CACHE_NAME);

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                Set<Object> keys = cacheStartedNode.getAll(txValues.keySet()).keySet();

                assertEquals(txValues.keySet(), new TreeSet<>(keys));

                tx.commit();
            }
        }
        finally {
            msgInterception = false;

            stopGrid(NODES_CNT);
        }
    }

    /**
     *
     */
    @SuppressWarnings("ConstantConditions")
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Partition single message sent from added node. */
        private static volatile CountDownLatch partSingleMsgSentFromAddedNode;

        /** Partition supply message sent count. */
        private static final AtomicInteger partSupplyMsgSentCnt = new AtomicInteger();

        /** Logger. */
        private IgniteLogger log;

        /**
         * @param log Logger.
         */
        public TestCommunicationSpi(IgniteLogger log) {
            this.log = log;
        }

        /**
         *
         */
        public static void init() {
            partSingleMsgSentFromAddedNode = new CountDownLatch(1);

            partSupplyMsgSentCnt.set(0);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(
            final ClusterNode node,
            final Message msg,
            final IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            if (msgInterception) {
                if (msg instanceof GridIoMessage) {
                    final Message msg0 = ((GridIoMessage)msg).message();

                    String locNodeId = ((IgniteEx)ignite).context().localNodeId().toString();

                    int nodeIdx = Integer.parseInt(locNodeId.substring(locNodeId.length() - 3));

                    if (nodeIdx == 0) {
                        if (msg0 instanceof GridNearTxPrepareRequest || msg0 instanceof GridDhtTxPrepareRequest) {
                            GridTestUtils.runAsync(new Callable<Void>() {
                                @Override public Void call() throws Exception {
                                    partSingleMsgSentFromAddedNode.await();

                                    sendMessage(node, msg, ackC, true);

                                    return null;
                                }
                            });

                            return;

                        }
                        else if (msg0 instanceof GridNearTxFinishRequest || msg0 instanceof GridDhtTxFinishRequest) {
                            GridTestUtils.runAsync(new Callable<Void>() {
                                @Override public Void call() throws Exception {
                                    final T1<Integer> i = new T1<>(0);

                                    while (waitForCondition(new GridAbsPredicate() {
                                        @Override public boolean apply() {
                                            return partSupplyMsgSentCnt.get() > i.get();
                                        }
                                    }, i.get() == 0 ? 5_000 : 500))
                                        i.set(partSupplyMsgSentCnt.get());

                                    sendMessage(node, msg, ackC, true);

                                    return null;
                                }
                            });

                            return;
                        }
                    }
                    else if (nodeIdx == NODES_CNT && msg0 instanceof GridDhtPartitionsSingleMessage)
                        partSingleMsgSentFromAddedNode.countDown();

                    if (msg0 instanceof GridDhtPartitionSupplyMessage)
                        partSupplyMsgSentCnt.incrementAndGet();
                }
            }

            sendMessage(node, msg, ackC, msgInterception);
        }

        /**
         * @param node Node.
         * @param msg Message.
         * @param ackC Ack closure.
         * @param logMsg Log Messages.
         */
        private void sendMessage(
            final ClusterNode node,
            final Message msg,
            final IgniteInClosure<IgniteException> ackC,
            boolean logMsg
        ) throws IgniteSpiException {
            if (logMsg) {
                String id = node.id().toString();
                String locNodeId = ((IgniteEx)ignite).context().localNodeId().toString();

                Message msg0 = ((GridIoMessage)msg).message();

                log.info(
                    String.format(">>> Output msg[type=%s, fromNode= %s, toNode=%s]",
                        msg0.getClass().getSimpleName(),
                        locNodeId.charAt(locNodeId.length() - 1),
                        id.charAt(id.length() - 1)
                    )
                );
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
