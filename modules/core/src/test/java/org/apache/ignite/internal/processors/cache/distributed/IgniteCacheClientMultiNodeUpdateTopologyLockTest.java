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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheClientMultiNodeUpdateTopologyLockTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE = "testCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticTx() throws Exception {
        startGrids(3);

        Ignite clientNode = startClientGrid(3);

        IgniteCache<Integer, Integer> cache = clientNode.createCache(cacheConfiguration(0, FULL_SYNC));

        awaitPartitionMapExchange();

        Integer key1 = movingKeysAfterJoin(ignite(1), TEST_CACHE, 1).get(0);
        Integer key2 = movingKeysAfterJoin(ignite(2), TEST_CACHE, 1).get(0);

        log.info("Start tx [key1=" + key1 + ", key2=" + key2 + ']');

        IgniteInternalFuture<?> startFut;

        TestRecordingCommunicationSpi spi2 = TestRecordingCommunicationSpi.spi(ignite(2));

        final TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(clientNode);

        final UUID node0Id = ignite(0).cluster().localNode().id();
        final UUID node2Id = ignite(2).cluster().localNode().id();

        spi2.record(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (!node0Id.equals(node.id()))
                    return false;

                return (msg instanceof GridDhtPartitionsSingleMessage) &&
                    ((GridDhtPartitionsSingleMessage)msg).exchangeId() != null;
            }
        });

        clientSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(final ClusterNode node, final Message msg) {
                if (!node2Id.equals(node.id()))
                    return false;

                if (msg instanceof GridNearTxFinishRequest) {
                    log.info("Delay message [msg=" + msg + ']');

                    GridTestUtils.runAsync(new Runnable() {
                        @Override public void run() {
                            try {
                                Thread.sleep(5000);
                            }
                            catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            log.info("Send delayed message [msg=" + msg + ']');

                            clientSpi.stopBlock(true);
                        }
                    });

                    return true;
                }

                return false;
            }
        });

        try (Transaction tx = clientNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key1, 1);

            startFut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(4);

                    return null;
                }
            }, "start-thread");

            spi2.waitForRecorded();

            U.sleep(5);

            cache.put(key2, 2);

            log.info("Commit tx");

            tx.commit();
        }

        assertEquals((Integer)1, cache.get(key1));
        assertEquals((Integer)2, cache.get(key2));

        startFut.get();

        assertEquals((Integer)1, cache.get(key1));
        assertEquals((Integer)2, cache.get(key2));

        awaitPartitionMapExchange();

        assertEquals((Integer)1, cache.get(key1));
        assertEquals((Integer)2, cache.get(key2));
    }

    /**
     * @param backups Number of backups.
     * @param writeSync Cache write synchronization mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(int backups,
        CacheWriteSynchronizationMode writeSync) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setName(TEST_CACHE);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(writeSync);
        ccfg.setBackups(backups);
        ccfg.setRebalanceMode(ASYNC);

        return ccfg;
    }
}
