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

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.testframework.MvccFeatureChecker.forcedMvcc;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Checks one-phase commit scenarios.
 */
public class IgniteOnePhaseCommitNearSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 4;

    /** */
    private int backups = 1;

    /** */
    private static Map<Class<?>, AtomicInteger> msgCntMap = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(igniteInstanceName));

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        cfg.setCommunicationSpi(new MessageCountingCommunicationSpi());

        return cfg;
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(String igniteInstanceName) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setBackups(backups);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setNearConfiguration(new NearCacheConfiguration());

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOnePhaseCommitFromNearNode() throws Exception {
        if (forcedMvcc())
            return;

        backups = 1;

        startGrids(GRID_CNT);

        try {
            awaitPartitionMapExchange();

            int key = generateNearKey();

            IgniteCache<Object, Object> cache = ignite(0).cache(DEFAULT_CACHE_NAME);

            checkKey(ignite(0).transactions(), cache, key);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param transactions Transactions instance.
     * @param cache Cache instance.
     * @param key Key.
     */
    private void checkKey(IgniteTransactions transactions, Cache<Object, Object> cache, int key) throws Exception {
        cache.put(key, key);

        finalCheck(key, true);

        TransactionIsolation[] isolations = {READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE};
        TransactionConcurrency[] concurrencies = {OPTIMISTIC, PESSIMISTIC};

        for (TransactionIsolation isolation : isolations) {
            for (TransactionConcurrency concurrency : concurrencies) {
                info("Checking transaction [isolation=" + isolation + ", concurrency=" + concurrency + ']');

                try (Transaction tx = transactions.txStart(concurrency, isolation)) {
                    cache.put(key, isolation + "-" + concurrency);

                    tx.commit();
                }

                finalCheck(key, true);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void finalCheck(final int key, boolean onePhase) throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    for (int i = 0; i < GRID_CNT; i++) {
                        GridCacheAdapter<Object, Object> cache = ((IgniteKernal)ignite(i)).internalCache(DEFAULT_CACHE_NAME);

                        GridCacheEntryEx entry = cache.peekEx(key);

                        if (entry != null) {
                            if (entry.lockedByAny()) {
                                info("Near entry is still locked [i=" + i + ", entry=" + entry + ']');

                                return false;
                            }
                        }

                        entry = cache.context().near().dht().peekEx(key);

                        if (entry != null) {
                            if (entry.lockedByAny()) {
                                info("DHT entry is still locked [i=" + i + ", entry=" + entry + ']');

                                return false;
                            }
                        }
                    }

                    return true;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    info("Entry was removed, will retry");

                    return false;
                }
            }
        }, 10_000);

        if (onePhase) {
            assertMessageCount(GridNearTxPrepareRequest.class, 1);
            assertMessageCount(GridDhtTxPrepareRequest.class, 1);
            assertMessageCount(GridNearTxFinishRequest.class, 1);
            assertMessageCount(GridDhtTxFinishRequest.class, 1);

            msgCntMap.clear();
        }
    }

    /**
     * @param cls Class to check.
     * @param cnt Expected count.
     */
    private void assertMessageCount(Class<?> cls, int cnt) {
        AtomicInteger val = msgCntMap.get(cls);

        int iVal = val == null ? 0 : val.get();

        assertEquals("Invalid message count for class: " + cls.getSimpleName(), cnt, iVal);
    }

    /**
     * @return Key.
     */
    protected int generateNearKey() {
        Affinity<Object> aff = ignite(0).affinity(DEFAULT_CACHE_NAME);

        int key = 0;

        while (true) {
            boolean primary = aff.isPrimary(ignite(1).cluster().localNode(), key);
            boolean primaryOrBackup = aff.isPrimaryOrBackup(ignite(0).cluster().localNode(), key);

            if (primary && !primaryOrBackup)
                return key;

            key++;
        }
    }

    /**
     *
     */
    private static class MessageCountingCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                GridIoMessage ioMsg = (GridIoMessage)msg;

                Class<?> cls = ioMsg.message().getClass();

                AtomicInteger cntr = msgCntMap.get(cls);

                if (cntr == null)
                    cntr = F.addIfAbsent(msgCntMap, cls, new AtomicInteger());

                cntr.incrementAndGet();
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }
}
