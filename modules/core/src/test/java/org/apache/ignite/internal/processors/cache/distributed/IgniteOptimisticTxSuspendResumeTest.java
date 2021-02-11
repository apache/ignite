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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.SUSPENDED;

/**
 *
 */
public class IgniteOptimisticTxSuspendResumeTest extends IgniteAbstractTxSuspendResumeTest {
    /** {@inheritDoc} */
    @Override protected TransactionConcurrency transactionConcurrency() {
        return OPTIMISTIC;
    }

    /**
     * Test start 1 transaction, suspendTx it. And then start another transaction, trying to write
     * the same key and commit it.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSuspendTxAndStartNew() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation tx1Isolation : TransactionIsolation.values()) {
                    for (TransactionIsolation tx2Isolation : TransactionIsolation.values()) {
                        Transaction tx1 = ignite.transactions().txStart(OPTIMISTIC, tx1Isolation);

                        cache.put(1, 1);

                        tx1.suspend();

                        assertFalse(cache.containsKey(1));

                        Transaction tx2 = ignite.transactions().txStart(OPTIMISTIC, tx2Isolation);

                        cache.put(1, 2);

                        tx2.commit();

                        assertEquals(2, (int)cache.get(1));

                        tx1.resume();

                        assertEquals(1, (int)cache.get(1));

                        tx1.close();

                        cache.removeAll();
                    }
                }
            }
        });
    }

    /**
     * Test start 1 transaction, suspendTx it. And then start another transaction, trying to write
     * the same key.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSuspendTxAndStartNewWithoutCommit() throws Exception {
        executeTestForAllCaches(new CI2Exc<Ignite, IgniteCache<Integer, Integer>>() {
            @Override public void applyx(Ignite ignite, final IgniteCache<Integer, Integer> cache) throws Exception {
                for (TransactionIsolation tx1Isolation : TransactionIsolation.values()) {
                    for (TransactionIsolation tx2Isolation : TransactionIsolation.values()) {
                        Transaction tx1 = ignite.transactions().txStart(OPTIMISTIC, tx1Isolation);

                        cache.put(1, 1);

                        tx1.suspend();

                        assertFalse(cache.containsKey(1));

                        Transaction tx2 = ignite.transactions().txStart(OPTIMISTIC, tx2Isolation);

                        cache.put(1, 2);

                        tx2.suspend();

                        assertFalse(cache.containsKey(1));

                        tx1.resume();

                        assertEquals(1, (int)cache.get(1));

                        tx1.suspend();

                        tx2.resume();

                        assertEquals(2, (int)cache.get(1));

                        tx2.rollback();

                        tx1.resume();
                        tx1.rollback();

                        cache.removeAll();
                    }
                }
            }
        });
    }

    /**
     * Test we can resume and complete transaction if topology changed while transaction is suspended.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSuspendTxAndResumeAfterTopologyChange() throws Exception {
        Ignite srv = ignite(ThreadLocalRandom.current().nextInt(SERVER_CNT));
        Ignite client = ignite(SERVER_CNT);
        Ignite clientNear = ignite(SERVER_CNT + 1);

        Map<String, List<List<Integer>>> cacheKeys = generateKeys(srv, TransactionIsolation.values().length);

        doCheckSuspendTxAndResume(srv, cacheKeys);
        doCheckSuspendTxAndResume(client, cacheKeys);
        doCheckSuspendTxAndResume(clientNear, cacheKeys);
    }

    /**
     * @param node Ignite isntance.
     * @param cacheKeys Different key types mapped to cache name.
     * @throws Exception If failed.
     */
    private void doCheckSuspendTxAndResume(Ignite node, Map<String, List<List<Integer>>> cacheKeys) throws Exception {
        ClusterNode locNode = node.cluster().localNode();

        log.info("Run test for node [node=" + locNode.id() + ", client=" + locNode.isClient() + ']');

        Map<IgniteCache<Integer, Integer>, Map<Transaction, Integer>> cacheTxMap = new IdentityHashMap<>();

        for (Map.Entry<String, List<List<Integer>>> cacheKeysEntry : cacheKeys.entrySet()) {
            String cacheName = cacheKeysEntry.getKey();

            IgniteCache<Integer, Integer> cache = node.cache(cacheName);

            Map<Transaction, Integer> suspendedTxs = new IdentityHashMap<>();

            for (List<Integer> keysList : cacheKeysEntry.getValue()) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    Transaction tx = node.transactions().txStart(OPTIMISTIC, isolation);

                    int key = keysList.get(isolation.ordinal());

                    cache.put(key, key);

                    tx.suspend();

                    suspendedTxs.put(tx, key);

                    String msg = "node=" + node.cluster().localNode() +
                        ", cache=" + cacheName + ", isolation=" + isolation + ", key=" + key;

                    assertEquals(msg, SUSPENDED, tx.state());
                }
            }

            cacheTxMap.put(cache, suspendedTxs);
        }

        int newNodeIdx = gridCount();

        startGrid(newNodeIdx);

        try {
            for (Map.Entry<IgniteCache<Integer, Integer>, Map<Transaction, Integer>> entry : cacheTxMap.entrySet()) {
                IgniteCache<Integer, Integer> cache = entry.getKey();

                for (Map.Entry<Transaction, Integer> suspendedTx : entry.getValue().entrySet()) {
                    Transaction tx = suspendedTx.getKey();

                    Integer key = suspendedTx.getValue();

                    tx.resume();

                    String msg = "node=" + node.cluster().localNode() +
                        ", cache=" + cache.getName() + ", isolation=" + tx.isolation() + ", key=" + key;

                    assertEquals(msg, ACTIVE, tx.state());

                    assertEquals(msg, key, cache.get(key));

                    tx.commit();

                    assertEquals(msg, key, cache.get(key));
                }
            }
        }
        finally {
            stopGrid(newNodeIdx);

            for (IgniteCache<Integer, Integer> cache : cacheTxMap.keySet())
                cache.removeAll();
        }
    }

    /**
     * Generates list of keys (primary, backup and neither primary nor backup).
     *
     * @param ignite Ignite instance.
     * @param keysCnt The number of keys generated for each type of key.
     * @return List of different keys mapped to cache name.
     */
    private Map<String, List<List<Integer>>> generateKeys(Ignite ignite, int keysCnt) {
        Map<String, List<List<Integer>>> cacheKeys = new HashMap<>();

        for (CacheConfiguration cfg : cacheConfigurations()) {
            String cacheName = cfg.getName();

            IgniteCache cache = ignite.cache(cacheName);

            List<List<Integer>> keys = new ArrayList<>();

            // Generate different keys: 0 - primary, 1 - backup, 2 - neither primary nor backup.
            for (int type = 0; type < 3; type++) {
                if (cfg.getCacheMode() == LOCAL)
                    continue;

                if (type == 1 && cfg.getCacheMode() == PARTITIONED && cfg.getBackups() == 0)
                    continue;

                if (type == 2 && cfg.getCacheMode() == REPLICATED)
                    continue;

                List<Integer> keys0 = findKeys(cache, keysCnt, type * 100_000, type);

                assertEquals(cacheName, keysCnt, keys0.size());

                keys.add(keys0);
            }

            if (!keys.isEmpty())
                cacheKeys.put(cacheName, keys);
        }

        return cacheKeys;
    }
}
