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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Simple test for preloading in ATOMIC cache.
 */
public class GridCacheAtomicPreloadSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean nearEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setNearConfiguration(nearEnabled ? new NearCacheConfiguration() : null);
        cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSimpleTxsNear() throws Exception {
        checkSimpleTxs(true, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSimpleTxsColocated() throws Exception {
        checkSimpleTxs(false, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSimpleTxsColocated() throws Exception {
        checkSimpleTxs(false, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSimpleTxsNear() throws Exception {
        checkSimpleTxs(false, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSimpleTxs(boolean nearEnabled, TransactionConcurrency concurrency) throws Exception {
        try {
            this.nearEnabled = nearEnabled;

            startGrids(3);

            awaitPartitionMapExchange();

            IgniteCache<Object, Object> cache = grid(0).cache(null);

            List<Integer> keys = generateKeys(grid(0).localNode(), cache);

            IgniteTransactions txs = grid(0).transactions();

            assert txs != null;

            for (int i = 0; i < keys.size(); i++) {
                Integer key = keys.get(i);

                info(">>>>>>>>>>>>>>>");
                info("Checking transaction for key [idx=" + i + ", key=" + key + ']');
                info(">>>>>>>>>>>>>>>");

                try (Transaction tx = txs.txStart(concurrency, REPEATABLE_READ)) {
                    try {
                        // Lock if pessimistic, read if optimistic.
                        cache.get(key);

                        cache.put(key, key + 1);

                        tx.commit();
                    }
                    catch (Exception e) {
                        // Print exception in case if
                        e.printStackTrace();

                        throw e;
                    }
                }

//                Thread.sleep(500);

                info(">>>>>>>>>>>>>>>");
                info("Finished checking transaction for key [idx=" + i + ", key=" + key + ']');
                info(">>>>>>>>>>>>>>>");

                checkTransactions();
                checkValues(key, key + 1);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private void checkTransactions() {
        for (int i = 0; i < 3; i++) {
            IgniteTxManager tm = ((IgniteKernal)grid(i)).context().cache().context().tm();

            assertEquals("Uncommitted transactions found on node [idx=" + i + ", mapSize=" + tm.idMapSize() + ']',
                0, tm.idMapSize());
        }
    }

    /**
     * @param key Key to check.
     * @param val Expected value.
     */
    private void checkValues(int key, int val) {
        for (int i = 0; i < 3; i++) {
            IgniteEx grid = grid(i);

            ClusterNode node = grid.localNode();

            IgniteCache<Object, Object> cache = grid.cache(null);

            boolean primary = grid.affinity(null).isPrimary(node, key);
            boolean backup = grid.affinity(null).isBackup(node, key);

            if (primary || backup)
                assertEquals("Invalid cache value [nodeId=" + node.id() + ", primary=" + primary +
                    ", backup=" + backup + ", key=" + key + ']', val, cache.localPeek(key, CachePeekMode.ONHEAP));
        }
    }

    /**
     * Generates a set of keys: near, primary, backup.
     *
     * @param node Node for which keys are generated.
     * @param cache Cache to get affinity for.
     * @return Collection of keys.
     */
    private List<Integer> generateKeys(ClusterNode node, IgniteCache<Object, Object> cache) {
        List<Integer> keys = new ArrayList<>(3);

        Affinity<Object> aff = affinity(cache);

        int base = 0;

//        // Near key.
        while (aff.isPrimary(node, base) || aff.isBackup(node, base))
            base++;

        keys.add(base);

//        Primary key.
        while (!aff.isPrimary(node, base))
            base++;

        keys.add(base);

        // Backup key.
        while (!aff.isBackup(node, base))
            base++;

        keys.add(base);

        return keys;
    }
}