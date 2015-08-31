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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Check for specific support issue.
 */
public class GridCacheOffheapUpdateSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setNearConfiguration(null);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setOffHeapMaxMemory(0);
        ccfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateInPessimisticTxOnRemoteNode() throws Exception {
        try {
            Ignite ignite = startGrids(2);

            IgniteCache<Object, Object> rmtCache = ignite.cache(null);

            int key = 0;

            while (!ignite.affinity(null).isPrimary(grid(1).localNode(), key))
                key++;

            IgniteCache<Object, Object> locCache = grid(1).cache(null);

            try (Transaction tx = grid(1).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                locCache.putIfAbsent(key, 0);

                tx.commit();
            }

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                assertEquals(0, rmtCache.get(key));

                rmtCache.put(key, 1);

                tx.commit();
            }

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                assertEquals(1, rmtCache.get(key));

                rmtCache.put(key, 2);

                tx.commit();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadEvictedPartition() throws Exception {
        try {
            Ignite grid = startGrid(0);

            IgniteCache<Object, Object> cache = grid.cache(null);

            for (int i = 0; i < 30; i++)
                cache.put(i, 0);

            startGrid(1);

            awaitPartitionMapExchange();

            for (int i = 0; i < 30; i++)
                grid(1).cache(null).put(i, 10);

            // Find a key that does not belong to started node anymore.
            int key = 0;

            ClusterNode locNode = grid.cluster().localNode();

            for (;key < 30; key++) {
                if (!grid.affinity(null).isPrimary(locNode, key) && !grid.affinity(null).isBackup(locNode, key))
                    break;
            }

            assertEquals(10, cache.get(key));

            try (Transaction ignored = grid.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                assertEquals(10, cache.get(key));
            }

            try (Transaction ignored = grid.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                assertEquals(10, cache.get(key));
            }
        }
        finally {
            stopAllGrids();
        }
    }
}