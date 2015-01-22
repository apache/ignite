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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * Check for specific support issue.
 */
public class GridCacheOffheapUpdateSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(GridCacheMode.PARTITIONED);
        ccfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        ccfg.setAtomicityMode(GridCacheAtomicityMode.TRANSACTIONAL);
        ccfg.setOffHeapMaxMemory(0);
        ccfg.setMemoryMode(GridCacheMemoryMode.OFFHEAP_TIERED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateInPessimisticTxOnRemoteNode() throws Exception {
        try {
            Ignite ignite = startGrids(2);

            GridCache<Object, Object> rmtCache = ignite.cache(null);

            int key = 0;

            while (!rmtCache.affinity().isPrimary(grid(1).localNode(), key))
                key++;

            GridCache<Object, Object> locCache = grid(1).cache(null);

            try (IgniteTx tx = locCache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                locCache.putxIfAbsent(key, 0);

                tx.commit();
            }

            try (IgniteTx tx = rmtCache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                assertEquals(0, rmtCache.get(key));

                rmtCache.putx(key, 1);

                tx.commit();
            }

            try (IgniteTx tx = rmtCache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                assertEquals(1, rmtCache.get(key));

                rmtCache.putx(key, 2);

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

            GridCache<Object, Object> cache = grid.cache(null);

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
                if (!cache.affinity().isPrimary(locNode, key) && !cache.affinity().isBackup(locNode, key))
                    break;
            }

            assertEquals(10, cache.get(key));

            try (IgniteTx ignored = cache.txStart(OPTIMISTIC, REPEATABLE_READ)) {
                assertEquals(10, cache.get(key));
            }

            try (IgniteTx ignored = cache.txStart(PESSIMISTIC, READ_COMMITTED)) {
                assertEquals(10, cache.get(key));
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
