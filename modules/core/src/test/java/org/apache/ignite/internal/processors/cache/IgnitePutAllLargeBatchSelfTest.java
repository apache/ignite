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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;

/**
 * Tests putAll method with large number of keys.
 */
public class IgnitePutAllLargeBatchSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 4;

    /** */
    private boolean nearEnabled;

    /** Backups. */
    private int backups = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Test cache configuration.
     */
    public CacheConfiguration cacheConfiguration(String gridName) {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(backups);
        ccfg.setNearConfiguration(nearEnabled ? new NearCacheConfiguration() : null);
        ccfg.setCacheMode(PARTITIONED);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPessimisticOneBackupPartitioned() throws Exception {
        backups = 1;

        checkPutAll(PESSIMISTIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPessimisticOneBackupNear() throws Exception {
        backups = 1;

        checkPutAll(PESSIMISTIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllOptimisticOneBackupPartitioned() throws Exception {
        backups = 1;

        checkPutAll(OPTIMISTIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllOptimisticOneBackupNear() throws Exception {
        backups = 1;

        checkPutAll(OPTIMISTIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPessimisticTwoBackupsPartitioned() throws Exception {
        backups = 2;

        checkPutAll(PESSIMISTIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPessimisticTwoBackupsNear() throws Exception {
        backups = 2;

        checkPutAll(PESSIMISTIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllOptimisticTwoBackupsPartitioned() throws Exception {
        backups = 2;

        checkPutAll(OPTIMISTIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllOptimisticTwoBackupsNear() throws Exception {
        backups = 2;

        checkPutAll(OPTIMISTIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPutAll(TransactionConcurrency concurrency, boolean nearEnabled) throws Exception {
        this.nearEnabled = nearEnabled;

        startGrids(GRID_CNT);

        awaitPartitionMapExchange();

        try {
            IgniteCache<Object, Object> cache = grid(0).cache(null);

            int keyCnt = 200;

            for (int i = 0; i < keyCnt; i++)
                cache.put(i, i);

            // Create readers if near cache is enabled.
            for (int g = 1; g < 2; g++) {
                for (int i = 30; i < 70; i++)
                    ((IgniteKernal)grid(g)).getCache(null).get(i);
            }

            info(">>> Starting test tx.");

            try (Transaction tx = grid(0).transactions().txStart(concurrency, TransactionIsolation.REPEATABLE_READ)) {
                Map<Integer, Integer> map = new LinkedHashMap<>();

                for (int i = 0; i < keyCnt; i++)
                    map.put(i, i * i);

                cache.getAll(map.keySet());

                cache.putAll(map);

                tx.commit();
            }

            //  Check that no stale transactions left and all locks are released.
            for (int g = 0; g < GRID_CNT; g++) {
                IgniteKernal k = (IgniteKernal)grid(g);

                GridCacheAdapter<Object, Object> cacheAdapter = k.context().cache().internalCache();

                assertEquals(0, cacheAdapter.context().tm().idMapSize());

                for (int i = 0; i < keyCnt; i++) {
                    if (cacheAdapter.isNear()) {
                        GridDhtCacheEntry entry = (GridDhtCacheEntry)
                            ((GridNearCacheAdapter<Object, Object>)cacheAdapter).dht().peekEx(i);

                        if (entry != null) {
                            assertFalse(entry.lockedByAny());
                            assertTrue(entry.localCandidates().isEmpty());
                            assertTrue(entry.remoteMvccSnapshot().isEmpty());
                        }
                    }

                    GridCacheEntryEx entry = cacheAdapter.peekEx(i);

                    if (entry != null) {
                        assertFalse(entry.lockedByAny());
                        assertTrue(entry.localCandidates().isEmpty());
                        assertTrue(entry.remoteMvccSnapshot().isEmpty());
                    }
                }
            }

            for (int g = 0; g < GRID_CNT; g++) {
                IgniteCache<Object, Object> checkCache =grid(g).cache(null);

                ClusterNode checkNode = grid(g).localNode();

                for (int i = 0; i < keyCnt; i++) {
                    if (grid(g).affinity(null).isPrimaryOrBackup(checkNode, i))
                        assertEquals(i * i, checkCache.localPeek(i, CachePeekMode.PRIMARY, CachePeekMode.BACKUP));
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreviousValuePartitionedOneBackup() throws Exception {
        backups = 1;
        nearEnabled = false;

        checkPreviousValue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreviousValuePartitionedTwoBackups() throws Exception {
        backups = 2;
        nearEnabled = false;

        checkPreviousValue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreviousValueNearOneBackup() throws Exception {
        backups = 1;
        nearEnabled = true;

        checkPreviousValue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreviousValueNearTwoBackups() throws Exception {
        backups = 2;
        nearEnabled = true;

        checkPreviousValue();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPreviousValue() throws Exception {
        startGrids(GRID_CNT);

        awaitPartitionMapExchange();

        try {
            Map<Integer, Integer> checkMap = new HashMap<>();

            IgniteCache<Integer, Integer> cache = grid(0).cache(null);

            for (int r = 0; r < 3; r++) {
                for (int i = 0; i < 10; i++) {
                    info("Put: " + i + ", " + r);

                    Integer cachePrev = cache.getAndPut(i, r);

                    Integer mapPrev = checkMap.put(i, r);

                    assertEquals(mapPrev, cachePrev);
                }

                info(">>>>>>> Done round: " + r);
            }
        }
        finally {
            stopAllGrids();
        }
    }
}