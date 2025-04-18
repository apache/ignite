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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrExpirationInfo;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/** */
public class PendingTreeCorruptionTest extends GridCommonAbstractTest {
    /** PDS enabled. */
    private boolean pds;

    /** */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(pds)
            )
            .setWalSegments(3)
            .setWalSegmentSize(512 * 1024)
        );

        return cfg;
    }

    /** */
    @Test
    public void testCorruptionWhileLoadingData() throws Exception {
        pds = true;

        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        String expireCacheName = "cacheWithExpire";
        String regularCacheName = "cacheWithoutExpire";
        String grpName = "cacheGroup";

        IgniteCache<Object, Object> expireCache = ig.getOrCreateCache(
            new CacheConfiguration<>(expireCacheName)
                .setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(MINUTES, 10)))
                .setGroupName(grpName)
        );

        IgniteCache<Object, Object> regularCache = ig.getOrCreateCache(
            new CacheConfiguration<>(regularCacheName)
                .setGroupName(grpName)
        );

        // This will initialize partition and cache structures.
        expireCache.put(0, 0);
        expireCache.remove(0);

        int expireCacheId = CU.cacheGroupId(expireCacheName, grpName);

        CacheGroupContext grp = ig.context().cache().cacheGroup(CU.cacheId(grpName));
        IgniteCacheOffheapManager.CacheDataStore store = grp.topology().localPartition(0).dataStore();

        assertNotNull(store);

        // Get pending tree of expire cache.
        PendingEntriesTree pendingTree = store.pendingTree();

        long year = TimeUnit.DAYS.toMillis(365);
        long expiration = System.currentTimeMillis() + year;

        ig.context().cache().context().database().checkpointReadLock();

        try {
            // Carefully calculated number. Just enough for the first split to happen, but not more.
            for (int i = 0; i < 202; i++)
                pendingTree.putx(new PendingRow(expireCacheId, expiration, expiration + i)); // link != 0

            // Open cursor, it'll cache first leaf of the tree.
            GridCursor<PendingRow> cur = pendingTree.find(
                null,
                new PendingRow(expireCacheId, expiration + year, 0),
                PendingEntriesTree.WITHOUT_KEY
            );

            // Required for "do" loop to work.
            assertTrue(cur.next());

            int cnt = 0;

            // Emulate real expiry loop but with a more precise control.
            do {
                PendingRow row = cur.get();

                pendingTree.removex(row);

                // Another carefully calculated moment. Here the page cache is exhausted AND the real page is merged
                // with its sibling, meaning that cached "nextPageId" points to empty page from reuse list.
                if (row.link - row.expireTime == 100) {
                    // Put into another cache will take a page from reuse list first. This means that cached
                    // "nextPageId" points to a data page.
                    regularCache.put(0, 0);
                }

                cnt++;
            }
            while (cur.next());

            assertEquals(202, cnt);
        }
        finally {
            ig.context().cache().context().database().checkpointReadUnlock();
        }
    }

    /** */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, value = "1000")
    public void testCorruptionOnExpiration() throws Exception {
        pds = false;

        IgniteEx srv = startGrid();

        srv.cluster().state(ClusterState.ACTIVE);

        CountDownLatch expirationStarted = new CountDownLatch(1);
        CountDownLatch entryUpdated = new CountDownLatch(1);

        IgniteCache<Object, Object> cache = srv.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new ExpirationWaitingRendezvousAffinityFunction(expirationStarted, entryUpdated)));

        // Warmup to ensure that the next put/remove/put will create row with the same link.
        cache.put(0, 0);
        cache.remove(0);

        IgniteInternalCache<Object, Object> cachex = srv.cachex(DEFAULT_CACHE_NAME);

        GridCacheVersion ver = new GridCacheVersion(1, 1, 1, 2);
        KeyCacheObject key = new KeyCacheObjectImpl(0, null, -1);
        CacheObjectImpl val = new CacheObjectImpl(0, null);
        GridCacheDrInfo drInfo = new GridCacheDrExpirationInfo(val, ver, 1, CU.toExpireTime(1000));

        Map<KeyCacheObject, GridCacheDrInfo> map = F.asMap(key, drInfo);

        cachex.putAllConflict(map);

        // Wait for PendingTree row removal.
        assertTrue(expirationStarted.await(10, SECONDS));

        // Remove entry and put entry with the same key, with the same expire time to the same place (with the same link).
        cachex.removeAllConflict(F.asMap(key, ver));
        cachex.putAllConflict(map);

        // Resume expiration thread.
        entryUpdated.countDown();

        // Wait for entry removal by expiration.
        assertTrue(GridTestUtils.waitForCondition(() -> !cache.containsKey(0), 1_000L));

        // Check pending tree is in consistent state.
        CacheGroupContext grp = cachex.context().group();
        PendingEntriesTree pendingTree = grp.topology().localPartition(0).dataStore().pendingTree();

        int cacheId = CU.cacheId(DEFAULT_CACHE_NAME);

        List<PendingRow> rows = pendingTree.remove(new PendingRow(cacheId, Long.MIN_VALUE, 0),
            new PendingRow(cacheId, U.currentTimeMillis(), 0), 1);

        assertTrue(rows.isEmpty());
    }

    /** */
    @SuppressWarnings("TransientFieldNotInitialized")
    private static class ExpirationWaitingRendezvousAffinityFunction extends RendezvousAffinityFunction {
        /** */
        private final transient CountDownLatch expirationStarted;

        /** */
        private final transient CountDownLatch entryUpdated;

        /** */
        public ExpirationWaitingRendezvousAffinityFunction(
            CountDownLatch expirationStarted,
            CountDownLatch entryUpdated
        ) {
            this.expirationStarted = expirationStarted;
            this.entryUpdated = entryUpdated;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            if (Thread.currentThread().getName().contains("ttl-cleanup-worker")) {
                expirationStarted.countDown();

                // Suspend ttl-cleanup-worker after PendingTree row is removed, but before the corresponding
                // expired row is deleted from cache data tree and row store.
                U.awaitQuiet(entryUpdated);
            }

            return super.partition(key);
        }
    }
}
