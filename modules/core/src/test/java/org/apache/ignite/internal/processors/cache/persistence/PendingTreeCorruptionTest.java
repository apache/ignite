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

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MINUTES;

/** */
public class PendingTreeCorruptionTest extends GridCommonAbstractTest {
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
                .setPersistenceEnabled(true)
            )
            .setWalSegments(3)
            .setWalSegmentSize(512 * 1024)
        );

        return cfg;
    }

    /** */
    @Test
    public void testCorruptionWhileLoadingData() throws Exception {
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
}
