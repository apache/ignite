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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test onheap caching of freelists.
 */
public class FreeListCachingTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        int pageSize = dsCfg.getPageSize() == 0 ? DataStorageConfiguration.DFLT_PAGE_SIZE : dsCfg.getPageSize();

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(pageSize * 40_000L));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testFreeListCaching() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        int partCnt = 10;

        GridCacheProcessor cacheProc = ignite.context().cache();
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cacheProc.context().database();

        dbMgr.enableCheckpoints(false).get();

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(partCnt))
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        GridCacheOffheapManager offheap = (GridCacheOffheapManager)cacheProc.cache(DEFAULT_CACHE_NAME).context().group()
            .offheap();

        for (int i = 0; i < 5_000; i++) {
            for (int p = 0; p < partCnt; p++) {
                Integer key = i * partCnt + p;
                cache.put(key, new byte[i + 1]);
                cache.remove(key);
            }
        }

        offheap.cacheDataStores().forEach(cacheData -> {
            PagesList list = (PagesList)cacheData.rowStore().freeList();

            AtomicLongArray bucketsSize = list.bucketsSize;

            // All buckets except reuse bucket must be empty after puts and removes of the same key.
            for (int i = 0; i < bucketsSize.length(); i++) {
                if (list.isReuseBucket(i))
                    assertTrue(bucketsSize.get(i) > 0);
                else
                    assertEquals(0, bucketsSize.get(i));
            }
        });

        for (int i = 0; i < 100; i++) {
            for (int p = 0; p < partCnt; p++)
                cache.put(i * partCnt + p, new byte[(i + p) * 10]);
        }

        for (int i = 0; i < 50; i += 2) {
            for (int p = 0; p < partCnt; p++)
                cache.remove(i * partCnt + p);
        }

        Map<Integer, List<Long>> partsBucketsSize = new HashMap<>();

        offheap.cacheDataStores().forEach(cacheData -> {
            PagesList list = (PagesList)cacheData.rowStore().freeList();

            AtomicLongArray bucketsSize = list.bucketsSize;

            List<Long> bucketsSizeList = new ArrayList<>(bucketsSize.length());

            partsBucketsSize.put(cacheData.partId(), bucketsSizeList);

            long notReuseSize = 0;

            for (int i = 0; i < bucketsSize.length(); i++) {
                bucketsSizeList.add(bucketsSize.get(i));

                PagesList.Stripe[] bucket = list.getBucket(i);

                // All buckets are expected to be cached onheap except reuse bucket, since reuse bucket is also used
                // by indexes bypassing caching.
                if (!list.isReuseBucket(i)) {
                    notReuseSize += bucketsSize.get(i);

                    assertNull("Expected null bucket [partId=" + cacheData.partId() + ", i=" + i + ", bucket=" +
                        bucket + ']', bucket);

                    PagesList.PagesCache pagesCache = list.getBucketCache(i, false);

                    assertEquals("Wrong pages cache size [partId=" + cacheData.partId() + ", i=" + i + ']',
                        bucketsSize.get(i), pagesCache == null ? 0 : pagesCache.size());
                }
            }

            assertTrue(notReuseSize > 0);
        });

        dbMgr.enableCheckpoints(true).get();

        forceCheckpoint(ignite);

        offheap.cacheDataStores().forEach(cacheData -> {
            PagesList list = (PagesList)cacheData.rowStore().freeList();

            AtomicLongArray bucketsSize = list.bucketsSize;

            for (int i = 0; i < bucketsSize.length(); i++) {
                long bucketSize = bucketsSize.get(i);

                PagesList.Stripe[] bucket = list.getBucket(i);

                // After checkpoint all buckets must flush onheap cache to page memory.
                if (bucketSize > 0) {
                    assertNotNull("Expected not null bucket [partId=" + cacheData.partId() + ", i=" + i + ']',
                        bucket);
                }

                PagesList.PagesCache pagesCache = list.getBucketCache(i, false);

                assertEquals("Wrong pages cache size [partId=" + cacheData.partId() + ", i=" + i + ']',
                    0, pagesCache == null ? 0 : pagesCache.size());

                assertEquals("Bucket size changed after checkpoint [partId=" + cacheData.partId() + ", i=" + i + ']',
                    (long)partsBucketsSize.get(cacheData.partId()).get(i), bucketSize);
            }
        });

        dbMgr.enableCheckpoints(false).get();

        for (int i = 0; i < 50; i++) {
            for (int p = 0; p < partCnt; p++)
                cache.put(i * partCnt + p, new byte[(i + p) * 10]);
        }

        offheap.cacheDataStores().forEach(cacheData -> {
            PagesList list = (PagesList)cacheData.rowStore().freeList();

            int totalCacheSize = 0;

            for (int i = 0; i < list.bucketsSize.length(); i++) {
                PagesList.PagesCache pagesCache = list.getBucketCache(i, false);

                totalCacheSize += pagesCache == null ? 0 : pagesCache.size();
            }

            assertTrue("Some buckets should be cached [partId=" + cacheData.partId() + ']', totalCacheSize > 0);
        });
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testPageListCacheLimit() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.getOrCreateCache("cache1");
        ignite.getOrCreateCache("cache2");

        GridCacheContext<?, ?> cctx1 = ignite.context().cache().cache("cache1").context();
        GridCacheContext<?, ?> cctx2 = ignite.context().cache().cache("cache2").context();

        GridCacheOffheapManager offheap1 = (GridCacheOffheapManager)cctx1.offheap();
        GridCacheOffheapManager offheap2 = (GridCacheOffheapManager)cctx2.offheap();

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ignite.context().cache().context().database();

        assertEquals(db.pageListCacheLimitHolder(cctx1.dataRegion()), db.pageListCacheLimitHolder(cctx2.dataRegion()));

        long limit = db.pageListCacheLimitHolder(cctx1.dataRegion()).get();

        try (IgniteDataStreamer<Object, Object> streamer1 = ignite.dataStreamer("cache1");
            IgniteDataStreamer<Object, Object> streamer2 = ignite.dataStreamer("cache2")) {
            // Fill caches to trigger "too many dirty pages" checkpoint.
            for (int i = 0; i < 50_000; i++) {
                streamer1.addData(i, new byte[i % 2048]);
                streamer2.addData(i, new byte[i % 2048]);

                // Calculates page list caches count and validate this value periodically.
                if (i % 5_000 == 0) {
                    streamer1.flush();
                    streamer2.flush();

                    AtomicInteger pageCachesCnt = new AtomicInteger();

                    for (GridCacheOffheapManager offheap : F.asList(offheap1, offheap2)) {
                        offheap.cacheDataStores().forEach(cacheData -> {
                            if (cacheData.rowStore() == null)
                                return;

                            PagesList list = (PagesList)cacheData.rowStore().freeList();

                            for (int b = 0; b < list.bucketsSize.length(); b++) {
                                PagesList.PagesCache pagesCache = list.getBucketCache(b, false);

                                if (pagesCache != null && pagesCache.size() > 0)
                                    pageCachesCnt.incrementAndGet();
                            }
                        });
                    }

                    // There can be a race and actual page list caches count can exceed the limit in very rare cases.
                    assertTrue("Page list caches count is more than expected [count: " + pageCachesCnt.get() +
                        ", limit=" + limit + ']', pageCachesCnt.get() <= limit + ignite.configuration()
                        .getDataStreamerThreadPoolSize() - 1);
                }
            }
        }
    }
}
