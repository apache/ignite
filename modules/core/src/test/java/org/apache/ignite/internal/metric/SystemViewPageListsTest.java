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

package org.apache.ignite.internal.metric;

import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import org.apache.ignite.internal.systemview.CachePagesListViewWalker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.CachePagesListView;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.PagesListView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.DATA_REGION_PAGE_LIST_VIEW;

/** Tests for {@link SystemView} for page lists. */
public class SystemViewPageListsTest extends SystemViewAbstractTest {
    /** */
    @Test
    public void testPagesList() throws Exception {
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration()
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDataRegionConfigurations(
                    new DataRegionConfiguration().setName("dr0").setMaxSize(100L * 1024 * 1024),
                    new DataRegionConfiguration().setName("dr1").setMaxSize(100L * 1024 * 1024)
                        .setPersistenceEnabled(true)
                )))) {
            ignite.cluster().state(ClusterState.ACTIVE);

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ignite.context().cache().context()
                .database();

            int pageSize = dbMgr.pageSize();

            dbMgr.enableCheckpoints(false).get();

            for (int i = 0; i < 2; i++) {
                IgniteCache<Object, Object> cache = ignite.getOrCreateCache(new CacheConfiguration<>("cache" + i)
                    .setDataRegionName("dr" + i).setAffinity(new RendezvousAffinityFunction().setPartitions(2)));

                int key = 0;

                // Fill up different free-list buckets.
                for (int j = 0; j < pageSize / 2; j++)
                    cache.put(key++, new byte[j + 1]);

                // Put some pages to one bucket to overflow pages cache.
                for (int j = 0; j < 1000; j++)
                    cache.put(key++, new byte[pageSize / 2]);
            }

            long dr0flPages = 0;
            int dr0flStripes = 0;

            SystemView<PagesListView> dataRegionPageLists = ignite.context().systemView().view(DATA_REGION_PAGE_LIST_VIEW);

            for (PagesListView pagesListView : dataRegionPageLists) {
                if (pagesListView.name().startsWith("dr0")) {
                    dr0flPages += pagesListView.bucketSize();
                    dr0flStripes += pagesListView.stripesCount();
                }
            }

            assertTrue(dr0flPages > 0);
            assertTrue(dr0flStripes > 0);

            int bucketsCnt = ((PagesList)ignite.context().cache().context().database().freeList("dr0")).bucketsCount();
            int[] bucketPagesSize = new int[bucketsCnt];

            for (PagesListView pagesListView : dataRegionPageLists) {
                int bucket = pagesListView.bucketNumber();

                if (bucketPagesSize[bucket] == 0) {
                    assertTrue(bucket == 0 || pagesListView.pageFreeSpace() != 0);
                    bucketPagesSize[bucket] = pagesListView.pageFreeSpace();
                }
                else
                    assertEquals(bucketPagesSize[bucket], pagesListView.pageFreeSpace());
            }

            int prev = 0;

            for (int size : bucketPagesSize) {
                if (size > 0) {
                    assertTrue(size > prev);
                    prev = size;
                }
            }

            SystemView<CachePagesListView> cacheGrpPageLists = ignite.context().systemView().view(CACHE_GRP_PAGE_LIST_VIEW);

            long dr1flPages = 0;
            int dr1flStripes = 0;
            int dr1flCached = 0;

            for (CachePagesListView pagesListView : cacheGrpPageLists) {
                if (pagesListView.cacheGroupId() == cacheId("cache1")) {
                    dr1flPages += pagesListView.bucketSize();
                    dr1flStripes += pagesListView.stripesCount();
                    dr1flCached += pagesListView.cachedPagesCount();
                }
            }

            assertTrue(dr1flPages > 0);
            assertTrue(dr1flStripes > 0);
            assertTrue(dr1flCached > 0);

            // Test filtering.
            assertTrue(cacheGrpPageLists instanceof FiltrableSystemView);

            Iterator<CachePagesListView> iter = ((FiltrableSystemView<CachePagesListView>)cacheGrpPageLists).iterator(Map.of(
                CachePagesListViewWalker.CACHE_GROUP_ID_FILTER, cacheId("cache1"),
                CachePagesListViewWalker.PARTITION_ID_FILTER, 0,
                CachePagesListViewWalker.BUCKET_NUMBER_FILTER, 0
            ));

            assertEquals(1, F.size(iter));

            iter = ((FiltrableSystemView<CachePagesListView>)cacheGrpPageLists).iterator(Map.of(
                CachePagesListViewWalker.CACHE_GROUP_ID_FILTER, cacheId("cache1"),
                CachePagesListViewWalker.BUCKET_NUMBER_FILTER, 0
            ));

            assertEquals(2, F.size(iter));
        }
    }
}
