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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDataStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.PageStoreCollection;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;

public class CachePartitionDefragmentationManager implements PageStoreCollection {
    public static final String DEFRAGMENTED_PARTITION_FILE_TEMPLATE = "part-dfrg-%d.bin";

    /** */
    private final LongAdderMetric allocatedMetric = new LongAdderMetric("defragmentedAllocatedTracker", null);

    /** */
    private final GridCacheSharedContext sharedCtx;

    public CachePartitionDefragmentationManager(GridCacheSharedContext sharedCtx) {
        this.sharedCtx = sharedCtx;
    }

    /** */
    public void executeDefragmentation() {
        CacheDefragmentationContext defrgCtx = sharedCtx.database().defragmentationContext();

        FilePageStoreManager filePageStoreMgr = (FilePageStoreManager)sharedCtx.pageStore();

        try {
            for (Integer grpId : defrgCtx.groupIdsForDefragmentation()) {
                File workDir = defrgCtx.workDirForGroupId(grpId);
                Collection<Integer> parts = defrgCtx.partitionsForGroupId(grpId);

                if (workDir != null && parts != null) {
                    //TODO do not forget about encrypted PageStores, this should be addressed
                    FilePageStoreFactory pageStoreFactory = filePageStoreMgr.getPageStoreFactory(grpId, false);

                    for (Integer p : parts) {
                        PageStore pageStore = pageStoreFactory.createPageStore(FLAG_DATA, () -> new File(workDir, String.format(DEFRAGMENTED_PARTITION_FILE_TEMPLATE, p)).toPath(), allocatedMetric);

                        pageStore.sync();

                        createCacheGroupContext(defrgCtx.groupContextByGroupId(grpId), p);
                    }
                }
            }
        }
        catch (IgniteCheckedException e) {
            // No-op for now.
            e.printStackTrace();
        }
    }

    private void createCacheGroupContext(CacheGroupContext grpCtx, Integer p) throws IgniteCheckedException {
        CacheDefragmentationContext defrgCtx = sharedCtx.database().defragmentationContext();

        DataRegion region = defrgCtx.defragmenationDataRegion();

        CacheGroupContext newContext = new CacheGroupContext(
            sharedCtx,
            grpCtx.groupId(),
            grpCtx.receivedFrom(),
            CacheType.USER,
            grpCtx.config(),
            grpCtx.affinityNode(),
            region,
            grpCtx.cacheObjectContext(),
            grpCtx.freeList(),
            grpCtx.reuseList(),
            grpCtx.localStartVersion(),
            true,
            false,
            true
        );

        newContext.start();

        GridCacheDataStore cacheDataStore = new GridCacheDataStore(newContext, p, true, defrgCtx.busyLock(), defrgCtx.log);

        cacheDataStore.init();

        PageMemory memory = region.pageMemory();

        FullPageId id = new FullPageId(memory.allocatePage(grpCtx.groupId(), PageIdAllocator.INDEX_PARTITION, PageIdAllocator.FLAG_IDX), grpCtx.groupId());

        LinkMap m = new LinkMap(grpCtx, memory, id.pageId());

        Iterable<IgniteCacheOffheapManager.CacheDataStore> stores = grpCtx.offheap().cacheDataStores();

        IgniteCacheOffheapManager.CacheDataStore store = StreamSupport
            .stream(stores.spliterator(), false)
            .filter(s -> grpCtx.groupId() == s.tree().groupId())
            .findFirst()
            .orElse(null);

        CacheDataTree tree = store.tree();

        CacheDataTree newTree = cacheDataStore.tree();

        CacheDataRow lower = tree.findFirst();
        CacheDataRow upper = tree.findLast();

        List<GridCacheContext> cacheContexts = grpCtx.caches();

        tree.iterate(lower, upper, (t, io, pageAddr, idx) -> {
            CacheDataRow row = tree.getRow(io, pageAddr, idx);
            int cacheId = row.cacheId();

            GridCacheContext context;

            if (cacheId == CU.UNDEFINED_CACHE_ID)
                context = cacheContexts.get(0);
            else
                context = cacheContexts.stream().filter(c -> c.cacheId() == cacheId).findFirst().orElse(null);

            assert context != null;

            CacheDataRow newRow = cacheDataStore.createRow(context, row.key(), row.value(), row.version(), row.expireTime(), null);

            long link = row.link();

            newTree.put(newRow);
            long newLink = newRow.link();

            m.put(link, newLink);

            return true;
        });
    }

    /** {@inheritDoc} */
    @Override public PageStore getStore(int grpId, int partId) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<PageStore> getStores(int grpId) throws IgniteCheckedException {
        return null;
    }
}
