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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDataStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;

import java.io.File;
import java.util.Collection;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;

public class CachePartitionDefragmentationManager {
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
        }
    }

    private void createCacheGroupContext(CacheGroupContext grpCtx, Integer p) throws IgniteCheckedException {
        CacheDefragmentationContext defrgCtx = sharedCtx.database().defragmentationContext();

        CacheGroupContext newContext = new CacheGroupContext(
            sharedCtx,
            grpCtx.groupId(),
            grpCtx.receivedFrom(),
            CacheType.USER,
            grpCtx.config(),
            grpCtx.affinityNode(),
            defrgCtx.defragmenationDataRegion(),
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
    }
}
