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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageReadWriteManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageReadWriteManagerImpl;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.DEFRAGMENTATION_MAPPING_REGION_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.DEFRAGMENTATION_PART_REGION_NAME;

/** */
public class CacheDefragmentationContext {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final GridCacheDatabaseSharedManager dbMgr;

    /** */
    private final PageStoreMap partPageStoresMap = new PageStoreMap();

    /** */
    private final PageStoreMap mappingPageStoresMap = new PageStoreMap();

    /** */
    private final Set<Integer> cacheGroupsForDefragmentation;

    /**
     * @param ctx Context.
     * @param dbMgr Database manager.
     * @param cacheGroupsForDefragmentation Cache group ids for defragmentation.
     */
    public CacheDefragmentationContext(
        GridKernalContext ctx,
        GridCacheDatabaseSharedManager dbMgr,
        List<Integer> cacheGroupsForDefragmentation
    ) {
        this.ctx = ctx;
        this.dbMgr = dbMgr;

        this.cacheGroupsForDefragmentation = new HashSet<>(cacheGroupsForDefragmentation);
    }

    /**
     * Add page store for defragmentable partition. It'll be used by checkpointer later.
     *
     * @param grpId Group id.
     * @param partId Partition id.
     * @param pageStore Page store.
     */
    public void addPartPageStore(
        int grpId,
        int partId,
        PageStore pageStore
    ) {
        partPageStoresMap.addPageStore(grpId, partId, pageStore);
    }

    /**
     * Remove page store from context because it's obsolete and shouldn't be used.
     *
     * @param grpId Group id.
     * @param partId Partition id.
     */
    public void removePartPageStore(
        int grpId,
        int partId
    ) {
        partPageStoresMap.removePageStore(grpId, partId);
    }

    /**
     * Add page store for partition mapping. It'll be used later to defragment index partition.
     *
     * @param grpId Group id.
     * @param partId Partition id.
     * @param pageStore Page store.
     */
    public void addMappingPageStore(
        int grpId,
        int partId,
        PageStore pageStore
    ) {
        mappingPageStoresMap.addPageStore(grpId, partId, pageStore);
    }

    /**
     * @return Page manager for defragmentable partitions.
     */
    public PageReadWriteManager partPageManager() {
        return new PageReadWriteManagerImpl(ctx, partPageStoresMap, "defrgPartitionsStore");
    }

    /**
     * @return Page manager for mapping.
     */
    public PageReadWriteManager mappingPageManager() {
        return new PageReadWriteManagerImpl(ctx, mappingPageStoresMap, "defrgLinkMappingStore");
    }

    /**
     * @return Data region to use for results of defragmentation.
     * @throws IgniteCheckedException Effectively impossible.
     */
    public DataRegion partitionsDataRegion() throws IgniteCheckedException {
        return dbMgr.dataRegion(DEFRAGMENTATION_PART_REGION_NAME);
    }

    /**
     * @return Data region to use for cache links mapping.
     * @throws IgniteCheckedException Effectively impossible.
     */
    public DataRegion mappingDataRegion() throws IgniteCheckedException {
        return dbMgr.dataRegion(DEFRAGMENTATION_MAPPING_REGION_NAME);
    }

    /** */
    public Set<Integer> cacheGroupsForDefragmentation() {
        return cacheGroupsForDefragmentation;
    }

    /** */
    public void onCacheGroupDefragmented(int grpId) {
        // Invalidate page stores.
    }
}
