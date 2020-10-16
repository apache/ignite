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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageReadWriteManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageReadWriteManagerImpl;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.collection.BitSetIntSet;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntRWHashMap;
import org.apache.ignite.internal.util.collection.IntSet;

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
    private final PageStoreMap oldPageStoresMap = new PageStoreMap();

    /** GroupId -> { PartId } */
    private final IntMap<IntSet> partitionsByGrpId = new IntRWHashMap<>();

    /** GroupId -> WorkDir */
    private final IntMap<File> cacheWorkDirsByGrpId = new IntHashMap<>();

    /** GroupId -> CacheGroupContext */
    private final IntMap<CacheGroupContext> grpContextsByGrpId = new IntHashMap<>();

    /** Logger. */
    public final IgniteLogger log;

    /** Busy lock. */
    private volatile GridSpinBusyLock busyLock;

    private final Set<Integer> cacheGroupsForDefragmentation;

    /**
     * @param ctx Context.
     * @param dbMgr Database manager.
     * @param log Logger.
     * @param cacheGroupsForDefragmentation Cache group ids for defragmentation.
     */
    public CacheDefragmentationContext(
        GridKernalContext ctx,
        GridCacheDatabaseSharedManager dbMgr,
        IgniteLogger log,
        List<Integer> cacheGroupsForDefragmentation
    ) {
        this.ctx = ctx;
        this.dbMgr = dbMgr;

        this.log = log;
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
     * @param grpId Group id.
     *
     * @return Work dir for cache group. It has partitions and index.bin among other files.
     */
    public File workDirForGroupId(int grpId) {
        return cacheWorkDirsByGrpId.get(grpId);
    }

    /**
     * @return Busy lock.
     */
    public GridSpinBusyLock busyLock() {
        return busyLock;
    }

    /**
     * @return Arrays of group ids for all non-system cache groups.
     */
    public int[] groupIdsForDefragmentation() {
        int[] grpIds = partitionsByGrpId.keys();

        Arrays.sort(grpIds);

        return grpIds;
    }

    /**
     * @param grpId Group id.
     * @return Actual partition files for cache group that were present on the storage during the start of the node.
     */
    public int[] partitionsForGroupId(int grpId) {
        IntSet partitions = partitionsByGrpId.get(grpId);

        return partitions == null ? null : partitions.toIntArray();
    }

    /**
     * @param grpId Grou pid.
     * @return Cache group context.
     */
    public CacheGroupContext groupContextByGroupId(int grpId) {
        return grpContextsByGrpId.get(grpId);
    }

    /**
     * @param grpId Group id.
     * @param partId Partition id.
     * @return Page store oject for given partition.
     */
    public PageStore pageStore(int grpId, int partId) {
        return oldPageStoresMap.getStore(grpId, partId);
    }

    /**
     * Registers work dir and partition store for given partition.
     *
     * @param grpId Group id.
     * @param cacheWorkDir Work dir.
     * @param partId Partition id.
     * @param partStore Partition store.
     *
     * @see CacheDefragmentationContext#workDirForGroupId(int)
     * @see CacheDefragmentationContext#pageStore(int, int)
     */
    public void onPageStoreCreated(int grpId, File cacheWorkDir, int partId, PageStore partStore) {
        //TODO defragmenting all groups despite the input for easier testing
//        if (!cacheGroupsForDefragmentation.contains(grpId))
//            return;

        cacheWorkDirsByGrpId.putIfAbsent(grpId, cacheWorkDir);

        oldPageStoresMap.addPageStore(grpId, partId, partStore);
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

    /**
     * Registers information about cache group.
     *
     * @param grp Cache group context.
     * @param partId Partition id.
     * @param busyLock Busy lock.
     *
     * @see CacheDefragmentationContext#groupContextByGroupId(int)
     * @see CacheDefragmentationContext#partitionsForGroupId(int)
     */
    public void onCacheStoreCreated(CacheGroupContext grp, int partId, GridSpinBusyLock busyLock) {
        if (!grp.userCache())// || !cacheGroupsForDefragmentation.contains(grp.groupId()))
            return;

        if (this.busyLock == null)
            this.busyLock = busyLock;

        int grpId = grp.groupId();
        grpContextsByGrpId.putIfAbsent(grpId, grp);

        try {
            if (!grp.shared().pageStore().exists(grpId, partId))
                return;

            IntSet partitions = partitionsByGrpId.get(grpId);

            if (partitions == null)
                partitionsByGrpId.put(grpId, partitions = new BitSetIntSet());

            partitions.add(partId);
        }
        catch (IgniteCheckedException ignore) {
            // No-op.
        }
    }

    /** */
    public void onCacheGroupDefragmented(int grpId) {
        // Invalidate page stores.
    }
}
