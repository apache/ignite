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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.collection.BitSetIntSet;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntSet;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.DEFRAGMENTATION_MAPPING_REGION_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.DEFRAGMENTATION_PART_REGION_NAME;

public class CacheDefragmentationContext {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final GridCacheDatabaseSharedManager dbMgr;

    /** */
    private final PageStoreMap partPageStoresMap = new PageStoreMap();

    /** */
    private final PageStoreMap mappingPageStoresMap = new PageStoreMap();

    /** GroupId -> { PartIdx } */
    private IntMap<IntSet> partitionsByGroupId = new IntHashMap<>();

    /** GroupId -> WorkDir */
    private IntMap<File> cacheWorkDirsByGroupId = new IntHashMap<>();

    /** GroupId -> PartIdx -> PageStore */
    private IntMap<IntMap<PageStore>> pageStoresByGrpId = new IntHashMap<>();

    //TODO Find a proper place for this map. Currently we have it in manager.
//    private Map<Integer, List> cacheStoresByGroupId = new HashMap<>();

    /** GroupId -> CacheGroupContext */
    private IntMap<CacheGroupContext> groupContextsByGroupId = new IntHashMap<>();

    public final IgniteLogger log;

    private volatile GridSpinBusyLock busyLock;

    public CacheDefragmentationContext(
        GridKernalContext ctx,
        GridCacheDatabaseSharedManager dbMgr,
        IgniteLogger log
    ) {
        this.ctx = ctx;
        this.dbMgr = dbMgr;

        this.log = log;
    }

    /** */
    public void addPartPageStore(
        int grpId,
        int partId,
        PageStore pageStore
    ) {
        partPageStoresMap.addPageStore(grpId, partId, pageStore);
    }

    /** */
    public void addMappingPageStore(
        int grpId,
        int partId,
        PageStore pageStore
    ) {
        mappingPageStoresMap.addPageStore(grpId, partId, pageStore);
    }

    /** */
    public IgnitePageStoreManager partPageStoreManager() {
        return new DefragmentationPageStoreManager(ctx, partPageStoresMap);
    }

    /** */
    public IgnitePageStoreManager mappingPageStoreManager() {
        return new DefragmentationPageStoreManager(ctx, mappingPageStoresMap);
    }

    public File workDirForGroupId(int grpId) {
        return cacheWorkDirsByGroupId.get(grpId);
    }

    public GridSpinBusyLock busyLock() {
        return busyLock;
    }

    public int[] groupIdsForDefragmentation() {
        int[] grpIds = partitionsByGroupId.keys();

        Arrays.sort(grpIds);

        return grpIds;
    }

    public int[] partitionsForGroupId(int grpId) {
        IntSet partitions = partitionsByGroupId.get(grpId);

        return partitions == null ? null : partitions.toIntArray();
    }

    public CacheGroupContext groupContextByGroupId(int grpId) {
        return groupContextsByGroupId.get(grpId);
    }

    /** */
    public PageStore pageStore(int grpId, int partIdx) {
        return pageStoresByGrpId.get(grpId).get(partIdx);
    }

    public void onPageStoreCreated(int grpId, File cacheWorkDir, int partIdx, PageStore partStore) {
        cacheWorkDirsByGroupId.putIfAbsent(grpId, cacheWorkDir);

        IntMap<PageStore> pageStores = pageStoresByGrpId.get(grpId);

        if (pageStores == null)
            pageStoresByGrpId.put(grpId, pageStores = new IntHashMap<>());

        pageStores.put(partIdx, partStore);
    }

    public DataRegion partitionsDataRegion() throws IgniteCheckedException {
        return dbMgr.dataRegion(DEFRAGMENTATION_PART_REGION_NAME);
    }

    public DataRegion mappingDataRegion() throws IgniteCheckedException {
        return dbMgr.dataRegion(DEFRAGMENTATION_MAPPING_REGION_NAME);
    }

    public void onCacheStoreCreated(CacheGroupContext grp, int partIdx, GridSpinBusyLock busyLock) {
        if (!grp.userCache())
            return;

        if (this.busyLock == null)
            this.busyLock = busyLock;

        int grpId = grp.groupId();
        groupContextsByGroupId.putIfAbsent(grpId, grp);

        try {
            if (!grp.shared().pageStore().exists(grpId, partIdx))
                return;

            IntSet partitions = partitionsByGroupId.get(grpId);

            if (partitions == null)
                partitionsByGroupId.put(grpId, partitions = new BitSetIntSet());

            partitions.add(partIdx);
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
