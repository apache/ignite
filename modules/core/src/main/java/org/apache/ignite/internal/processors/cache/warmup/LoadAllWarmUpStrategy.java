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

package org.apache.ignite.internal.processors.cache.warmup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.LoadAllWarmUpConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.stream.Collectors.averagingInt;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.util.IgniteUtils.doInParallel;

/**
 * "Load all" warm-up strategy, which loads pages to persistent data region
 * until it reaches {@link DataRegionConfiguration#getMaxSize} with priority
 * to index partitions. Loading occurs sequentially for each cache group,
 * starting with index partition, and then all others in ascending order.
 */
public class LoadAllWarmUpStrategy implements WarmUpStrategy<LoadAllWarmUpConfiguration> {
    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /**
     * Cache group contexts supplier.
     * Since {@link GridCacheProcessor} starts later.
     */
    @GridToStringExclude
    private final Supplier<Collection<CacheGroupContext>> grpCtxSup;

    /** Stop flag. */
    private volatile boolean stop;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param grpCtxSup Cache group contexts supplier. Since {@link GridCacheProcessor} starts later.
     */
    public LoadAllWarmUpStrategy(IgniteLogger log, Supplier<Collection<CacheGroupContext>> grpCtxSup) {
        this.log = log;
        this.grpCtxSup = grpCtxSup;
    }

    /** {@inheritDoc} */
    @Override public Class<LoadAllWarmUpConfiguration> configClass() {
        return LoadAllWarmUpConfiguration.class;
    }

    /** {@inheritDoc} */
    @Override public void warmUp(
        LoadAllWarmUpConfiguration cfg,
        DataRegion region
    ) throws IgniteCheckedException {
        if (stop)
            return;

        assert region.config().isPersistenceEnabled();

        Map<CacheGroupContext, List<LoadPartition>> loadDataInfo = loadDataInfo(region);

        long availableLoadPageCnt = availableLoadPageCount(region);

        if (log.isInfoEnabled()) {
            Collection<List<LoadPartition>> parts = loadDataInfo.values();

            log.info("Order of cache groups loaded into data region [name=" + region.config().getName()
                + ", partCnt=" + parts.stream().mapToLong(Collection::size).sum()
                + ", pageCnt=" + parts.stream().flatMap(Collection::stream).mapToLong(LoadPartition::pages).sum()
                + ", availablePageCnt=" + availableLoadPageCnt + ", grpNames=" +
                loadDataInfo.keySet().stream().map(CacheGroupContext::cacheOrGroupName).collect(toList()) + ']');
        }

        AtomicLong loadedPageCnt = new AtomicLong();

        for (Map.Entry<CacheGroupContext, List<LoadPartition>> e : loadDataInfo.entrySet()) {
            CacheGroupContext grp = e.getKey();
            List<LoadPartition> parts = e.getValue();

            LoadPartition idxPart = parts.get(0);

            int avgPartPagesCnt = parts.stream()
                .filter(p -> p.part() != INDEX_PARTITION)
                .map(LoadPartition::pages)
                .collect(averagingInt(i -> i)).intValue();

            if (avgPartPagesCnt != 0 && idxPart.pages() > avgPartPagesCnt) {
                List<LoadPartition> idxParts = new ArrayList<>(idxPart.pages() / avgPartPagesCnt + 1);

                for (int i = 0; i < idxPart.pages(); i += avgPartPagesCnt)
                    idxParts.add(new LoadPartition(idxPart.part(), Math.min(idxPart.pages() - i, avgPartPagesCnt), i));

                parts.remove(0);

                idxParts.addAll(parts);

                parts = idxParts;
            }

            if (log.isInfoEnabled()) {
                log.info("Start warm-up cache group, with estimated statistics [name=" + grp.cacheOrGroupName()
                    + ", partCnt=" + parts.size() + ", pageCnt="
                    + parts.stream().mapToLong(LoadPartition::pages).sum() + ']');
            }

            PageMemoryEx pageMemEx = (PageMemoryEx)region.pageMemory();

            GridKernalContext ctx = grp.shared().kernalContext();

            doInParallel(
                U.availableThreadCount(ctx, GridIoPolicy.SYSTEM_POOL, 2),
                ctx.pools().getSystemExecutorService(),
                parts,
                part -> {
                    long pageId = pageMemEx.partitionMetaPageId(grp.groupId(), part.part());

                    pageId += part.startPageIdx();

                    for (int i = 0; i < part.pages(); i++, pageId++, loadedPageCnt.incrementAndGet()) {
                        if (stop) {
                            if (log.isInfoEnabled()) {
                                log.info("Stop warm-up cache group with loaded statistics [name="
                                    + grp.cacheOrGroupName() + ", pageCnt=" + loadedPageCnt.get()
                                    + ", remainingPageCnt=" + (availableLoadPageCnt - loadedPageCnt.get()) + ']');
                            }

                            return null;
                        }

                        long pagePtr = -1;

                        try {
                            pagePtr = pageMemEx.acquirePage(grp.groupId(), pageId);
                        }
                        finally {
                            if (pagePtr != -1)
                                pageMemEx.releasePage(grp.groupId(), pageId, pagePtr);
                        }
                    }

                    return null;
                }
            );
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        stop = true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LoadAllWarmUpStrategy.class, this);
    }

    /**
     * Getting count of pages available for loading into data region.
     *
     * @param region Data region.
     * @return Count(non-negative) of pages available for loading into data region.
     */
    protected long availableLoadPageCount(DataRegion region) {
        long maxSize = region.config().getMaxSize();
        long curSize = region.pageMemory().loadedPages() * region.pageMemory().systemPageSize();

        return Math.max(0, (maxSize - curSize) / region.pageMemory().systemPageSize());
    }

    /**
     * Calculation of cache groups, partitions and count of pages that can load
     * into data region. Calculation starts and includes an index partition for
     * each group.
     *
     * @param region Data region.
     * @return Loadable groups and partitions.
     * @throws IgniteCheckedException – if faild.
     */
    protected Map<CacheGroupContext, List<LoadPartition>> loadDataInfo(
        DataRegion region
    ) throws IgniteCheckedException {
        // Get cache groups of data region.
        List<CacheGroupContext> regionGrps = grpCtxSup.get().stream()
            .filter(grpCtx -> region.equals(grpCtx.dataRegion())).collect(toList());

        long availableLoadPageCnt = availableLoadPageCount(region);

        // Computing groups, partitions, and pages to load into data region.
        Map<CacheGroupContext, List<LoadPartition>> loadableGrps = new LinkedHashMap<>();

        for (int i = 0; i < regionGrps.size() && availableLoadPageCnt > 0; i++) {
            CacheGroupContext grp = regionGrps.get(i);

            // Index partition in priority.
            List<GridDhtLocalPartition> locParts = grp.topology().localPartitions();

            for (int j = -1; j < locParts.size() && availableLoadPageCnt > 0; j++) {
                int p = j == -1 ? INDEX_PARTITION : locParts.get(j).id();

                int partPageCnt = grp.shared().pageStore().pages(grp.groupId(), p);

                if (partPageCnt > 0) {
                    int pageCnt = (availableLoadPageCnt - partPageCnt) >= 0 ? partPageCnt : (int)availableLoadPageCnt;

                    availableLoadPageCnt -= pageCnt;

                    loadableGrps.computeIfAbsent(grp, grpCtx -> new ArrayList<>()).add(new LoadPartition(p, pageCnt, 0));
                }
            }
        }

        return loadableGrps;
    }

    /**
     * Information about loaded partition.
     */
    static class LoadPartition {
        /** Partition id. */
        private final int part;

        /** Number of pages to load. */
        private final int pages;

        /** Index of first page to load. */
        private final int startPageIdx;

        /**
         * Constructor.
         *
         * @param part Partition id.
         * @param pages Number of pages to load.
         */
        public LoadPartition(int part, int pages, int startPageIdx) {
            assert part >= 0 : "Partition id cannot be negative.";
            assert pages > 0 : "Number of pages to load must be greater than zero.";

            this.part = part;
            this.pages = pages;
            this.startPageIdx = startPageIdx;
        }

        /**
         * Return partition id.
         *
         * @return Partition id.
         */
        public int part() {
            return part;
        }

        /**
         * Return number of pages to load.
         *
         * @return Number of pages to load.
         */
        public int pages() {
            return pages;
        }

        /**
         * Return index of first page to load.
         *
         * @return Index of first page to load.
         */
        public int startPageIdx() {
            return startPageIdx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LoadPartition.class, this);
        }
    }
}
