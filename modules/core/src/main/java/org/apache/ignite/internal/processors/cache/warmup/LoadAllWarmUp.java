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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.LoadAllWarmUpConfiguration;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

/**
 * "Load all" warm-up strategy, which loads pages to persistent data region
 * until it reaches {@link DataRegionConfiguration#getMaxSize} with priority
 * to index partitions. Loading occurs sequentially for each cache group.
 */
public class LoadAllWarmUp implements WarmUpStrategy<LoadAllWarmUpConfiguration> {
    /** Logger. */
    private final IgniteLogger log;

    /**
     * Cache group contexts supplier.
     * Since {@link GridCacheProcessor} starts later.
     */
    private final Supplier<Collection<CacheGroupContext>> grpCtxSup;

    /** Stop flag. */
    private volatile boolean stop;

    /**
     * Constructor.
     *
     * @param log Logger.
     * @param grpCtxSup Cache group contexts supplier. Since {@link GridCacheProcessor} starts later.
     */
    public LoadAllWarmUp(IgniteLogger log, Supplier<Collection<CacheGroupContext>> grpCtxSup) {
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

        Map<CacheGroupContext, T2<Integer, Long>> loadDataInfo = loadDataInfo(region);

        if (log.isInfoEnabled()) {
            log.info("Order of cache groups loaded into data region [name=" + region.config().getName()
                + ", partCnt=" + loadDataInfo.values().stream().mapToLong(T2::get1).sum()
                + ", pageCnt=" + loadDataInfo.values().stream().mapToLong(T2::get2).sum()
                + ", availablePageCnt=" + availableLoadPageCount(region) + ", grpNames=" +
                loadDataInfo.keySet().stream().map(CacheGroupContext::cacheOrGroupName).collect(toList()) + ']');
        }

        long allPartCnt = 0;
        long allPageCnt = 0;

        for (Map.Entry<CacheGroupContext, T2<Integer, Long>> e : loadDataInfo.entrySet()) {
            CacheGroupContext grp = e.getKey();

            // Including index partition and its pages.
            final int partCnt = e.getValue().get1();
            final long pageCnt = e.getValue().get2();

            if (log.isInfoEnabled()) {
                log.info("Start warm-up cache group, starting from index [name=" + grp.cacheOrGroupName()
                    + ", partCnt=" + partCnt + ", pageCnt=" + pageCnt + ']');
            }

            PageMemoryEx pageMemEx = (PageMemoryEx)region.pageMemory();

            long grpPageCnt = 0;

            // Starting with index.
            for (int i = -1; i < partCnt - 1; i++, allPartCnt++) {
                long pageId = pageMemEx.partitionMetaPageId(grp.groupId(), (i == -1 ? INDEX_PARTITION : i));
                long pages = grp.shared().pageStore().pages(grp.groupId(), (i == -1 ? INDEX_PARTITION : i));

                for (long j = 0; j < pages && grpPageCnt < pageCnt; j++, pageId++, grpPageCnt++, allPageCnt++) {
                    if (stop) {
                        if (log.isInfoEnabled()) {
                            log.info("Stop warm-up cache group with loading statistics [name="
                                + grp.cacheOrGroupName() + ", partCnt=" + (i + 1) + "partPageCnt=" + j +
                                ", grpPageCnt=" + grpPageCnt + ", allPartCnt=" + allPartCnt + ", allPageCnt=" +
                                allPageCnt + ']');
                        }

                        return;
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
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        stop = true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(LoadAllWarmUp.class, this);
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
     * Calculation of cache groups, count of partitions and pages that can load
     * into data region. Calculation starts and includes an index partition for
     * each group.
     *
     * @param region Data region.
     * @return Mapping: {cacheGrp -> {partCnt, pageCnt}}.
     * @throws IgniteCheckedException – if faild.
     */
    protected Map<CacheGroupContext, T2<Integer, Long>> loadDataInfo(
        DataRegion region
    ) throws IgniteCheckedException {
        // Get cache groups of data region.
        List<CacheGroupContext> regionGrps = grpCtxSup.get().stream()
            .filter(grpCtx -> region.equals(grpCtx.dataRegion())).collect(toList());

        long availableLoadPageCnt = availableLoadPageCount(region);

        // Computing groups, partitions, and pages to load into data region.
        Map<CacheGroupContext, T2<Integer, Long>> loadableGrps = new LinkedHashMap<>();

        for (int i = 0; i < regionGrps.size() && availableLoadPageCnt > 0; i++) {
            CacheGroupContext grp = regionGrps.get(i);

            int partCnt = 0;
            long pageCnt = 0;

            // Index partition in priority.
            for (int j = -1; j < grp.topology().localPartitions().size() && availableLoadPageCnt > 0; j++, partCnt++) {
                int partPageCnt = grp.shared().pageStore().pages(grp.groupId(), (j == -1 ? INDEX_PARTITION : j));

                pageCnt += (availableLoadPageCnt - partPageCnt) >= 0 ? partPageCnt : availableLoadPageCnt;

                availableLoadPageCnt -= partPageCnt;
            }

            loadableGrps.put(grp, new T2<>(partCnt, pageCnt));
        }

        return loadableGrps;
    }
}
