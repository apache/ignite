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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.configuration.CheckpointWriteOrder;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;

/**
 * Checkpoint scope is the unsorted sets of all dirty pages collections from all regions
 */
public class CheckpointScope {
    /** Dirty Pages number from all regions. */
    private int pagesNum = 0;

    /** Pages collections. */
    private Collection<GridMultiCollectionWrapper<FullPageId>> pages;

    /**
     * @param regions Regions count.
     */
    public CheckpointScope(int regions) {
        pages = new ArrayList<>(regions);
    }

    /**
     * @return global array with all checkpoint pages
     */
    public FullPageId[] toArray() {
        FullPageId[] pageIds = new FullPageId[pagesNum];

        int idx = 0;

        for (GridMultiCollectionWrapper<FullPageId> col : pages) {
            for (int i = 0; i < col.collectionsSize(); i++) {
                for (FullPageId next : col.innerCollection(i)) {
                    assert next != null;

                    pageIds[idx] = next;

                    idx++;
                }
            }
        }
        assert idx == pagesNum;

        return pageIds;
    }

    /**
     * Splits pages to {@code pagesSubLists} subtasks. If any thread will be faster, it will help slower threads.
     *
     * @param pageIds full pages collection.
     * @param pagesSubArrays required subArraysCount.
     * @return full page arrays to be processed as standalone tasks.
     */
    public static Collection<FullPageId[]> split(FullPageId[] pageIds, int pagesSubArrays) {
        final Collection<FullPageId[]> res = new ArrayList<>();

        if (pagesSubArrays == 1) {
            res.add(pageIds);

            return res;
        }

        final int totalSize = pageIds.length;

        for (int i = 0; i < pagesSubArrays; i++) {
            int from = totalSize * i / (pagesSubArrays);

            int to = totalSize * (i + 1) / (pagesSubArrays);

            res.add(Arrays.copyOfRange(pageIds, from, to));
        }
        return res;
    }

    /**
     * @param nextCpPagesCol next cp pages collection from region
     */
    public void addCpPages(GridMultiCollectionWrapper<FullPageId> nextCpPagesCol) {
        pagesNum += nextCpPagesCol.size();
        pages.add(nextCpPagesCol);
    }

    /**
     * @return {@code true} if there is checkpoint pages available
     */
    public boolean hasPages() {
        return pagesNum > 0;
    }

    /**
     * @return total checkpoint pages from all caches and regions
     */
    public int totalCpPages() {
        return pagesNum;
    }

    /**
     * Reorders list of checkpoint pages and splits them into needed number of sublists according to
     * {@link DataStorageConfiguration#getCheckpointThreads()} and
     * {@link DataStorageConfiguration#getCheckpointWriteOrder()}.
     *
     * @param persistenceCfg persistent configuration.
     */
    public Collection<FullPageId[]> splitAndSortCpPagesIfNeeded(DataStorageConfiguration persistenceCfg) {
        final FullPageId[] cpPagesArr = toArray();

        if (persistenceCfg.getCheckpointWriteOrder() == CheckpointWriteOrder.SEQUENTIAL)
            Arrays.sort(cpPagesArr, GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR);

        int cpThreads = persistenceCfg.getCheckpointThreads();

        int pagesSubLists = cpThreads == 1 ? 1 : cpThreads * 4;

        return split(cpPagesArr, pagesSubLists);
    }
}
