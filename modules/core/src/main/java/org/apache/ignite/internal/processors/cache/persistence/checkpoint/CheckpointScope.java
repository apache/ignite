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
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.jetbrains.annotations.NotNull;

/**
 * Checkpoint scope is the unsorted sets of all dirty pages collections from all regions
 */
public class CheckpointScope {
    /** Dirty Pages number from all regions. */
    private int pagesNum = 0;

    private Collection<GridMultiCollectionWrapper<FullPageId>> res;

    public CheckpointScope(int regions) {
        res = new ArrayList<>(regions);
    }

    /**
     * @return global array with all checkpoint pages
     */
    public FullPageId[] toArray() {
        FullPageId[] pageIds = new FullPageId[pagesNum];
        int idx = 0;
        for (GridMultiCollectionWrapper<FullPageId> col : res) {
            for (int i = 0; i < col.collectionsSize(); i++) {
                for (FullPageId next : col.innerCollection(i)) {
                    pageIds[idx] = next;
                    idx++;
                }
            }
        }
        return pageIds;
    }

    @NotNull
    public static GridMultiCollectionWrapper<FullPageId> split( FullPageId[] pageIds, int pagesSubLists) {
        // Splitting pages to (threads * 4) subtasks. If any thread will be faster, it will help slower threads.

        if(pagesSubLists==1)
            return new GridMultiCollectionWrapper<>(Arrays.asList(pageIds));

        final int totalSize = pageIds.length;
        Collection[] pagesSubListArr = new Collection[pagesSubLists];

        for (int i = 0; i < pagesSubLists; i++) {
            int from = totalSize * i / (pagesSubLists);

            int to = totalSize * (i + 1) / (pagesSubLists);

            final FullPageId[] ids = Arrays.copyOfRange(pageIds, from, to);
            pagesSubListArr[i] = Arrays.asList(ids);
        }

        return new GridMultiCollectionWrapper<FullPageId>(pagesSubListArr);
    }

    /**
     * @param nextCpPagesCol next cp pages collection from region
     */
    public void addCpPages(GridMultiCollectionWrapper<FullPageId> nextCpPagesCol) {
        pagesNum += nextCpPagesCol.size();
        res.add(nextCpPagesCol);
    }

    /**
     * @return {@code true} if there is checkpoint pages available
     */
    public boolean hasPages() {
        return pagesNum > 0;
    }

    public int totalCpPages() {
        return pagesNum;
    }
}
