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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.CheckpointWriteOrder;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesConcurrentHashSet;

/**
 * Checkpoint scope is the unsorted sets of all dirty pages collections from all regions
 */
public class CheckpointScope {
    public static final int EXPECTED_SEGMENTS_AND_SUB_SETS = Runtime.getRuntime().availableProcessors() * 4 + 1;
    /** Dirty Pages number from all regions. */
    private int pagesNum = -1;

    private List<BucketEnabledCollector> regionsPages;

    static class BucketEnabledCollector {
        /**
         * Map bucket -> multiset in this bucket
         */
        private Map<Integer, MultiSetCollector> pagesByBucket = new HashMap<>(PagesConcurrentHashSet.BUCKETS_COUNT);

        public int size() {
            int size = 0;

            for (MultiSetCollector next : pagesByBucket.values()) {
                size += next.size();
            }

            return size;
        }

        public MultiSetCollector bucket(int idx) {
            MultiSetCollector collector = pagesByBucket.get(idx);

            if (collector == null) {
                collector = new MultiSetCollector();

                pagesByBucket.put(idx, collector);
            }
            return collector;
        }
    }

    static class MultiSetCollector {
        /**
         * Not merged sets for same bucket, but from different segments.
         */
        List<Set<FullPageId>> unmergedSets = new ArrayList<>(EXPECTED_SEGMENTS_AND_SUB_SETS);

        public int size() {
            int size = 0;
            for (Set<FullPageId> next : unmergedSets) {
                size += next.size();
            }
            return size;
        }

        public void add(Set<FullPageId> bucket) {
            // dont merge sets here for performance reasons
            unmergedSets.add(bucket);
        }
    }

    /**
     * @param regions Regions count.
     */
    public CheckpointScope(int regions) {
        regionsPages = new ArrayList<>(regions);
    }

    /**
     * @return buffers wiht arrays with all checkpoint pages, may be processed independently
     */
    public List<FullPageIdsBuffer> toBuffers() {
        List<FullPageIdsBuffer> buffers = new ArrayList<>();

        int globalIdx = 0;
        for (BucketEnabledCollector regions : regionsPages) {
            Set<Map.Entry<Integer, MultiSetCollector>> entries = regions.pagesByBucket.entrySet();
            for (Map.Entry<Integer, MultiSetCollector> next : entries) {
                Integer bucket = next.getKey();

                MultiSetCollector value = next.getValue();

                int localIdx = 0;
                FullPageId[] independentPageId = new FullPageId[value.size()];

                for (Set<FullPageId> set : value.unmergedSets) {
                    //here may check if set is navigatable
                    for (FullPageId id : set) {
                        independentPageId[localIdx] = id;
                        localIdx++;

                        globalIdx++;

                        assert bucket.equals(PagesConcurrentHashSet.getBucketIndex(id));
                    }
                }

                //actual number of pages may be less
                buffers.add(new FullPageIdsBuffer(independentPageId, 0, localIdx));

            }
        }
        //actual number of pages may decrease here because of pages eviction (evicted page may have been already saved).
        pagesNum = globalIdx;

        return buffers;
    }

    /**
     * @param sets next dirty pages collections from region (array from all segments)
     */
    public void addDataRegionCpPages(PagesConcurrentHashSet[] sets) {
        BucketEnabledCollector pagesByBucket = new BucketEnabledCollector();

        for (PagesConcurrentHashSet setToAdd : sets) {
            for (int idx = 0; idx < PagesConcurrentHashSet.BUCKETS_COUNT; idx++) {

                Set<FullPageId> setForBucket = setToAdd.getOptionalSetForBucket(idx);
                if (setForBucket != null)
                    pagesByBucket.bucket(idx).add(setForBucket);
            }
        }

        regionsPages.add(pagesByBucket);
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
     * @return {@code true} if there is checkpoint pages available
     */
    public boolean hasPages() {
        return calcPagesNum() > 0;
    }

    private int calcPagesNum() {
        if (pagesNum < 0) {
            int sum = 0;

            for (BucketEnabledCollector next : regionsPages) {
                sum += next.size();
            }

            pagesNum = sum;
        }

        return pagesNum;
    }

    /**
     * @return total checkpoint pages from all caches and regions
     */
    public int totalCpPages() {
        return calcPagesNum();
    }

    /**
     * Reorders list of checkpoint pages and splits them into needed number of sublists according to
     * {@link DataStorageConfiguration#getCheckpointThreads()} and
     * {@link DataStorageConfiguration#getCheckpointWriteOrder()}.
     *
     * @param persistenceCfg persistent configuration.
     */
    public Collection<FullPageIdsBuffer> splitAndSortCpPagesIfNeeded(DataStorageConfiguration persistenceCfg) {
        final List<FullPageIdsBuffer> cpPagesBuf = toBuffers();
        final Collection<FullPageIdsBuffer> allBuffers = new ArrayList<>();

        for (FullPageIdsBuffer next : cpPagesBuf) {
            if (persistenceCfg.getCheckpointWriteOrder() == CheckpointWriteOrder.SEQUENTIAL)
                next.sort(GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR);

            int cpThreads = persistenceCfg.getCheckpointThreads();

            allBuffers.addAll(next.split(cpThreads));
        }

        return allBuffers;
    }

}
