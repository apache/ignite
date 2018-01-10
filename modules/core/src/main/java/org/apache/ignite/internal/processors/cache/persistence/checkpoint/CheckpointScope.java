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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import org.apache.ignite.configuration.CheckpointWriteOrder;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesStripedConcurrentHashSet;

/**
 * Checkpoint scope is the unsorted sets of all dirty pages collections from all regions
 */
public class CheckpointScope {
    /** Expected segments count, sub sets added to each collector would be the same. Based on default segments count. */
    public static final int EXPECTED_SEGMENTS_COUNT = Runtime.getRuntime().availableProcessors() * 4 + 1;

    /** Dirty Pages number from all regions. */
    private int pagesNum = -1;

    /** All dirty pages from all regions. Usually contain 1 element. */
    private List<StripedMultiSetCollector> regionsPages;

    /**
     * Creates scope.
     * @param regions Regions count.
     */
    public CheckpointScope(int regions) {
        regionsPages = new ArrayList<>(regions);
    }

    /**
     * @return buffers with arrays with all checkpoint pages, each buffer may be processed independently.
     */
    public List<FullPageIdsBuffer> toBuffers() {
        List<FullPageIdsBuffer> buffers = new ArrayList<>();
        int totalSize = 0;

        for (StripedMultiSetCollector regions : regionsPages) {
            for (MultiSetCollector next : regions.pagesByBucket.values()) {
                FullPageIdsBuffer buf = mergeSetsForBucket(next);

                totalSize += buf.remaining();

                buffers.add(buf);
            }
        }
        //actual number of pages may decrease here because of pages eviction (evicted page may have been already saved).
        pagesNum = totalSize;

        return buffers;
    }

    /**
     * Creates buffer from multi sets. This method supports decreasing collection size in parallel with method execution.
     *
     * @param collector multi set collector.
     * @return buffer created from umerged sets.
     */
    private FullPageIdsBuffer mergeSetsForBucket(MultiSetCollector collector) {
        Comparator<FullPageId> comp = GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR;

        return collector.isSorted(comp)
            ? FullPageIdsBuffer.createBufferFromSortedCollections(collector.unmergedSets, comp)
            : FullPageIdsBuffer.createBufferFromMultiCollection(collector.unmergedSets);
    }


    /**
     * Adds several striped sets of pages from data region.
     * @param sets dirty pages collections from region (array with sets from all segments).
     */
    public void addDataRegionCpPages(PagesStripedConcurrentHashSet[] sets) {
        StripedMultiSetCollector pagesByBucket = new StripedMultiSetCollector();

        for (PagesStripedConcurrentHashSet setToAdd : sets) {
            for (int idx = 0; idx < PagesStripedConcurrentHashSet.BUCKETS_COUNT; idx++) {
                Set<FullPageId> setForBucket = setToAdd.getOptionalSetForBucket(idx);

                if (setForBucket != null)
                    pagesByBucket.getOrCreateBucket(idx).add(setForBucket);
            }
        }

        regionsPages.add(pagesByBucket);
    }

    /**
     * @return {@code true} if there is checkpoint pages available
     */
    public boolean hasPages() {
        return calcPagesNum() > 0;
    }

    /**
     * @return calculated page's number from all regions and all buckets, actual size may became less because eviction
     * can write pages in parallel with checkpoint.
     */
    private int calcPagesNum() {
        if (pagesNum < 0) {
            int sum = 0;

            for (StripedMultiSetCollector next : regionsPages) {
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

    /**
     * Class for separate collection pages from corresponding bucket (stripe). Class is not thread safe.
     */
    private static class StripedMultiSetCollector {
        /**
         * Maps bucket (stripe) index -> multi set collector for this bucket.
         */
        private Map<Integer, MultiSetCollector> pagesByBucket = new HashMap<>(PagesStripedConcurrentHashSet.BUCKETS_COUNT);

        /**
         * @return overall size of contained sets for all buckets.
         */
        public int size() {
            int size = 0;

            for (MultiSetCollector next : pagesByBucket.values()) {
                size += next.size();
            }

            return size;
        }

        /**
         * @param bucketIdx Bucket index.
         * @return collector of sets for bucket.
         */
        MultiSetCollector getOrCreateBucket(int bucketIdx) {
            MultiSetCollector collector = pagesByBucket.get(bucketIdx);

            if (collector == null) {
                collector = new MultiSetCollector();

                pagesByBucket.put(bucketIdx, collector);
            }
            return collector;
        }
    }

    /**
     * Class to collect several sets for same bucket (stripe), avoiding collection data copy. Class is not thread safe.
     */
    private static class MultiSetCollector {
        /**
         * Not merged sets for same bucket, but from different segments.
         */
        private Collection<Set<FullPageId>> unmergedSets = new ArrayList<>(EXPECTED_SEGMENTS_COUNT);

        /**
         * @return overall size of contained sets.
         */
        public int size() {
            int size = 0;

            for (Set<FullPageId> next : unmergedSets) {
                size += next.size();
            }

            return size;
        }

        /**
         * Appends next set to this collector, this method does not merge sets for performance reasons.
         * @param set data to add
         */
        public void add(Set<FullPageId> set) {
            unmergedSets.add(set);
        }

        /**
         * @return Returns {@code true} if all sets are presorted.
         * @param comparator
         */
        public boolean isSorted(Comparator<FullPageId> comparator) {
            for (Set<FullPageId> next : unmergedSets) {
                if (!(next instanceof SortedSet))
                    return false;

                SortedSet sortedSet = (SortedSet)next;

                if (sortedSet.comparator() != comparator)
                    return false;
            }
            return true;
        }
    }
}
