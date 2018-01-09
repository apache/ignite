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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

/**
 * Special set to store page IDs in buckets based on cache Group ID + partition ID. Pages stored in one file in page
 * store are guaranteed to be placed in one bucket.
 */
public class PagesConcurrentHashSet implements Iterable<Set<FullPageId>> {
    /** Buckets count. */
    public static final int BUCKETS_COUNT = 16;

    /** Buckets array, non null elements contained data for corresponding bucket. */
    private final AtomicReferenceArray<Set<FullPageId>> buckets
        = new AtomicReferenceArray<>(BUCKETS_COUNT);

    /** Use skip list. */
    private boolean useSkipList = false;

    /**
     * Adds the specified element to this set if it is not already present (optional operation).
     *
     * @param id Page ID to add to set.
     * @return {@code true} if add operation was successful.
     */
    public boolean add(FullPageId id) {
        Set<FullPageId> set = getOrCreateSetForBucket(getBucketIndex(id));

        assert set != null : "Inner set was concurrently destroyed";

        return set.add(id);
    }

    /**
     * Removes the specified element from this set if it is present (optional operation).
     *
     * @param id page ID to remove from set.
     * @return {@code true} if remove was successful.
     */
    public boolean remove(FullPageId id) {
        Set<FullPageId> set = getOptionalSetForBucket(getBucketIndex(id));

        return set != null && set.remove(id);
    }

    /**
     * Gets internal underlying set if already exist, or creates new set.
     * @param bucketIdx bucket index, 0-based.
     * @return currently allocated set or null if bucket is empty.
     */
    private Set<FullPageId> getOrCreateSetForBucket(int bucketIdx) {
        Set<FullPageId> set = getOptionalSetForBucket(bucketIdx);

        if (set == null) {
            final Set<FullPageId> update = createNewSet();

            set = buckets.compareAndSet(bucketIdx, null, update) ? update : getOptionalSetForBucket(bucketIdx);
        }
        return set;
    }

    /**
     * @return created new set for bucket data storage.
     */
    private Set<FullPageId> createNewSet() {
        if (useSkipList)
            return new ConcurrentSkipListSet<>(GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR);
        else
            return new GridConcurrentHashSet<>();
    }

    /**
     * @param bucketIdx bucket index, 0-based.
     * @return currently allocated set or null if bucket is empty.
     */
    @Nullable public Set<FullPageId> getOptionalSetForBucket(int bucketIdx) {
        assert bucketIdx >= 0;
        assert bucketIdx < BUCKETS_COUNT;

        return buckets.get(bucketIdx);
    }

    /**
     * Determines bucket for page identifier, based on group ID and partition ID of this page.
     *
     * @param id page ID.
     * @return bucket index, 0-min, {@link #BUCKETS_COUNT}-1 - max
     */
    public static int getBucketIndex(FullPageId id) {
        final int grpId = id.groupId();
        final int partId = PageIdUtils.partId(id.pageId());
        final int hash = grpId + partId * 31;

        return hash % BUCKETS_COUNT;
    }

    /**
     * Returns {@code true} if this set contains the specified element.
     *
     * @param id page ID.
     * @return true if page is contained in set.
     */
    public boolean contains(FullPageId id) {
        Set<FullPageId> set = getOptionalSetForBucket(getBucketIndex(id));

        return set != null && set.contains(id);
    }

    /**
     * @return the number of elements in this set.
     */
    public int size() {
        int sz = 0;

        for (int i = 0; i < BUCKETS_COUNT; i++) {
            final Set<FullPageId> ids = getOptionalSetForBucket(i);

            if (ids == null)
                continue;

            sz += ids.size();
        }
        return sz;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Set<FullPageId>> iterator() {
        return new FullPageIdsIterator();
    }

    /**
     * Utility method to check if some set from {@code pages} sets array contains page.
     * @param pages pages sets array.
     * @param id page to find.
     * @return true if page is contained at least in one set in this array.
     */
    public static boolean contains(PagesConcurrentHashSet[] pages, FullPageId id) {
        for (PagesConcurrentHashSet bucketSet : pages) {
            for (Set<FullPageId> innerSet : bucketSet) {
                if (innerSet.contains(id))
                    return true;
            }
        }
        return false;
    }

    /**
     * @param pages sets array.
     * @return overall size.
     */
    public static int size(PagesConcurrentHashSet[] pages) {
        int size = 0;

        for (PagesConcurrentHashSet bucketSet : pages) {
            for (Set<FullPageId> innerSet : bucketSet) {
                size += innerSet.size();
            }
        }

        return size;
    }

    /**
     * Iterator over current local partitions.
     */
    private class FullPageIdsIterator implements Iterator<Set<FullPageId>> {
        /** Next index. */
        private int nextIdx;

        /** Next set of pages. */
        private Set<FullPageId> nextSet;

        /**
         * Creates iterator.
         */
        private FullPageIdsIterator() {
            advance();
        }

        /**
         * Try to advance to next bucket.
         */
        private void advance() {
            while (nextIdx < buckets.length()) {
                Set<FullPageId> part = getOptionalSetForBucket(nextIdx);

                if (part != null) {
                    nextSet = part;
                    return;
                }

                nextIdx++;
            }
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return nextSet != null;
        }

        /** {@inheritDoc} */
        @Override public Set<FullPageId> next() {
            if (nextSet == null)
                throw new NoSuchElementException();

            Set<FullPageId> retVal = nextSet;

            nextSet = null;
            nextIdx++;

            advance();

            return retVal;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }
}
