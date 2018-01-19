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

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Special set to store page IDs in buckets based on cache Group ID + partition ID. Pages stored in one file in page
 * store are guaranteed to be placed in one bucket. This allows to process each bucket (0..15) separately in different
 * threads at stage of sorting.
 *
 * Inner set for buckets in this implementation is not sorted (hash) set.
 */
public class PagesStripedConcurrentHashSet extends AbstractSet<FullPageId> implements PageIdCollection {
    /** Buckets count. */
    public static final int BUCKETS_COUNT = 16;

    /** Buckets array, non null elements contained data for corresponding bucket. */
    private final AtomicReferenceArray<Set<FullPageId>> buckets
        = new AtomicReferenceArray<>(BUCKETS_COUNT);

    /**
     * Adds the specified element to this set if it is not already present (optional operation).
     *
     * @param id Page ID to add to set.
     * @return {@code true} if add operation was successful.
     */
    @Override public boolean add(FullPageId id) {
        assert id != null : "Null elements not supported";
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

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        return o instanceof FullPageId && remove((FullPageId)o);
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
     * @return created new inner set for bucket (stripe) data storage.
     */
    protected Set<FullPageId> createNewSet() {
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

        return U.safeAbs(hash) % BUCKETS_COUNT;
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

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        return o instanceof FullPageId && contains((FullPageId)o);
    }

    /**
     * @return the number of elements in this set.
     */
    @Override public int size() {
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
    @Override public Iterator<FullPageId> iterator() {
        return new FullPageIdsIterator();
    }

    /**
     * Utility method to check if some set from {@code pages} sets array contains page.
     * @param pages pages sets array.
     * @param id page to find.
     * @return true if page is contained at least in one set in this array.
     */
    public static boolean contains(PageIdCollection[] pages, FullPageId id) {
        for (PageIdCollection bucketSet : pages) {
            if (bucketSet.contains(id))
                return true;
        }
        return false;
    }

    /**
     * @param pages sets array.
     * @return overall size.
     */
    public static int size(PageIdCollection[] pages) {
        int size = 0;

        for (PageIdCollection bucketSet : pages) {
            size += bucketSet.size();
        }

        return size;
    }

    /**
     * Iterator over all sets in buckets. Not created (empty) sets are skipped.
     */
    private class FullPageIdsIterator implements Iterator<FullPageId> {
        /** Next set index. */
        private int nextSetIdx;

        /** Next set of pages iterator. */
        private Iterator<FullPageId> curSetIter;

        /** Next page peeked. */
        private FullPageId nextElement;

        /**
         * Creates iterator.
         */
        private FullPageIdsIterator() {
            advanceSet();

            if (curSetIter != null)
                advance();
        }

        /**
         * Switch to next element of iterator
         */
        private void advance() {
            while (true) {
                nextElement = curSetIter.hasNext() ? curSetIter.next() : null;

                if (nextElement != null)
                    return;
                else {
                    advanceSet();

                    if (curSetIter == null)
                        return;
                }
            }
        }
        /**
         * Try to advance to next bucket.
         */
        private void advanceSet() {
            curSetIter = null;
            while (nextSetIdx < buckets.length()) {
                Set<FullPageId> bucket = getOptionalSetForBucket(nextSetIdx);

                nextSetIdx++;
                if (bucket != null) {
                    curSetIter = bucket.iterator();

                    return;
                }
            }
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return nextElement != null;
        }

        /** {@inheritDoc} */
        @Override public FullPageId next() {
            if (nextElement == null)
                throw new NoSuchElementException();

            FullPageId retVal = nextElement;

            advance();

            return retVal;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }
}
