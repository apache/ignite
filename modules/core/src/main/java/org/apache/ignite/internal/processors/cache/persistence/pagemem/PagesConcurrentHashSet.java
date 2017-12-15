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
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class PagesConcurrentHashSet implements Iterable<Set<FullPageId>> {
    public static final int BUCKETS_COUNT = 16;

    private final AtomicReferenceArray<GridConcurrentHashSet<FullPageId>> buckets
        = new AtomicReferenceArray<>(BUCKETS_COUNT);

    public boolean add(FullPageId id) {
        Set<FullPageId> set = getOrCreateSet(id);

        assert set != null : "Inner set was concurrently destroyed";

        return set.add(id);
    }

    public boolean remove(FullPageId id) {
        Set<FullPageId> set = getOptionalSetForBucket(getBucketIndex(id));

        return set != null && set.remove(id);

    }

    private Set<FullPageId> getOrCreateSet(FullPageId id) {
        return getSetForBucket(getBucketIndex(id));
    }

    private Set<FullPageId> getSetForBucket(int bucket) {
        Set<FullPageId> set = getOptionalSetForBucket(bucket);

        if (set == null) {
            final GridConcurrentHashSet<FullPageId> update = new GridConcurrentHashSet<>();

            set = buckets.compareAndSet(bucket, null, update) ? update : getOptionalSetForBucket(bucket);
        }

        return set;
    }

    /**
     * @param bucket
     * @return
     */
    @Nullable public GridConcurrentHashSet<FullPageId> getOptionalSetForBucket(int bucket) {
        return buckets.get(bucket);
    }

    public static int getBucketIndex(FullPageId id) {
        final int grpId = id.groupId();
        final int partId = PageIdUtils.partId(id.pageId());
        final int hash = grpId + partId * 31;
        return hash % BUCKETS_COUNT;
    }

    public boolean contains(FullPageId id) {
        GridConcurrentHashSet<FullPageId> set = getOptionalSetForBucket(getBucketIndex(id));

        return set != null && set.contains(id);

    }

    public int size() {
        int sz = 0;
        for (int i = 0; i < BUCKETS_COUNT; i++) {
            final GridConcurrentHashSet<FullPageId> ids = getOptionalSetForBucket(i);
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

    public static boolean contains(PagesConcurrentHashSet[] pages, FullPageId id) {
        for (PagesConcurrentHashSet bucketSet : pages) {
            for (Set<FullPageId> innerSet : bucketSet) {
                if (innerSet.contains(id))
                    return true;
            }
        }
        return false;
    }

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
         * Constructor
         */
        private FullPageIdsIterator() {
            advance();
        }

        /**
         * Try to advance to next partition.
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
