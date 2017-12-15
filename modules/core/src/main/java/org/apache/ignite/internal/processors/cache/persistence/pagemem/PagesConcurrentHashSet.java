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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.util.GridConcurrentHashSet;

/**
 *
 */
public class PagesConcurrentHashSet implements Iterable<GridConcurrentHashSet<FullPageId>> {
    private final int BUCKETS_COUNT = 16;

    private final AtomicReferenceArray<GridConcurrentHashSet<FullPageId>> buckets
        = new AtomicReferenceArray<>(BUCKETS_COUNT);

    public boolean add(FullPageId id) {
        GridConcurrentHashSet<FullPageId> set = getOrCreateSet(id);

        assert set != null : "Inner set was concurrently destroyed";

        return set.add(id);
    }

    public boolean remove(FullPageId id) {

        GridConcurrentHashSet<FullPageId> set = buckets.get(getBucketNumber(id));

        return set != null && set.remove(id);

    }

    private GridConcurrentHashSet<FullPageId> getOrCreateSet(FullPageId id) {
        return getSetForBucket(getBucketNumber(id));
    }

    private GridConcurrentHashSet<FullPageId> getSetForBucket(int bucket) {
        GridConcurrentHashSet<FullPageId> set = buckets.get(bucket);

        if (set == null) {
            final GridConcurrentHashSet<FullPageId> update = new GridConcurrentHashSet<>();

            set = buckets.compareAndSet(bucket, null, update) ? update : buckets.get(bucket);
        }

        return set;
    }

    private int getBucketNumber(FullPageId id) {
        final int grpId = id.groupId();
        final int partId = PageIdUtils.partId(id.pageId());
        final int hash = grpId + partId * 31;
        return hash % BUCKETS_COUNT;
    }

    public boolean contains(FullPageId id) {
        GridConcurrentHashSet<FullPageId> set = buckets.get(getBucketNumber(id));
        return set != null && set.contains(id);

    }

    public int size() {
        int sz = 0;
        for (int i = 0; i < BUCKETS_COUNT; i++) {
            final GridConcurrentHashSet<FullPageId> ids = buckets.get(i);
            if (ids == null)
                continue;

            sz += ids.size();
        }
        return sz;
    }

    /** {@inheritDoc} */
    @Override public Iterator<GridConcurrentHashSet<FullPageId>> iterator() {
        return new FullPageIdsIterator();
    }

    public static boolean contains(PagesConcurrentHashSet[] pages, FullPageId id) {
        for (PagesConcurrentHashSet bucketSet : pages) {
            for (GridConcurrentHashSet<FullPageId> innerSet : bucketSet) {
                if (innerSet.contains(id))
                    return false;
            }
        }
        return true;
    }

    public static int size(PagesConcurrentHashSet[] pages) {
        int size = 0;
        for (PagesConcurrentHashSet bucketSet : pages) {
            for (GridConcurrentHashSet<FullPageId> innerSet : bucketSet) {
                size += innerSet.size();
            }
        }
        return size;
    }



    /**
     * Iterator over current local partitions.
     */
    private class FullPageIdsIterator implements Iterator<GridConcurrentHashSet<FullPageId>> {
        /** Next index. */
        private int nextIdx;

        /** Next set of pages. */
        private GridConcurrentHashSet<FullPageId> nextSet;

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
                GridConcurrentHashSet<FullPageId> part = buckets.get(nextIdx);

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
        @Override public GridConcurrentHashSet<FullPageId> next() {
            if (nextSet == null)
                throw new NoSuchElementException();

            GridConcurrentHashSet<FullPageId> retVal = nextSet;

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
