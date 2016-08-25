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

package org.apache.ignite.internal.processors.cache.database.freelist;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 */
public final class FreeListNew extends PagesList implements FreeList, ReuseList {
    /** */
    private static final int BUCKETS = 256; // Must be power of 2.

    /** */
    private static final int REUSE_BUCKET = BUCKETS - 1; // TODO or 0?

    /** */
    private final int shift;

    /** */
    private final AtomicReferenceArray<long[]> buckets = new AtomicReferenceArray<>(BUCKETS);

    /** */
    private final AtomicLongArray bitmap = new AtomicLongArray(BUCKETS / 64);

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param reuseList Reuse list or {@code null} if this free list will be a reuse list for itself.
     * @param wal Write ahead log.
     */
    public FreeListNew(int cacheId, PageMemory pageMem, ReuseList reuseList, IgniteWriteAheadLogManager wal) {
        super(cacheId, pageMem, wal);

        this.reuseList = reuseList == null ? this : reuseList;

        int pageSize = pageMem.pageSize();

        assert U.isPow2(pageSize);
        assert U.isPow2(BUCKETS);
        assert BUCKETS <= pageSize;

        int shift = 0;

        while (pageSize > BUCKETS) {
            shift++;
            pageSize >>>= 1;
        }

        this.shift = shift;
    }

    private int bucket(int freeSpace) {
        assert freeSpace > 0: freeSpace;

        return freeSpace >>> shift;
    }

    /** {@inheritDoc} */
    @Override public void insertDataRow(CacheDataRow row) throws IgniteCheckedException {
        // TODO port from old impl + use methods `put` and `removeDataPage` instead of trees
    }

    /** {@inheritDoc} */
    @Override public void removeDataRowByLink(long link) throws IgniteCheckedException {
        // TODO port from old impl + use methods `put` and `removeDataPage` instead of trees
    }

    /** {@inheritDoc} */
    @Override protected long[] getBucket(int bucket) {
        return buckets.get(bucket);
    }

    /** {@inheritDoc} */
    @Override protected boolean casBucket(int bucket, long[] exp, long[] upd) {
        return buckets.compareAndSet(bucket, exp, upd);
    }

    /** {@inheritDoc} */
    @Override protected boolean isReuseBucket(int bucket) {
        return bucket == REUSE_BUCKET && reuseList == this;
    }

    /** {@inheritDoc} */
    @Override public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {
        assert reuseList == this: "not allowed to be a reuse list";

        put(bag, null, 0);
    }

    /** {@inheritDoc} */
    @Override public long takeRecycledPage(DataStructure client, ReuseBag bag) throws IgniteCheckedException {
        assert reuseList == this: "not allowed to be a reuse list";

        if (bag.pollFreePage() != 0L) // TODO drop client and bag from the signature
            throw new IllegalStateException();

        return takeEmptyPage(0);
    }

    /** {@inheritDoc} */
    @Override public long recycledPagesCount() throws IgniteCheckedException {
        assert reuseList == this: "not allowed to be a reuse list";

        return storedPagesCount(REUSE_BUCKET);
    }
}
