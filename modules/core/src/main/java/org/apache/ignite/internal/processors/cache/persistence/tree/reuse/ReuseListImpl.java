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

package org.apache.ignite.internal.processors.cache.persistence.tree.reuse;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;

/**
 * Reuse list.
 */
public class ReuseListImpl extends PagesList implements ReuseList {
    /** */
    private static final AtomicReferenceFieldUpdater<ReuseListImpl, Stripe[]> bucketUpdater =
        AtomicReferenceFieldUpdater.newUpdater(ReuseListImpl.class, Stripe[].class, "bucket");

    /** */
    private volatile Stripe[] bucket;

    /** Onheap pages cache. */
    private final PagesCache bucketCache;

    /**
     * @param cacheId   Cache ID.
     * @param name Name (for debug purpose).
     * @param pageMem   Page memory.
     * @param wal       Write ahead log manager.
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new metadata should be initialized.
     * @throws IgniteCheckedException If failed.
     */
    public ReuseListImpl(
        int cacheId,
        String name,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        long metaPageId,
        boolean initNew,
        PageLockListener lockLsnr,
        GridKernalContext ctx,
        AtomicLong pageListCacheLimit
    ) throws IgniteCheckedException {
        super(
            cacheId,
            name,
            pageMem,
            1,
            wal,
            metaPageId,
            lockLsnr,
            ctx
        );

        bucketCache = new PagesCache(pageListCacheLimit);

        reuseList = this;

        init(metaPageId, initNew);
    }

    /** {@inheritDoc} */
    @Override protected boolean isReuseBucket(int bucket) {
        assert bucket == 0 : bucket;

        return true;
    }

    /** {@inheritDoc} */
    @Override public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {
        put(bag, 0, 0, 0, 0, IoStatisticsHolderNoOp.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override public long takeRecycledPage() throws IgniteCheckedException {
        return takeEmptyPage(0, null, IoStatisticsHolderNoOp.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override public long recycledPagesCount() throws IgniteCheckedException {
        return storedPagesCount(0);
    }

    /** {@inheritDoc} */
    @Override protected Stripe[] getBucket(int bucket) {
        return this.bucket;
    }

    /** {@inheritDoc} */
    @Override protected int getBucketIndex(int freeSpace) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected boolean casBucket(int bucket, Stripe[] exp, Stripe[] upd) {
        return bucketUpdater.compareAndSet(this, exp, upd);
    }

    /** {@inheritDoc} */
    @Override protected PagesCache getBucketCache(int bucket, boolean create) {
        return bucketCache;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ReuseList [name=" + name + ']';
    }
}
