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

package org.apache.ignite.internal.processors.cache.database.tree.reuse;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.freelist.PagesList;

/**
 * Reuse list.
 */
public final class ReuseListNew extends PagesList implements ReuseList {
    /** */
    private static final AtomicReferenceFieldUpdater<ReuseListNew, long[]> bucketUpdater =
        AtomicReferenceFieldUpdater.newUpdater(ReuseListNew.class, long[].class, "bucket");

    /** */
    private volatile long[] bucket;

    /**
     * @param cacheId   Cache ID.
     * @param pageMem   Page memory.
     * @param wal       Write ahead log manager.
     */
    public ReuseListNew(int cacheId, PageMemory pageMem, IgniteWriteAheadLogManager wal) {
        super(cacheId, pageMem, wal);

        reuseList = this;
    }

    /** {@inheritDoc} */
    @Override protected boolean isReuseBucket(int bucket) {
        assert bucket == 0: bucket;

        return true;
    }

    /** {@inheritDoc} */
    @Override public void addForRecycle(ReuseBag bag) throws IgniteCheckedException {
        put(bag, null, 0);
    }

    /** {@inheritDoc} */
    @Override public long takeRecycledPage(DataStructure client, ReuseBag bag) throws IgniteCheckedException {
//        if (bag.pollFreePage() != 0L) // TODO drop client and bag from the signature
//            throw new IllegalStateException();

        return takeEmptyPage(0, null);
    }

    /** {@inheritDoc} */
    @Override public long recycledPagesCount() throws IgniteCheckedException {
        return storedPagesCount(0);
    }

    /** {@inheritDoc} */
    @Override protected long[] getBucket(int bucket) {
        return this.bucket;
    }

    /** {@inheritDoc} */
    @Override protected boolean casBucket(int bucket, long[] exp, long[] upd) {
        return bucketUpdater.compareAndSet(this, exp, upd);
    }
}
