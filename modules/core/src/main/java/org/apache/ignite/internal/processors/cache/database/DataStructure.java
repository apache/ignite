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

package org.apache.ignite.internal.processors.cache.database;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;

/**
 * Base class for all the data structures based on {@link PageMemory}.
 */
public abstract class DataStructure {
    /** For tests. */
    public static Random rnd;

    /** */
    protected final int cacheId;

    /** */
    protected final PageMemory pageMem;

    /** */
    protected final IgniteWriteAheadLogManager wal;

    /** */
    protected ReuseList reuseList;

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     */
    public DataStructure(
        int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal
    ) {
        assert pageMem != null;

        this.cacheId = cacheId;
        this.pageMem = pageMem;
        this.wal = wal;
    }

    /**
     * @return Cache ID.
     */
    public final int getCacheId() {
        return cacheId;
    }

    /**
     * @param max Max.
     * @return Random value from {@code 0} (inclusive) to the given max value (exclusive).
     */
    public static int randomInt(int max) {
        Random rnd0 = rnd != null ? rnd : ThreadLocalRandom.current();

        return rnd0.nextInt(max);
    }

    /**
     * @param bag Reuse bag.
     * @return Allocated page.
     * @throws IgniteCheckedException If failed.
     */
    protected final long allocatePage(ReuseBag bag) throws IgniteCheckedException {
        long pageId = bag != null ? bag.pollFreePage() : 0;

        if (pageId == 0 && reuseList != null)
            pageId = reuseList.takeRecycledPage();

        if (pageId == 0)
            pageId = allocatePageNoReuse();

        assert pageId != 0;

        return pageId;
    }

    /**
     * @return Page ID of newly allocated page.
     * @throws IgniteCheckedException If failed.
     */
    protected long allocatePageNoReuse() throws IgniteCheckedException {
        return pageMem.allocatePage(cacheId, PageIdAllocator.INDEX_PARTITION, FLAG_IDX);
    }

    /**
     * @param pageId Page ID.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    protected final Page page(long pageId) throws IgniteCheckedException {
        byte flag = PageIdUtils.flag(pageId);

        assert flag == FLAG_IDX && PageIdUtils.partId(pageId) == INDEX_PARTITION ||
            flag == FLAG_DATA && PageIdUtils.partId(pageId) <= MAX_PARTITION_ID : U.hexLong(pageId);

        return pageMem.page(cacheId, pageId);
    }

    /**
     * @param page Page.
     * @return Buffer.
     */
    protected final ByteBuffer tryWriteLock(Page page) {
        ByteBuffer buf = page.tryGetForWrite();

        if (buf != null)
            onWriteLock(page);

        return buf;
    }


    /**
     * @param page Page.
     * @return Buffer.
     */
    protected final ByteBuffer writeLock(Page page) {
        ByteBuffer buf = page.getForWrite();

        onWriteLock(page);

        return buf;
    }

    /**
     * @param page Page.
     */
    protected final void writeUnlock(Page page, boolean dirty) {
        page.releaseWrite(dirty);

        onWriteUnlock(page);
    }

    /**
     * @param page Page.
     * @return Buffer.
     */
    protected final ByteBuffer readLock(Page page) {
        ByteBuffer buf = page.getForRead();

        onReadLock(page);

        return buf;
    }

    /**
     * @param page Page.
     */
    protected final void readUnlock(Page page) {
        page.releaseRead();

        onReadUnlock(page);
    }

    /**
     * @param page Page.
     */
    protected void onWriteLock(Page page) {
        // No-op.
    }

    /**
     * @param page Page.
     */
    protected void onWriteUnlock(Page page) {
        // No-op.
    }

    /**
     * @param page Page.
     */
    protected void onReadLock(Page page) {
        // No-op.
    }

    /**
     * @param page Page.
     */
    protected void onReadUnlock(Page page) {
        // No-op.
    }
}
