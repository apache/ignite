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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridArrays;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.processors.cache.database.tree.io.PageIO.getPageId;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.initPage;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 * Striped doubly-linked list of page IDs optionally organized in buckets.
 */
public abstract class PagesList extends DataStructure {
    /** */
    private final CheckingPageHandler<Void> cutTail = new CheckingPageHandler<Void>() {
        @Override protected boolean run0(long pageId, Page page, ByteBuffer buf, PagesListNodeIO io,
            Void ignore, int bucket) throws IgniteCheckedException {
            long tailId = io.getNextId(buf);

            io.setNextId(buf, 0L);

            updateTail(bucket, tailId, pageId);

            return true;
        }
    };

    /** */
    private final CheckingPageHandler<ByteBuffer> putDataPage = new CheckingPageHandler<ByteBuffer>() {
        @Override protected boolean run0(long pageId, Page page, ByteBuffer buf, PagesListNodeIO io,
            ByteBuffer dataPageBuf, int bucket) throws IgniteCheckedException {
            if (io.getNextId(buf) != 0L)
                return false; // Splitted.

            long dataPageId = getPageId(dataPageBuf);
            DataPageIO dataIO = DataPageIO.VERSIONS.forPage(dataPageBuf);

            int idx = io.addPage(buf, dataPageId);

            if (idx == -1)
                handlePageFull(pageId, io, buf, dataPageId, dataIO, dataPageBuf, bucket);
            else
                dataIO.setFreeListPageId(dataPageBuf, pageId);

            return true;
        }

        /**
         * @param pageId Page ID.
         * @param io IO.
         * @param buf Buffer.
         * @param dataPageId Data page ID.
         * @param dataIO Data page IO.
         * @param dataPageBuf Data page buffer.
         * @param bucket Bucket index.
         * @throws IgniteCheckedException If failed.
         */
        private void handlePageFull(
            long pageId,
            PagesListNodeIO io,
            ByteBuffer buf,
            long dataPageId,
            DataPageIO dataIO,
            ByteBuffer dataPageBuf,
            int bucket
        ) throws IgniteCheckedException {
            // Attempt to add page failed: the node page is full.
            if (isReuseBucket(bucket)) {
                // If we are on the reuse bucket, we can not allocate new page, because it may cause deadlock.
                assert dataIO.isEmpty(dataPageBuf); // We can put only empty data pages to reuse bucket.

                // Change page type to index and add it as next node page to this list.
                dataPageId = PageIdUtils.changeType(dataPageId, FLAG_IDX);

                setupNextPage(io, pageId, buf, dataPageId, dataPageBuf);
                updateTail(bucket, pageId, dataPageId);
            }
            else {
                // Just allocate a new node page and add our data page there.
                long nextId = allocatePage(null);

                try (Page next = page(nextId)) {
                    ByteBuffer nextBuf = next.getForWrite();

                    try {
                        setupNextPage(io, pageId, buf, nextId, nextBuf);

                        int idx = io.addPage(nextBuf, dataPageId);

                        assert idx != -1;

                        dataIO.setFreeListPageId(dataPageBuf, nextId);

                        updateTail(bucket, pageId, nextId);
                    }
                    finally {
                        next.releaseWrite(true);
                    }
                }
            }
        }
    };

    /** */
    private final CheckingPageHandler<ReuseBag> putReuseBag = new CheckingPageHandler<ReuseBag>() {
        @SuppressWarnings("ForLoopReplaceableByForEach")
        @Override protected boolean run0(final long pageId, Page page, final ByteBuffer buf, PagesListNodeIO io,
            ReuseBag bag, int bucket) throws IgniteCheckedException {
            if (io.getNextId(buf) != 0L)
                return false; // Splitted.

            long nextId;
            ByteBuffer prevBuf = buf;
            long prevId = pageId;

            List<Page> locked = null;

            try {
                while ((nextId = bag.pollFreePage()) != 0L) {
                    int idx = io.addPage(prevBuf, nextId);

                    if (idx == -1) { // Attempt to add page failed: the node page is full.
                        Page next = page(nextId);

                        ByteBuffer nextBuf = next.getForWrite();

                        if (locked == null)
                            locked = new ArrayList<>(2);

                        locked.add(next);

                        setupNextPage(io, prevId, prevBuf, nextId, nextBuf);

                        // Switch to this new page, which is now a part of our list
                        // to add the rest of the bag to the new page.
                        prevBuf = nextBuf;
                        prevId = nextId;
                    }
                }
            }
            finally {
                if (locked != null) {
                    // We have to update our bucket with the new tail.
                    updateTail(bucket, pageId, prevId);

                    // Release write.
                    for (int i = 0; i < locked.size(); i++)
                        locked.get(i).releaseWrite(true);
                }
            }

            return true;
        }
    };

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param reuseList Reuse list.
     * @param wal Write ahead log manager.
     */
    public PagesList(int cacheId, PageMemory pageMem, ReuseList reuseList, IgniteWriteAheadLogManager wal) {
        super(cacheId, pageMem, reuseList, wal);

        assert reuseList != null;
    }

    /**
     * @param bucket Bucket index.
     * @return Bucket.
     */
    protected abstract long[] getBucket(int bucket);

    /**
     * @param bucket Bucket index.
     * @param exp Expected bucket.
     * @param upd Updated bucket.
     * @return {@code true} If succeeded.
     */
    protected abstract boolean casBucket(int bucket, long[] exp, long[] upd);

    /**
     * @param bucket Bucket index.
     * @return {@code true} If it is a reuse bucket.
     */
    protected boolean isReuseBucket(int bucket) {
        return false;
    }

    /**
     * @param io IO.
     * @param prevId Previous page ID.
     * @param prev Previous page buffer.
     * @param nextId Next page ID.
     * @param next Next page buffer.
     */
    private void setupNextPage(PagesListNodeIO io, long prevId, ByteBuffer prev, long nextId, ByteBuffer next) {
        assert io.getNextId(prev) == 0L;

        io.initNewPage(next, nextId);
        io.setPreviousId(next, prevId);

        io.setNextId(prev, nextId);
    }

    /**
     * Adds stripe to the given bucket.
     *
     * @param bucket Bucket.
     * @throws IgniteCheckedException If failed.
     */
    private long addStripe(int bucket) throws IgniteCheckedException {
        long pageId = allocatePage(null);

        initPage(pageId, page(pageId), PagesListNodeIO.VERSIONS.latest(), wal);

        for (;;) {
            long[] old = getBucket(bucket);
            long[] upd;

            if (old != null) {
                int len = old.length;

                upd = Arrays.copyOf(old, len + 1);

                upd[len] = pageId;
            }
            else
                upd = new long[]{pageId};

            if (casBucket(bucket, old, upd)) {
                metaAddStripeHead(bucket, pageId);

                return pageId;
            }
        }
    }

    /**
     * @param bucket Bucket index.
     * @param headId Head page ID.
     */
    private void metaAddStripeHead(int bucket, long headId) {

    }

    /**
     * @param bucket Bucket index.
     * @param oldHeadId Old head page ID.
     * @param newHeadId New head page ID or {@code 0L} to drop the stripe.
     */
    private void metaReplaceStripeHead(int bucket, long oldHeadId, long newHeadId) {
        // TODO
    }

    /**
     * @param bucket Bucket index.
     * @param oldTailId Old tail page ID to replace.
     * @param newTailId New tail page ID.
     */
    private void updateTail(int bucket, long oldTailId, long newTailId) {
        int idx = -1;

        for (;;) {
            long[] tails = getBucket(bucket);

            assert tails != null;
            assert tails.length > 0;

            idx = findTailIndex(tails, oldTailId, idx);

            long[] newTails;

            if (newTailId == 0L) {
                if (tails.length != 1)
                    newTails = GridArrays.remove(tails, idx);
                else
                    newTails = null; // Drop the bucket completely.
            }
            else {
                newTails = tails.clone();

                newTails[idx] = newTailId;
            }

            if (casBucket(bucket, tails, newTails)) {
                if (newTailId == 0L)
                    metaReplaceStripeHead(bucket, oldTailId, 0L); // Drop stripe.

                return;
            }
        }
    }

    /**
     * @param tails Tails.
     * @param tailId Tail ID to find.
     * @param expIdx Expected index.
     * @return Index of found tail ID.
     */
    private static int findTailIndex(long[] tails, long tailId, int expIdx) {
        if (expIdx != -1 && tails.length > expIdx && tails[expIdx] == tailId)
            return expIdx;

        for (int i = 0; i < tails.length; i++) {
            if (tails[i] == tailId)
                return i;
        }

        throw new IllegalStateException("Tail not found.");
    }

    /**
     * @param bucket Bucket.
     * @return Page ID where the given page
     * @throws IgniteCheckedException If failed.
     */
    private long getPageForPut(int bucket) throws IgniteCheckedException {
        long[] tails = getBucket(bucket);

        if (tails == null)
            return addStripe(bucket);

        return randomTail(tails);
    }

    /**
     * @param tails Tails.
     * @return Random tail.
     */
    private static long randomTail(long[] tails) {
        int len = tails.length;

        assert len != 0;

        return len == 1 ? tails[0] : tails[randomInt(len)];
    }

    /**
     * @param bag Reuse bag.
     * @param dataPageBuf Data page buffer.
     * @param bucket Bucket.
     * @throws IgniteCheckedException If failed.
     */
    protected final void put(ReuseBag bag, ByteBuffer dataPageBuf, int bucket) throws IgniteCheckedException {
        assert bag == null ^ dataPageBuf == null;

        for (;;) {
            long tailId = getPageForPut(bucket);

            try (Page tail = page(tailId)) {
                if (bag != null ?
                    // Here we can always take pages from the bag to build our list.
                    writePage(tailId, tail, putReuseBag, bag, bucket) :
                    // Here we can use the data page to build list only if it is empty and
                    // it is being put into reuse bucket. Usually this will be true, but there is
                    // a case when there is no reuse bucket in the free list, but then deadlock
                    // on node page allocation from separate reuse list is impossible.
                    // If the data page is not empty it can not be put into reuse bucket and thus
                    // the deadlock is impossible as well.
                    writePage(tailId, tail, putDataPage, dataPageBuf, bucket))
                    return;
            }
        }
    }

    /**
     * @param bucket Bucket index.
     * @return Page for take.
     */
    private long getPageForTake(int bucket) {
        long[] tails = getBucket(bucket);

        if (tails == null)
            return 0L;

        return randomTail(tails);
    }

    /**
     * @param bucket Bucket index.
     * @return Removed page ID.
     * @throws IgniteCheckedException If failed.
     */
    protected final long takeEmptyPage(int bucket) throws IgniteCheckedException {
        assert isReuseBucket(bucket);

        long tailId = getPageForTake(bucket);

        if (tailId == 0L)
            return 0L;

        try (Page tail = page(tailId)) {
            ByteBuffer tailBuf = tail.getForWrite();

            try {
                PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(tailBuf);

                long pageId = io.takeAnyPage(tailBuf);

                if (pageId != 0L)
                    return pageId;

                // The tail page is empty, we can unlink and return it if we have a previous page.
                long prevId = io.getPreviousId(tailBuf);

                if (prevId != 0L) {
                    try (Page prev = page(prevId)) {
                        // Lock pages from next to previous.
                        Boolean ok = writePage(prevId, prev, cutTail, null, bucket);

                        assert ok;
                    }

                    // Rotate page so that successors will see this update.
                    tailId = PageIdUtils.rotatePageId(tailId);

                    PageIO.setPageId(tailBuf, tailId);

                    return tailId;
                }

                // If we do not have a previous page (we are at head), then we still can return
                // current page but we have to drop the whole stripe. Since it is a reuse bucket,
                // we will not do that, but just return 0L, because this may produce contention on
                // meta page.

                return 0L;
            }
            finally {
                tail.releaseWrite(true);
            }
        }
    }

    protected final void removeDataPage(ByteBuffer dataPageBuf, int bucket) throws IgniteCheckedException {
        long dataPageId = getPageId(dataPageBuf);

        DataPageIO dataIO = DataPageIO.VERSIONS.forPage(dataPageBuf);

        long pageId = dataIO.getFreeListPageId(dataPageBuf);

        // Reset free list page ID.
        dataIO.setFreeListPageId(dataPageBuf, 0L);

        try (Page page = page(pageId)) {
            long prevId;
            long nextId;

            ByteBuffer buf = page.getForWrite();

            try {
                PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(buf);

                io.removePage(buf, dataPageId);

                if (!io.isEmpty(buf))
                    return; // In optimistic case we still have something in the page and can leave it as is.

                // If the page is empty, we have to try to drop it and link next and previous with each other.
                nextId = io.getNextId(buf);
                prevId = io.getPreviousId(buf);

                // If there are no next page, then we can try to merge without releasing current write lock,
                // because if we will need to lock previous page, the locking order will be already correct.
                if (nextId == 0L)
                    mergeNoNext(buf, pageId, prevId, bucket);
            }
            finally {
                page.releaseWrite(true);
            }

            // Perform a fair merge after lock release (to have a correct locking order).
            if (nextId != 0L)
                merge(pageId, page, nextId, bucket);
        }
    }

    /**
     * @param buf Byte buffer.
     * @param pageId Page ID.
     * @param prevId Previous page ID.
     * @param bucket Bucket index.
     * @throws IgniteCheckedException If failed.
     */
    private void mergeNoNext(ByteBuffer buf, long pageId, long prevId, int bucket)
        throws IgniteCheckedException {
        // If we do not have a next page (we are tail) and we are on reuse bucket,
        // then we can leave as is as well, because it is normal to have an empty tail page here.
        if (isReuseBucket(bucket))
            return;

        if (prevId != 0L) { // Cut tail if we have a previous page.
            try (Page prev = page(prevId)) {
                Boolean ok = writePage(prevId, prev, cutTail, null, 0);

                assert ok; // Because we keep lock on current tail and do a world consistency check.
            }
        }
        else // If we don't have a previous, then we are tail page of free list, just drop the stripe.
            updateTail(bucket, pageId, 0L);

        recyclePage(pageId, buf);
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param nextId Next page ID.
     * @param bucket Bucket index.
     * @throws IgniteCheckedException If failed.
     */
    private void merge(long pageId, Page page, long nextId, int bucket)
        throws IgniteCheckedException {
        assert nextId != 0; // We should do mergeNoNext then.

        // Lock all the pages in correct order (from next to previous) and do the merge in retry loop.
        for (;;) {
            try (Page next = nextId == 0L ? null : page(nextId)) {
                boolean write = false;

                ByteBuffer nextBuf = next == null ? null : next.getForWrite();
                ByteBuffer buf = page.getForWrite();

                try {
                    if (getPageId(buf) != pageId)
                        return; // Someone has merged or taken our empty page concurrently. Nothing to do here.

                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(buf);

                    if (!io.isEmpty(buf))
                        return; // No need to merge anymore.

                    // Check if we see a consistent state of the world.
                    if (io.getNextId(buf) == nextId) {
                        doMerge(pageId, io, buf, nextId, nextBuf, bucket);

                        write = true;

                        return; // Done.
                    }

                    // Reread next page ID and go for retry.
                    nextId = io.getNextId(buf);
                }
                finally {
                    if (next != null)
                        next.releaseWrite(write);

                    page.releaseWrite(write);
                }
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param io IO.
     * @param buf Byte buffer.
     * @param nextId Next page ID.
     * @param nextBuf Next buffer.
     * @param bucket Bucket index.
     * @throws IgniteCheckedException If failed.
     */
    private void doMerge(long pageId, PagesListNodeIO io, ByteBuffer buf, long nextId, ByteBuffer nextBuf, int bucket)
        throws IgniteCheckedException {
        long prevId = io.getPreviousId(buf);

        if (nextId == 0L)
            mergeNoNext(buf, pageId, prevId, bucket);
        else {
            // No one must be able to merge it while we keep a reference.
            assert getPageId(nextBuf) == nextId;

            if (prevId == 0L) { // No previous page: we are at head.
                // These references must be updated at the same time in write locks.
                assert PagesListNodeIO.VERSIONS.forPage(nextBuf).getPreviousId(nextBuf) == pageId;

                // Drop the page from meta: replace current head with next page.
                metaReplaceStripeHead(bucket, pageId, nextId);
            }
            else // Do a fair merge: link previous and next to each other.
                fairMerge(prevId, pageId, nextId, nextBuf);

            recyclePage(pageId, buf);
        }
    }

    /**
     * Link previous and next to each other.
     *
     * @param prevId Previous Previous page ID.
     * @param pageId Page ID.
     * @param nextBuf Next buffer.
     * @param nextId Next page ID.
     * @throws IgniteCheckedException If failed.
     */
    private void fairMerge(long prevId, long pageId, long nextId, ByteBuffer nextBuf) throws IgniteCheckedException {
        try (Page prev = page(prevId)) {
            ByteBuffer prevBuf = prev.getForWrite();

            try {
                assert getPageId(prevBuf) == prevId; // Because we keep a reference.

                PagesListNodeIO prevIO = PagesListNodeIO.VERSIONS.forPage(prevBuf);
                PagesListNodeIO nextIO = PagesListNodeIO.VERSIONS.forPage(nextBuf);

                // These references must be updated at the same time in write locks.
                assert prevIO.getNextId(prevBuf) == pageId;
                assert nextIO.getPreviousId(nextBuf) == pageId;

                prevIO.setNextId(prevBuf, nextId);
                nextIO.setPreviousId(nextBuf, prevId);
            }
            finally {
                prev.releaseWrite(true);
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param buf Byte buffer.
     * @throws IgniteCheckedException If failed.
     */
    private void recyclePage(long pageId, ByteBuffer buf) throws IgniteCheckedException {
        pageId = PageIdUtils.rotatePageId(pageId);

        PageIO.setPageId(buf, pageId);

        reuseList.add(pageId);
    }

    /**
     * Page handler.
     */
    private static abstract class CheckingPageHandler<X> extends PageHandler<X, PagesListNodeIO, Boolean> {
        /** {@inheritDoc} */
        @Override public final Boolean run(long pageId, Page page, PagesListNodeIO io, ByteBuffer buf, X arg, int intArg)
            throws IgniteCheckedException {
            if (getPageId(buf) != pageId)
                return Boolean.FALSE;

            return run0(pageId, page, buf, io, arg, intArg);
        }

        /**
         * @param pageId Page ID.
         * @param page Page.
         * @param buf Buffer.
         * @param io IO.
         * @param arg Argument.
         * @param intArg Integer argument.
         * @throws IgniteCheckedException If failed.
         */
        protected abstract boolean run0(long pageId, Page page, ByteBuffer buf, PagesListNodeIO io, X arg, int intArg)
            throws IgniteCheckedException;
    }
}
