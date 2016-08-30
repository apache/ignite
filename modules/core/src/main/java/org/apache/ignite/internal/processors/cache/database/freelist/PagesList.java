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
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridArrays;
import org.apache.ignite.internal.util.typedef.F;

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
                    initPage(nextId, next, PagesListNodeIO.VERSIONS.latest(), wal);

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

                        initPage(nextId, next, PagesListNodeIO.VERSIONS.latest(), wal);

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
     * @param wal Write ahead log manager.
     */
    public PagesList(int cacheId, PageMemory pageMem, IgniteWriteAheadLogManager wal) {
        super(cacheId, pageMem, wal);
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
    protected abstract boolean isReuseBucket(int bucket);

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

                upd = Arrays.copyOf(old, len + 2);

                // Tail will be from the left, head from the right, but now they are the same.
                upd[len + 1] = upd[len] = pageId;
            }
            else
                upd = new long[]{pageId, pageId};

            if (casBucket(bucket, old, upd))
                return pageId;
        }
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

            debugLog("updateTail b=" + bucket + ", old=" + oldTailId + ", new=" + newTailId);

            // Tail must exist to be updated.
            assert !F.isEmpty(tails) : "Missing tails [bucket=" + bucket + ", tails=" + Arrays.toString(tails) + ']';

            idx = findTailIndex(tails, oldTailId, idx);

            assert tails[idx] == oldTailId;

            long[] newTails;

            if (newTailId == 0L) {
                // Have to drop stripe.
                assert tails[idx + 1] == oldTailId; // The last page must be the same for both: tail and head.

                if (tails.length != 2) {
                    // Remove tail and head.
                    newTails = GridArrays.remove(tails, idx + 1); // TODO optimize - do in a single operation.
                    newTails = GridArrays.remove(newTails, idx);
                }
                else
                    newTails = null; // Drop the bucket completely.
            }
            else {
                newTails = tails.clone();

                newTails[idx] = newTailId;
            }

            if (casBucket(bucket, tails, newTails))
                return;
        }
    }

    /**
     * @param tails Tails.
     * @param tailId Tail ID to find.
     * @param expIdx Expected index.
     * @return First found index of the given tail ID.
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

        return tails[randomInt(len >>> 1) << 1]; // Choose only even tails, because odds are heads.
    }

    /**
     * !!! For tests only, does not provide any correctness guarantees for concurrent access.
     *
     * @param bucket Bucket index.
     * @return Number of pages stored in this list.
     * @throws IgniteCheckedException If failed.
     */
    protected final long storedPagesCount(int bucket) throws IgniteCheckedException {
        long res = 0;

        long[] tails = getBucket(bucket);

        if (tails != null) {
            // Step == 2 because we store both tails of the same list.
            for (int i = 0; i < tails.length; i += 2) {
                long pageId = tails[i];

                try (Page page = page(pageId)) {
                    ByteBuffer buf = page.getForRead();

                    try {
                        PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(buf);

                        int cnt = io.getCount(buf);

                        assert cnt >= 0;

                        res += cnt;
                    }
                    finally {
                        page.releaseRead();
                    }
                }
            }
        }

        return res;
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
    protected final long takeEmptyPage(int bucket, IOVersions pageIo) throws IgniteCheckedException {
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

                //assert findTailIndex(getBucket(bucket), tailId, -1) >= 0;

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

                    if (pageIo != null)
                        pageIo.latest().initNewPage(tailBuf, tailId);

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

    /**
     * @param dataPageBuf Data page buffer.
     * @param bucket Bucket index.
     * @throws IgniteCheckedException If failed.
     */
    protected final void removeDataPage(ByteBuffer dataPageBuf, int bucket) throws IgniteCheckedException {
        long dataPageId = getPageId(dataPageBuf);

        DataPageIO dataIO = DataPageIO.VERSIONS.forPage(dataPageBuf);

        long pageId = dataIO.getFreeListPageId(dataPageBuf);

        assert pageId != 0;

        try (Page page = page(pageId)) {
            long prevId;
            long nextId;

            long recycleId = 0L;

            ByteBuffer buf = page.getForWrite();

            boolean rmvd = false;

            try {
                PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(buf);

                rmvd = io.removePage(buf, dataPageId);

                if (!rmvd) {
                    debugLog("removeDataPage not found b=" + bucket + ", pageId=" + pageId);

                    return;
                }

                // Reset free list page ID.
                dataIO.setFreeListPageId(dataPageBuf, 0L);

                if (!io.isEmpty(buf))
                    return; // In optimistic case we still have something in the page and can leave it as is.

                // If the page is empty, we have to try to drop it and link next and previous with each other.
                nextId = io.getNextId(buf);
                prevId = io.getPreviousId(buf);

                debugLog("removeDataPage remove empty b=" + bucket + ", next=" + nextId + ", prev=" + prevId);

                // If there are no next page, then we can try to merge without releasing current write lock,
                // because if we will need to lock previous page, the locking order will be already correct.
                if (nextId == 0L)
                    recycleId = mergeNoNext(buf, pageId, prevId, bucket);
            }
            finally {
                page.releaseWrite(rmvd);
            }

            // Perform a fair merge after lock release (to have a correct locking order).
            if (nextId != 0L)
                recycleId = merge(pageId, page, nextId, bucket);

            if (recycleId != 0L)
                reuseList.addForRecycle(new SingletonReuseBag(recycleId));

            debugLog("removeDataPage end b=" + bucket + ", next=" + nextId + ", prev=" + prevId);
        }
    }

    private void debugLog(String msg) {
        //System.out.println(Thread.currentThread().getName() + ": " + msg);
    }

    /**
     * @param buf Byte buffer.
     * @param pageId Page ID.
     * @param prevId Previous page ID.
     * @param bucket Bucket index.
     * @throws IgniteCheckedException If failed.
     */
    private long mergeNoNext(ByteBuffer buf, long pageId, long prevId, int bucket)
        throws IgniteCheckedException {
        // If we do not have a next page (we are tail) and we are on reuse bucket,
        // then we can leave as is as well, because it is normal to have an empty tail page here.
        if (isReuseBucket(bucket))
            return 0L;

        if (prevId != 0L) { // Cut tail if we have a previous page.
            try (Page prev = page(prevId)) {
                debugLog("mergeNoNext cutTail b=" + bucket + ", prev=" + prevId);

                Boolean ok = writePage(prevId, prev, cutTail, null, bucket);

                assert ok; // Because we keep lock on current tail and do a world consistency check.
            }
        }
        else // If we don't have a previous, then we are tail page of free list, just drop the stripe.
            updateTail(bucket, pageId, 0L);

        recyclePage(pageId, buf);

        return pageId;
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param nextId Next page ID.
     * @param bucket Bucket index.
     * @throws IgniteCheckedException If failed.
     */
    private long merge(long pageId, Page page, long nextId, int bucket)
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
                        return 0L; // Someone has merged or taken our empty page concurrently. Nothing to do here.

                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(buf);

                    if (!io.isEmpty(buf))
                        return 0L; // No need to merge anymore.

                    // Check if we see a consistent state of the world.
                    if (io.getNextId(buf) == nextId) {
                        long recycleId = doMerge(pageId, io, buf, nextId, nextBuf, bucket);

                        write = true;

                        return recycleId; // Done.
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
    private long doMerge(long pageId, PagesListNodeIO io, ByteBuffer buf, long nextId, ByteBuffer nextBuf, int bucket)
        throws IgniteCheckedException {
        debugLog("doMerge b=" + bucket + ", nextId=" + nextId + ", pageId=" + pageId);

        long prevId = io.getPreviousId(buf);

        if (nextId == 0L)
            return mergeNoNext(buf, pageId, prevId, bucket);
        else {
            // No one must be able to merge it while we keep a reference.
            assert getPageId(nextBuf) == nextId;

            if (prevId == 0L) { // No previous page: we are at head.
                // These references must be updated at the same time in write locks.
                assert PagesListNodeIO.VERSIONS.forPage(nextBuf).getPreviousId(nextBuf) == pageId;

                PagesListNodeIO nextIO = PagesListNodeIO.VERSIONS.forPage(nextBuf);
                nextIO.setPreviousId(nextBuf, 0);

                // Drop the page from meta: replace current head with next page.
                // It is a bit hacky, but method updateTail should work here.
                updateTail(bucket, pageId, nextId);
            }
            else { // Do a fair merge: link previous and next to each other.
                debugLog("fairMerge b=" + bucket + ", nextId=" + nextId + ", pageId=" + pageId);

                fairMerge(prevId, pageId, nextId, nextBuf);
            }

            recyclePage(pageId, buf);

            return pageId;
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

    /**
     * Singleton reuse bag.
     */
    private static final class SingletonReuseBag implements ReuseBag {
        /** */
        long pageId;

        /**
         * @param pageId Page ID.
         */
        public SingletonReuseBag(long pageId) {
            this.pageId = pageId;
        }

        /** {@inheritDoc} */
        @Override public void addFreePage(long pageId) {
            throw new IllegalStateException("Should never be called.");
        }

        /** {@inheritDoc} */
        @Override public long pollFreePage() {
            long res = pageId;

            pageId = 0L;

            return res;
        }
    }
}
