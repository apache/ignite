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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageSetFreeListPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageListMetaResetCountRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListAddPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListInitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListRemovePageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListSetNextRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PagesListSetPreviousRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RotatedIdPartRecord;
import org.apache.ignite.internal.processors.cache.persistence.DataStructure;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridArrays;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdUtils.MAX_ITEMID_NUM;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getPageId;

/**
 * Striped doubly-linked list of page IDs optionally organized in buckets.
 */
public abstract class PagesList extends DataStructure {
    /** */
    private static final int TRY_LOCK_ATTEMPTS =
            IgniteSystemProperties.getInteger("IGNITE_PAGES_LIST_TRY_LOCK_ATTEMPTS", 10);

    /** */
    private static final int MAX_STRIPES_PER_BUCKET =
        IgniteSystemProperties.getInteger("IGNITE_PAGES_LIST_STRIPES_PER_BUCKET",
            Math.max(8, Runtime.getRuntime().availableProcessors()));

    /** */
    protected final AtomicLong[] bucketsSize;

    /** */
    protected volatile boolean changed;

    /** Page ID to store list metadata. */
    private final long metaPageId;

    /** Number of buckets. */
    private final int buckets;

    /** Name (for debug purposes). */
    protected final String name;

    /** */
    private final PageHandler<Void, Boolean> cutTail = new CutTail();

    /**
     *
     */
    private final class CutTail extends PageHandler<Void, Boolean> {
        @Override public Boolean run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIO iox,
            Boolean walPlc,
            Void ignore,
            int bucket) throws IgniteCheckedException {
            assert getPageId(pageAddr) == pageId;

            PagesListNodeIO io = (PagesListNodeIO)iox;

            long tailId = io.getNextId(pageAddr);

            assert tailId != 0;

            io.setNextId(pageAddr, 0L);

            if (needWalDeltaRecord(pageId, page, walPlc))
                wal.log(new PagesListSetNextRecord(cacheId, pageId, 0L));

            updateTail(bucket, tailId, pageId);

            return TRUE;
        }
    }

    /**
     * @param cacheId Cache ID.
     * @param name Name (for debug purpose).
     * @param pageMem Page memory.
     * @param buckets Number of buckets.
     * @param wal Write ahead log manager.
     * @param metaPageId Metadata page ID.
     */
    protected PagesList(
        int cacheId,
        String name,
        PageMemory pageMem,
        int buckets,
        IgniteWriteAheadLogManager wal,
        long metaPageId
    ) {
        super(cacheId, pageMem, wal);

        this.name = name;
        this.buckets = buckets;
        this.metaPageId = metaPageId;

        bucketsSize = new AtomicLong[buckets];

        for (int i = 0; i < buckets; i++)
            bucketsSize[i] = new AtomicLong();
    }

    /**
     * @param metaPageId Metadata page ID.
     * @param initNew {@code True} if new list if created, {@code false} if should be initialized from metadata.
     * @throws IgniteCheckedException If failed.
     */
    protected final void init(long metaPageId, boolean initNew) throws IgniteCheckedException {
        if (metaPageId != 0L) {
            if (initNew)
                init(metaPageId, PagesListMetaIO.VERSIONS.latest());
            else {
                Map<Integer, GridLongList> bucketsData = new HashMap<>();

                long nextId = metaPageId;

                while (nextId != 0) {
                    final long pageId = nextId;
                    final long page = acquirePage(pageId);

                    try {
                        long pageAddr = readLock(pageId, page); // No concurrent recycling on init.

                        assert pageAddr != 0L;

                        try {
                            PagesListMetaIO io = PagesListMetaIO.VERSIONS.forPage(pageAddr);

                            io.getBucketsData(pageAddr, bucketsData);

                            nextId = io.getNextMetaPageId(pageAddr);

                            assert nextId != pageId :
                                "Loop detected [next=" + U.hexLong(nextId) + ", cur=" + U.hexLong(pageId) + ']';
                        }
                        finally {
                            readUnlock(pageId, page, pageAddr);
                        }
                    }
                    finally {
                        releasePage(pageId, page);
                    }
                }

                for (Map.Entry<Integer, GridLongList> e : bucketsData.entrySet()) {
                    int bucket = e.getKey();
                    long bucketSize = 0;

                    Stripe[] old = getBucket(bucket);
                    assert old == null;

                    long[] upd = e.getValue().array();

                    Stripe[] tails = new Stripe[upd.length];

                    for (int i = 0; i < upd.length; i++) {
                        long tailId = upd[i];

                        long prevId = tailId;
                        int cnt = 0;

                        while (prevId != 0L) {
                            final long pageId = prevId;
                            final long page = acquirePage(pageId);
                            try  {
                                long pageAddr = readLock(pageId, page);

                                assert pageAddr != 0L;

                                try {
                                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                                    cnt += io.getCount(pageAddr);
                                    prevId = io.getPreviousId(pageAddr);

                                    // In reuse bucket the page itself can be used as a free page.
                                    if (isReuseBucket(bucket) && prevId != 0L)
                                        cnt++;
                                }
                                finally {
                                    readUnlock(pageId, page, pageAddr);
                                }
                            }
                            finally {
                                releasePage(pageId, page);
                            }
                        }

                        Stripe stripe = new Stripe(tailId, cnt == 0);
                        tails[i] = stripe;
                        bucketSize += cnt;
                    }

                    boolean ok = casBucket(bucket, null, tails);

                    assert ok;

                    bucketsSize[bucket].set(bucketSize);
                }
            }
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void saveMetadata() throws IgniteCheckedException {
        assert metaPageId != 0;

        long curId = 0L;
        long curPage = 0L;
        long curAddr = 0L;

        PagesListMetaIO curIo = null;

        long nextPageId = metaPageId;

        if (!changed)
            return;

        try {
            for (int bucket = 0; bucket < buckets; bucket++) {
                Stripe[] tails = getBucket(bucket);

                if (tails != null) {
                    int tailIdx = 0;

                    while (tailIdx < tails.length) {
                        int written = curPage != 0L ?
                            curIo.addTails(pageMem.realPageSize(grpId), curAddr, bucket, tails, tailIdx) :
                            0;

                        if (written == 0) {
                            if (nextPageId == 0L) {
                                nextPageId = allocatePageNoReuse();

                                if (curPage != 0L) {
                                    curIo.setNextMetaPageId(curAddr, nextPageId);

                                    releaseAndClose(curId, curPage, curAddr);
                                }

                                curId = nextPageId;
                                curPage = acquirePage(curId);
                                curAddr = writeLock(curId, curPage);

                                curIo = PagesListMetaIO.VERSIONS.latest();

                                curIo.initNewPage(curAddr, curId, pageSize());
                            }
                            else {
                                releaseAndClose(curId, curPage, curAddr);

                                curId = nextPageId;
                                curPage = acquirePage(curId);
                                curAddr = writeLock(curId, curPage);

                                curIo = PagesListMetaIO.VERSIONS.forPage(curAddr);

                                curIo.resetCount(curAddr);
                            }

                            nextPageId = curIo.getNextMetaPageId(curAddr);
                        }
                        else
                            tailIdx += written;
                    }
                }
            }
        }
        finally {
            releaseAndClose(curId, curPage, curAddr);
        }

        while (nextPageId != 0L) {
            long pageId = nextPageId;

            long page = acquirePage(pageId);
            try {
                long pageAddr = writeLock(pageId, page);

                try {
                    PagesListMetaIO io = PagesListMetaIO.VERSIONS.forPage(pageAddr);

                    io.resetCount(pageAddr);

                    if (needWalDeltaRecord(pageId, page, null))
                        wal.log(new PageListMetaResetCountRecord(grpId, pageId));

                    nextPageId = io.getNextMetaPageId(pageAddr);
                }
                finally {
                    writeUnlock(pageId, page, pageAddr, true);
                }
            }
            finally {
                releasePage(pageId, page);
            }
        }

        changed = false;
    }

    /**
     * @param pageId Page ID.
     * @param page Page absolute pointer.
     * @param pageAddr Page address.
     */
    private void releaseAndClose(long pageId, long page, long pageAddr) {
        if (page != 0L) {
            try {
                // No special WAL record because we most likely changed the whole page.
                writeUnlock(pageId, page, pageAddr, TRUE, true);
            }
            finally {
                releasePage(pageId, page);
            }
        }
    }

    /**
     * @param bucket Bucket index.
     * @return Bucket.
     */
    protected abstract Stripe[] getBucket(int bucket);

    /**
     * @param bucket Bucket index.
     * @param exp Expected bucket.
     * @param upd Updated bucket.
     * @return {@code true} If succeeded.
     */
    protected abstract boolean casBucket(int bucket, Stripe[] exp, Stripe[] upd);

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
    private void setupNextPage(PagesListNodeIO io, long prevId, long prev, long nextId, long next) {
        assert io.getNextId(prev) == 0L;

        io.initNewPage(next, nextId, pageSize());
        io.setPreviousId(next, prevId);

        io.setNextId(prev, nextId);
    }

    /**
     * Adds stripe to the given bucket.
     *
     * @param bucket Bucket.
     * @param bag Reuse bag.
     * @param reuse {@code True} if possible to use reuse list.
     * @throws IgniteCheckedException If failed.
     * @return Tail page ID.
     */
    private Stripe addStripe(int bucket, ReuseBag bag, boolean reuse) throws IgniteCheckedException {
        long pageId = allocatePage(bag, reuse);

        init(pageId, PagesListNodeIO.VERSIONS.latest());

        Stripe stripe = new Stripe(pageId, true);

        for (;;) {
            Stripe[] old = getBucket(bucket);
            Stripe[] upd;

            if (old != null) {
                int len = old.length;

                upd = Arrays.copyOf(old, len + 1);

                upd[len] = stripe;
            }
            else
                upd = new Stripe[]{stripe};

            if (casBucket(bucket, old, upd))
                return stripe;
        }
    }

    /**
     * @param bucket Bucket index.
     * @param oldTailId Old tail page ID to replace.
     * @param newTailId New tail page ID.
     * @return {@code True} if stripe was removed.
     */
    private boolean updateTail(int bucket, long oldTailId, long newTailId) {
        int idx = -1;

        for (;;) {
            Stripe[] tails = getBucket(bucket);

            // Tail must exist to be updated.
            assert !F.isEmpty(tails) : "Missing tails [bucket=" + bucket + ", tails=" + Arrays.toString(tails) +
                ", metaPage=" + U.hexLong(metaPageId) + ']';

            idx = findTailIndex(tails, oldTailId, idx);

            assert tails[idx].tailId == oldTailId;

            if (newTailId == 0L) {
                if (tails.length <= MAX_STRIPES_PER_BUCKET / 2) {
                    tails[idx].empty = true;

                    return false;
                }

                Stripe[] newTails;

                if (tails.length != 1)
                    newTails = GridArrays.remove(tails, idx);
                else
                    newTails = null; // Drop the bucket completely.

                if (casBucket(bucket, tails, newTails)) {
                    // Reset tailId for invalidation of locking when stripe was taken concurrently.
                    tails[idx].tailId = 0L;

                    return true;
                }
            }
            else {
                // It is safe to assign new tail since we do it only when write lock on tail is held.
                tails[idx].tailId = newTailId;

                return true;
            }
        }
    }

    /**
     * @param tails Tails.
     * @param tailId Tail ID to find.
     * @param expIdx Expected index.
     * @return First found index of the given tail ID.
     */
    private static int findTailIndex(Stripe[] tails, long tailId, int expIdx) {
        if (expIdx != -1 && tails.length > expIdx && tails[expIdx].tailId == tailId)
            return expIdx;

        for (int i = 0; i < tails.length; i++) {
            if (tails[i].tailId == tailId)
                return i;
        }

        throw new IllegalStateException("Tail not found: " + tailId);
    }

    /**
     * @param bucket Bucket.
     * @param bag Reuse bag.
     * @return Page ID where the given page
     * @throws IgniteCheckedException If failed.
     */
    private Stripe getPageForPut(int bucket, ReuseBag bag) throws IgniteCheckedException {
        // Striped pool optimization.
        IgniteThread igniteThread = IgniteThread.current();

        Stripe[] tails = getBucket(bucket);

        if (igniteThread != null && igniteThread.policy() == GridIoPolicy.DATA_STREAMER_POOL) {
            int stripeIdx = igniteThread.stripe();

            assert stripeIdx != -1 : igniteThread;

            while (tails == null || stripeIdx >= tails.length) {
                addStripe(bucket, bag, true);

                tails = getBucket(bucket);
            }

            return tails[stripeIdx];
        }

        if (tails == null)
            return addStripe(bucket, bag, true);

        return randomTail(tails);
    }

    /**
     * @param tails Tails.
     * @return Random tail.
     */
    private static Stripe randomTail(Stripe[] tails) {
        int len = tails.length;

        assert len != 0;

        return tails[randomInt(len)];
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

        Stripe[] tails = getBucket(bucket);

        if (tails != null) {
            for (Stripe tail : tails) {
                long tailId = tail.tailId;

                while (tailId != 0L) {
                    final long pageId = tailId;
                    final long page = acquirePage(pageId);
                    try {
                        long pageAddr = readLock(pageId, page);

                        assert pageAddr != 0L;

                        try {
                            PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                            int cnt = io.getCount(pageAddr);

                            assert cnt >= 0;

                            res += cnt;
                            tailId = io.getPreviousId(pageAddr);

                            // In reuse bucket the page itself can be used as a free page.
                            if (isReuseBucket(bucket) && tailId != 0L)
                                res++;
                        }
                        finally {
                            readUnlock(pageId, page, pageAddr);
                        }
                    }
                    finally {
                        releasePage(pageId, page);
                    }
                }
            }
        }

        assert res == bucketsSize[bucket].get() : "Wrong bucket size counter [exp=" + res + ", cntr=" + bucketsSize[bucket].get() + ']';

        return res;
    }

    /**
     * @param bag Reuse bag.
     * @param dataId Data page ID.
     * @param dataPage Data page pointer.
     * @param dataAddr Data page address.
     * @param bucket Bucket.
     * @throws IgniteCheckedException If failed.
     */
    protected final void put(
        ReuseBag bag,
        final long dataId,
        final long dataPage,
        final long dataAddr,
        int bucket)
        throws IgniteCheckedException {
        assert bag == null ^ dataAddr == 0L;

        for (int lockAttempt = 0; ;) {
            Stripe stripe = getPageForPut(bucket, bag);

            // No need to continue if bag has been utilized at getPageForPut.
            if (bag != null && bag.isEmpty())
                return;

            final long tailId = stripe.tailId;

            // Stripe was removed from bucket concurrently.
            if (tailId == 0L)
                continue;

            final long tailPage = acquirePage(tailId);

            try {
                long tailAddr = writeLockPage(tailId, tailPage, bucket, lockAttempt++, bag); // Explicit check.

                if (tailAddr == 0L) {
                    // No need to continue if bag has been utilized at writeLockPage.
                    if (bag != null && bag.isEmpty())
                        return;
                    else
                        continue;
                }

                if (stripe.tailId != tailId) {
                    // Another thread took the last page.
                    writeUnlock(tailId, tailPage, tailAddr, false);

                    lockAttempt--; // Ignore current attempt.

                    continue;
                }

                assert PageIO.getPageId(tailAddr) == tailId
                    : "tailId = " + U.hexLong(tailId) + ", pageId = " + U.hexLong(PageIO.getPageId(tailAddr));
                assert PageIO.getType(tailAddr) == PageIO.T_PAGE_LIST_NODE
                    : "tailId = " + U.hexLong(tailId) + ", type = " + PageIO.getType(tailAddr);

                boolean ok = false;

                try {
                    PagesListNodeIO io = PageIO.getPageIO(tailAddr);

                    ok = bag != null ?
                        // Here we can always take pages from the bag to build our list.
                        putReuseBag(tailId, tailPage, tailAddr, io, bag, bucket) :
                        // Here we can use the data page to build list only if it is empty and
                        // it is being put into reuse bucket. Usually this will be true, but there is
                        // a case when there is no reuse bucket in the free list, but then deadlock
                        // on node page allocation from separate reuse list is impossible.
                        // If the data page is not empty it can not be put into reuse bucket and thus
                        // the deadlock is impossible as well.
                        putDataPage(tailId, tailPage, tailAddr, io, dataId, dataPage, dataAddr, bucket);

                    if (ok) {
                        stripe.empty = false;

                        return;
                    }
                }
                finally {
                    writeUnlock(tailId, tailPage, tailAddr, ok);
                }
            }
            finally {
                releasePage(tailId, tailPage);
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param io IO.
     * @param dataId Data page ID.
     * @param dataPage Data page pointer.
     * @param dataAddr Data page address.
     * @param bucket Bucket.
     * @return {@code true} If succeeded.
     * @throws IgniteCheckedException If failed.
     */
    private boolean putDataPage(
        final long pageId,
        final long page,
        final long pageAddr,
        PagesListNodeIO io,
        final long dataId,
        final long dataPage,
        final long dataAddr,
        int bucket
    ) throws IgniteCheckedException {
        if (io.getNextId(pageAddr) != 0L)
            return false; // Splitted.

        int idx = io.addPage(pageAddr, dataId, pageSize());

        if (idx == -1)
            handlePageFull(pageId, page, pageAddr, io, dataId, dataPage, dataAddr, bucket);
        else {
            incrementBucketSize(bucket);

            if (needWalDeltaRecord(pageId, page, null))
                wal.log(new PagesListAddPageRecord(grpId, pageId, dataId));

            AbstractDataPageIO dataIO = PageIO.getPageIO(dataAddr);
            dataIO.setFreeListPageId(dataAddr, pageId);

            if (needWalDeltaRecord(dataId, dataPage, null))
                wal.log(new DataPageSetFreeListPageRecord(grpId, dataId, pageId));
        }

        return true;
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param io IO.
     * @param dataId Data page ID.
     * @param data Data page pointer.
     * @param dataAddr Data page address.
     * @param bucket Bucket index.
     * @throws IgniteCheckedException If failed.
     * */
    private void handlePageFull(
        final long pageId,
        final long page,
        final long pageAddr,
        PagesListNodeIO io,
        final long dataId,
        final long data,
        final long dataAddr,
        int bucket
    ) throws IgniteCheckedException {
        AbstractDataPageIO dataIO = PageIO.getPageIO(dataAddr);

        // Attempt to add page failed: the node page is full.
        if (isReuseBucket(bucket)) {
            // If we are on the reuse bucket, we can not allocate new page, because it may cause deadlock.
            assert dataIO.isEmpty(dataAddr); // We can put only empty data pages to reuse bucket.

            // Change page type to index and add it as next node page to this list.
            long newDataId = PageIdUtils.changeType(dataId, FLAG_IDX);

            setupNextPage(io, pageId, pageAddr, newDataId, dataAddr);

            if (needWalDeltaRecord(pageId, page, null))
                wal.log(new PagesListSetNextRecord(grpId, pageId, newDataId));

            if (needWalDeltaRecord(dataId, data, null))
                wal.log(new PagesListInitNewPageRecord(
                    grpId,
                    dataId,
                    io.getType(),
                    io.getVersion(),
                    newDataId,
                    pageId, 0L));

            // In reuse bucket the page itself can be used as a free page.
            incrementBucketSize(bucket);

            updateTail(bucket, pageId, newDataId);
        }
        else {
            // Just allocate a new node page and add our data page there.
            final long nextId = allocatePage(null);
            final long nextPage = acquirePage(nextId);

            try {
                long nextPageAddr = writeLock(nextId, nextPage); // Newly allocated page.

                assert nextPageAddr != 0L;

                // Here we should never write full page, because it is known to be new.
                Boolean nextWalPlc = FALSE;

                try {
                    setupNextPage(io, pageId, pageAddr, nextId, nextPageAddr);

                    if (needWalDeltaRecord(pageId, page, null))
                        wal.log(new PagesListSetNextRecord(grpId, pageId, nextId));

                    int idx = io.addPage(nextPageAddr, dataId, pageSize());

                    if (needWalDeltaRecord(nextId, nextPage, nextWalPlc))
                        wal.log(new PagesListInitNewPageRecord(
                            grpId,
                            nextId,
                            io.getType(),
                            io.getVersion(),
                            nextId,
                            pageId,
                            dataId
                        ));

                    assert idx != -1;

                    dataIO.setFreeListPageId(dataAddr, nextId);

                    if (needWalDeltaRecord(dataId, data, null))
                        wal.log(new DataPageSetFreeListPageRecord(grpId, dataId, nextId));

                    incrementBucketSize(bucket);

                    updateTail(bucket, pageId, nextId);
                }
                finally {
                    writeUnlock(nextId, nextPage, nextPageAddr, nextWalPlc, true);
                }
            }
            finally {
                releasePage(nextId, nextPage);
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param io IO.
     * @param bag Reuse bag.
     * @param bucket Bucket.
     * @return {@code true} If succeeded.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private boolean putReuseBag(
        final long pageId,
        final long page,
        final long pageAddr,
        PagesListNodeIO io,
        ReuseBag bag,
        int bucket
    ) throws IgniteCheckedException {
        assert bag != null : "bag is null";
        assert !bag.isEmpty() : "bag is empty";

        if (io.getNextId(pageAddr) != 0L)
            return false; // Splitted.

        long nextId;

        long prevId = pageId;
        long prevPage = page;
        long prevAddr = pageAddr;

        Boolean walPlc = null;

        GridLongList locked = null; // TODO may be unlock right away and do not keep all these pages locked?

        try {
            while ((nextId = bag.pollFreePage()) != 0L) {
                assert PageIdUtils.itemId(nextId) > 0 && PageIdUtils.itemId(nextId) <= MAX_ITEMID_NUM : U.hexLong(nextId);

                int idx = io.addPage(prevAddr, nextId, pageSize());

                if (idx == -1) { // Attempt to add page failed: the node page is full.

                    final long nextPage = acquirePage(nextId);

                    try {
                        long nextPageAddr = writeLock(nextId, nextPage); // Page from reuse bag can't be concurrently recycled.

                        assert nextPageAddr != 0L;

                        if (locked == null)
                            locked = new GridLongList(6);

                        locked.add(nextId);
                        locked.add(nextPage);
                        locked.add(nextPageAddr);

                        setupNextPage(io, prevId, prevAddr, nextId, nextPageAddr);

                        if (needWalDeltaRecord(prevId, prevPage, walPlc))
                            wal.log(new PagesListSetNextRecord(grpId, prevId, nextId));

                        // Here we should never write full page, because it is known to be new.
                        if (needWalDeltaRecord(nextId, nextPage, FALSE))
                            wal.log(new PagesListInitNewPageRecord(
                                grpId,
                                nextId,
                                io.getType(),
                                io.getVersion(),
                                nextId,
                                prevId,
                                0L
                            ));

                        // In reuse bucket the page itself can be used as a free page.
                        if (isReuseBucket(bucket))
                            incrementBucketSize(bucket);

                        // Switch to this new page, which is now a part of our list
                        // to add the rest of the bag to the new page.
                        prevAddr = nextPageAddr;
                        prevId = nextId;
                        prevPage = nextPage;
                        // Starting from tis point all wal records are written for reused pages from the bag.
                        // This mean that we use delta records only.
                        walPlc = FALSE;
                    }
                    finally {
                        releasePage(nextId, nextPage);
                    }
                }
                else {
                    // TODO: use single WAL record for bag?
                    if (needWalDeltaRecord(prevId, prevPage, walPlc))
                        wal.log(new PagesListAddPageRecord(grpId, prevId, nextId));

                    incrementBucketSize(bucket);
                }
            }
        }
        finally {
            if (locked != null) {
                // We have to update our bucket with the new tail.
                updateTail(bucket, pageId, prevId);

                // Release write.
                for (int i = 0; i < locked.size(); i += 3)
                    writeUnlock(locked.get(i), locked.get(i + 1), locked.get(i + 2), FALSE, true);
            }
        }

        return true;
    }

    /**
     * @param bucket Bucket index.
     * @return Page for take.
     */
    private Stripe getPageForTake(int bucket) {
        Stripe[] tails = getBucket(bucket);

        if (tails == null || bucketsSize[bucket].get() == 0)
            return null;

        int len = tails.length;

        // Striped pool optimization.
        IgniteThread igniteThread = IgniteThread.current();

        if (igniteThread != null && igniteThread.policy() == GridIoPolicy.DATA_STREAMER_POOL) {
            int stripeIdx = igniteThread.stripe();

            assert stripeIdx != -1 : igniteThread;

            if (stripeIdx >= len)
                return null;

            Stripe stripe = tails[stripeIdx];

            return stripe.empty ? null : stripe;
        }

        int init = randomInt(len);
        int cur = init;

        while (true) {
            Stripe stripe = tails[cur];

            if (!stripe.empty)
                return stripe;

            if ((cur = (cur + 1) % len) == init)
                return null;
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param bucket Bucket.
     * @param lockAttempt Lock attempts counter.
     * @param bag Reuse bag.
     * @return Page address if page is locked of {@code null} if can retry lock.
     * @throws IgniteCheckedException If failed.
     */
    private long writeLockPage(long pageId, long page, int bucket, int lockAttempt, ReuseBag bag)
        throws IgniteCheckedException {
        // Striped pool optimization.
        IgniteThread igniteThread = IgniteThread.current();

        if (igniteThread != null && igniteThread.policy() == GridIoPolicy.DATA_STREAMER_POOL) {
            assert igniteThread.stripe() != -1 : igniteThread;

            return writeLock(pageId, page);
        }

        long pageAddr = tryWriteLock(pageId, page);

        if (pageAddr != 0L)
            return pageAddr;

        if (lockAttempt == TRY_LOCK_ATTEMPTS) {
            Stripe[] stripes = getBucket(bucket);

            if (stripes == null || stripes.length < MAX_STRIPES_PER_BUCKET) {
                addStripe(bucket, bag, !isReuseBucket(bucket));

                return 0L;
            }
        }

        return lockAttempt < TRY_LOCK_ATTEMPTS ? 0L : writeLock(pageId, page); // Must be explicitly checked further.
    }

    /**
     * @param bucket Bucket index.
     * @param initIoVers Optional IO to initialize page.
     * @return Removed page ID.
     * @throws IgniteCheckedException If failed.
     */
    protected final long takeEmptyPage(int bucket, @Nullable IOVersions initIoVers) throws IgniteCheckedException {
        for (int lockAttempt = 0; ;) {
            Stripe stripe = getPageForTake(bucket);

            if (stripe == null)
                return 0L;

            final long tailId = stripe.tailId;

            // Stripe was removed from bucket concurrently.
            if (tailId == 0L)
                continue;

            final long tailPage = acquirePage(tailId);

            try {
                long tailAddr = writeLockPage(tailId, tailPage, bucket, lockAttempt++, null); // Explicit check.

                if (tailAddr == 0L)
                    continue;

                if (stripe.empty || stripe.tailId != tailId) {
                    // Another thread took the last page.
                    writeUnlock(tailId, tailPage, tailAddr, false);

                    if (bucketsSize[bucket].get() > 0) {
                        lockAttempt--; // Ignore current attempt.

                        continue;
                    }
                    else
                        return 0L;
                }

                assert PageIO.getPageId(tailAddr) == tailId
                    : "tailId = " + U.hexLong(tailId) + ", pageId = " + U.hexLong(PageIO.getPageId(tailAddr));
                assert PageIO.getType(tailAddr) == PageIO.T_PAGE_LIST_NODE
                    : "tailId = " + U.hexLong(tailId) + ", type = " + PageIO.getType(tailAddr);

                boolean dirty = false;
                long dataPageId;
                long recycleId = 0L;

                try {
                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(tailAddr);

                    if (io.getNextId(tailAddr) != 0) {
                        // It is not a tail anymore, retry.
                        continue;
                    }

                    long pageId = io.takeAnyPage(tailAddr);

                    if (pageId != 0L) {
                        decrementBucketSize(bucket);

                        if (needWalDeltaRecord(tailId, tailPage, null))
                            wal.log(new PagesListRemovePageRecord(grpId, tailId, pageId));

                        dirty = true;

                        assert !isReuseBucket(bucket) ||
                            PageIdUtils.itemId(pageId) > 0 && PageIdUtils.itemId(pageId) <= MAX_ITEMID_NUM
                            : "Incorrectly recycled pageId in reuse bucket: " + U.hexLong(pageId);

                        dataPageId = pageId;

                        if (io.isEmpty(tailAddr)) {
                            long prevId = io.getPreviousId(tailAddr);

                            // If we got an empty page in non-reuse bucket, move it back to reuse list
                            // to prevent empty page leak to data pages.
                            if (!isReuseBucket(bucket)) {
                                if (prevId != 0L) {
                                    Boolean ok = write(prevId, cutTail, null, bucket, FALSE);

                                    assert ok == TRUE : ok;

                                    recycleId = recyclePage(tailId, tailPage, tailAddr, null);
                                }
                                else
                                    stripe.empty = true;
                            }
                            else
                                stripe.empty = prevId == 0L;
                        }
                    }
                    else {
                        // The tail page is empty, but stripe is not. It might
                        // happen only if we are in reuse bucket and it has
                        // a previous page, so, the current page can be collected
                        assert isReuseBucket(bucket);

                        long prevId = io.getPreviousId(tailAddr);

                        assert prevId != 0L;

                        Boolean ok = write(prevId, cutTail, bucket, FALSE);

                        assert ok == TRUE : ok;

                        decrementBucketSize(bucket);

                        if (initIoVers != null)
                            dataPageId = initReusedPage(tailId, tailPage, tailAddr, 0, FLAG_DATA, initIoVers.latest());
                        else
                            dataPageId = recyclePage(tailId, tailPage, tailAddr, null);

                        dirty = true;
                    }

                    // If we do not have a previous page (we are at head), then we still can return
                    // current page but we have to drop the whole stripe. Since it is a reuse bucket,
                    // we will not do that, but just return 0L, because this may produce contention on
                    // meta page.
                }
                finally {
                    writeUnlock(tailId, tailPage, tailAddr, dirty);
                }

                // Put recycled page (if any) to the reuse bucket after tail is unlocked.
                if (recycleId != 0L) {
                    assert !isReuseBucket(bucket);

                    reuseList.addForRecycle(new SingletonReuseBag(recycleId));
                }

                return dataPageId;
            }
            finally {
                releasePage(tailId, tailPage);
            }
        }
    }

    /**
     * Reused page must obtain correctly assembled page id, then initialized by proper {@link PageIO} instance
     * and non-zero {@code itemId} of reused page id must be saved into special place.
     *
     * @param reusedPageId Reused page id.
     * @param reusedPage Reused page.
     * @param reusedPageAddr Reused page address.
     * @param partId Partition id.
     * @param flag Flag.
     * @param initIo Initial io.
     * @return Prepared page id.
     */
    protected final long initReusedPage(long reusedPageId, long reusedPage, long reusedPageAddr,
        int partId, byte flag, PageIO initIo) throws IgniteCheckedException {

        long newPageId = PageIdUtils.pageId(partId, flag, PageIdUtils.pageIndex(reusedPageId));

        initIo.initNewPage(reusedPageAddr, newPageId, pageSize());

        boolean needWalDeltaRecord = needWalDeltaRecord(reusedPageId, reusedPage, null);

        if (needWalDeltaRecord) {
            wal.log(new InitNewPageRecord(grpId, reusedPageId, initIo.getType(),
                initIo.getVersion(), newPageId));
        }

        int itemId = PageIdUtils.itemId(reusedPageId);

        if (itemId != 0) {
            PageIO.setRotatedIdPart(reusedPageAddr, itemId);

            if (needWalDeltaRecord)
                wal.log(new RotatedIdPartRecord(grpId, newPageId, itemId));
        }

        return newPageId;
    }

    /**
     * @param dataId Data page ID.
     * @param dataPage Data page pointer.
     * @param dataAddr Data page address.
     * @param dataIO Data page IO.
     * @param bucket Bucket index.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if page was removed.
     */
    protected final boolean removeDataPage(
        final long dataId,
        final long dataPage,
        final long dataAddr,
        AbstractDataPageIO dataIO,
        int bucket)
        throws IgniteCheckedException {
        final long pageId = dataIO.getFreeListPageId(dataAddr);

        assert pageId != 0;

        final long page = acquirePage(pageId);
        try {
            long nextId;

            long recycleId = 0L;

            long pageAddr = writeLock(pageId, page); // Explicit check.

            if (pageAddr == 0L)
                return false;

            boolean rmvd = false;

            try {
                PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                rmvd = io.removePage(pageAddr, dataId);

                if (!rmvd)
                    return false;

                decrementBucketSize(bucket);

                if (needWalDeltaRecord(pageId, page, null))
                    wal.log(new PagesListRemovePageRecord(grpId, pageId, dataId));

                // Reset free list page ID.
                dataIO.setFreeListPageId(dataAddr, 0L);

                if (needWalDeltaRecord(dataId, dataPage, null))
                    wal.log(new DataPageSetFreeListPageRecord(grpId, dataId, 0L));

                if (!io.isEmpty(pageAddr))
                    return true; // In optimistic case we still have something in the page and can leave it as is.

                // If the page is empty, we have to try to drop it and link next and previous with each other.
                nextId = io.getNextId(pageAddr);

                // If there are no next page, then we can try to merge without releasing current write lock,
                // because if we will need to lock previous page, the locking order will be already correct.
                if (nextId == 0L) {
                    long prevId = io.getPreviousId(pageAddr);

                    recycleId = mergeNoNext(pageId, page, pageAddr, prevId, bucket);
                }
            }
            finally {
                writeUnlock(pageId, page, pageAddr, rmvd);
            }

            // Perform a fair merge after lock release (to have a correct locking order).
            if (nextId != 0L)
                recycleId = merge(pageId, page, nextId, bucket);

            if (recycleId != 0L)
                reuseList.addForRecycle(new SingletonReuseBag(recycleId));

            return true;
        }
        finally {
            releasePage(pageId, page);
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param prevId Previous page ID.
     * @param bucket Bucket index.
     * @return Page ID to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long mergeNoNext(
        long pageId,
        long page,
        long pageAddr,
        long prevId,
        int bucket)
        throws IgniteCheckedException {
        // If we do not have a next page (we are tail) and we are on reuse bucket,
        // then we can leave as is as well, because it is normal to have an empty tail page here.
        if (isReuseBucket(bucket))
            return 0L;

        if (prevId != 0L) { // Cut tail if we have a previous page.
            Boolean ok = write(prevId, cutTail, null, bucket, FALSE);

            assert ok == TRUE: ok;
        }
        else {
            // If we don't have a previous, then we are tail page of free list, just drop the stripe.
            boolean rmvd = updateTail(bucket, pageId, 0L);

            if (!rmvd)
                return 0L;
        }

        return recyclePage(pageId, page, pageAddr, null);
    }

    /**
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param nextId Next page ID.
     * @param bucket Bucket index.
     * @return Page ID to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long merge(
        final long pageId,
        final long page,
        long nextId,
        int bucket)
        throws IgniteCheckedException {
        assert nextId != 0; // We should do mergeNoNext then.

        // Lock all the pages in correct order (from next to previous) and do the merge in retry loop.
        for (;;) {
            final long curId = nextId;
            final long curPage = curId == 0L ? 0L : acquirePage(curId);
            try {
                boolean write = false;

                final long curAddr = curPage == 0L ? 0L : writeLock(curId, curPage); // Explicit check.
                final long pageAddr = writeLock(pageId, page); // Explicit check.

                if (pageAddr == 0L) {
                    if (curAddr != 0L) // Unlock next page if needed.
                        writeUnlock(curId, curPage, curAddr, false);

                    return 0L; // Someone has merged or taken our empty page concurrently. Nothing to do here.
                }

                try {
                    PagesListNodeIO io = PagesListNodeIO.VERSIONS.forPage(pageAddr);

                    if (!io.isEmpty(pageAddr))
                        return 0L; // No need to merge anymore.

                    // Check if we see a consistent state of the world.
                    if (io.getNextId(pageAddr) == curId && (curId == 0L) == (curAddr == 0L)) {
                        long recycleId = doMerge(pageId, page, pageAddr, io, curId, curPage, curAddr, bucket);

                        write = true;

                        return recycleId; // Done.
                    }

                    // Reread next page ID and go for retry.
                    nextId = io.getNextId(pageAddr);
                }
                finally {
                    if (curAddr != 0L)
                        writeUnlock(curId, curPage, curAddr, write);

                    writeUnlock(pageId, page, pageAddr, write);
                }
            }
            finally {
                if (curPage != 0L)
                    releasePage(curId, curPage);
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page absolute pointer.
     * @param pageAddr Page address.
     * @param io IO.
     * @param nextId Next page ID.
     * @param nextPage Next page absolute pointer.
     * @param nextAddr Next page address.
     * @param bucket Bucket index.
     * @return Page to recycle.
     * @throws IgniteCheckedException If failed.
     */
    private long doMerge(
        long pageId,
        long page,
        long pageAddr,
        PagesListNodeIO io,
        long nextId,
        long nextPage,
        long nextAddr,
        int bucket
    ) throws IgniteCheckedException {
        long prevId = io.getPreviousId(pageAddr);

        if (nextId == 0L)
            return mergeNoNext(pageId, page, pageAddr, prevId, bucket);
        else {
            // No one must be able to merge it while we keep a reference.
            assert getPageId(nextAddr) == nextId;

            if (prevId == 0L) { // No previous page: we are at head.
                // These references must be updated at the same time in write locks.
                assert PagesListNodeIO.VERSIONS.forPage(nextAddr).getPreviousId(nextAddr) == pageId;

                PagesListNodeIO nextIO = PagesListNodeIO.VERSIONS.forPage(nextAddr);
                nextIO.setPreviousId(nextAddr, 0);

                if (needWalDeltaRecord(nextId, nextPage, null))
                    wal.log(new PagesListSetPreviousRecord(grpId, nextId, 0L));
            }
            else // Do a fair merge: link previous and next to each other.
                fairMerge(prevId, pageId, nextId, nextPage, nextAddr);

            return recyclePage(pageId, page, pageAddr, null);
        }
    }

    /**
     * Link previous and next to each other.
     * @param prevId Previous Previous page ID.
     * @param pageId Page ID.
     * @param nextId Next page ID.
     * @param nextPage Next page absolute pointer.
     * @param nextAddr Next page address.
     * @throws IgniteCheckedException If failed.
     */
    private void fairMerge(
        final long prevId,
        long pageId,
        long nextId,
        long nextPage,
        long nextAddr)
        throws IgniteCheckedException {
        long prevPage = acquirePage(prevId);

        try {
            final long prevAddr = writeLock(prevId, prevPage); // No check, we keep a reference.
            assert prevAddr != 0L;
            try {
                PagesListNodeIO prevIO = PagesListNodeIO.VERSIONS.forPage(prevAddr);
                PagesListNodeIO nextIO = PagesListNodeIO.VERSIONS.forPage(nextAddr);

                // These references must be updated at the same time in write locks.
                assert prevIO.getNextId(prevAddr) == pageId;
                assert nextIO.getPreviousId(nextAddr) == pageId;

                prevIO.setNextId(prevAddr, nextId);

                if (needWalDeltaRecord(prevId, prevPage, null))
                    wal.log(new PagesListSetNextRecord(grpId, prevId, nextId));

                nextIO.setPreviousId(nextAddr, prevId);

                if (needWalDeltaRecord(nextId, nextPage, null))
                    wal.log(new PagesListSetPreviousRecord(grpId, nextId, prevId));
            }
            finally {
                writeUnlock(prevId, prevPage, prevAddr, true);
            }
        }
        finally {
            releasePage(prevId, prevPage);
        }
    }

    /**
     * Increments bucket size and updates changed flag.
     *
     * @param bucket Bucket number.
     */
    private void incrementBucketSize(int bucket) {
        bucketsSize[bucket].incrementAndGet();

        // Ok to have a race here, see the field javadoc.
        if (!changed)
            changed = true;
    }

    /**
     * Increments bucket size and updates changed flag.
     *
     * @param bucket Bucket number.
     */
    private void decrementBucketSize(int bucket) {
        bucketsSize[bucket].decrementAndGet();

        // Ok to have a race here, see the field javadoc.
        if (!changed)
            changed = true;
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
        SingletonReuseBag(long pageId) {
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

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return pageId == 0L;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SingletonReuseBag.class, this, "pageId", U.hexLong(pageId));
        }
    }

    /**
     *
     */
    public static final class Stripe {
        /** */
        public volatile long tailId;

        /** */
        volatile boolean empty;

        /**
         * @param tailId Tail ID.
         * @param empty Empty flag.
         */
        Stripe(long tailId, boolean empty) {
            this.tailId = tailId;
            this.empty = empty;
        }
    }
}
