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
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.DataStructure;
import org.apache.ignite.internal.processors.cache.database.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;

import static org.apache.ignite.internal.processors.cache.database.freelist.PagesList.Result.OK;
import static org.apache.ignite.internal.processors.cache.database.freelist.PagesList.Result.RETRY;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.initPage;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 * Striped doubly-linked list of page IDs optionally organized in buckets.
 */
public abstract class PagesList extends DataStructure {
    /** */
    private final CheckingPageHandler<ByteBuffer> putDataPage = new CheckingPageHandler<ByteBuffer>() {
        @Override protected Result run0(long pageId, Page page, ByteBuffer buf, PagesListNodeIO io,
            ByteBuffer dataPageBuf, int bucket) throws IgniteCheckedException {

            // TODO

            return OK;
        }
    };

    /** */
    private final CheckingPageHandler<ReuseBag> putReuseBag = new CheckingPageHandler<ReuseBag>() {
        @Override protected Result run0(long pageId, Page page, ByteBuffer buf, PagesListNodeIO io,
            ReuseBag arg, int bucket) throws IgniteCheckedException {

            // TODO

            return OK;
        }
    };

    /**
     * @param name Name of the data structure.
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     */
    public PagesList(String name, int cacheId, PageMemory pageMem, IgniteWriteAheadLogManager wal) {
        super(name, cacheId, pageMem, wal);
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

    private void split(ByteBuffer src, PagesListNodeIO io) throws IgniteCheckedException {
        long fwdId = allocatePage();

        try (Page fwd = page(fwdId)) {
            writePage(fwdId, fwd, null, io, wal, src, 0);
        }
    }

    /**
     * @return Allocated page ID.
     * @throws IgniteCheckedException If failed.
     */
    protected final long allocatePage() throws IgniteCheckedException {
        // TODO partId
        // TODO allocate reused

        return pageMem.allocatePage(cacheId, 0, PageIdAllocator.FLAG_IDX);
    }

    /**
     * @param pageId Page ID.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    protected final Page page(long pageId) throws IgniteCheckedException {
        // TODO may be mask counter

        return pageMem.page(cacheId, pageId);
    }

    /**
     * Adds stripe to the given bucket.
     *
     * @param bucket Bucket.
     * @throws IgniteCheckedException If failed.
     */
    private long addStripe(int bucket) throws IgniteCheckedException {
        long pageId = allocatePage();

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

            if (casBucket(bucket, old, upd))
                return pageId;
        }
    }

    /**
     *
     *
     * @param bucket Bucket.
     * @return Page ID where the given page
     * @throws IgniteCheckedException If failed.
     */
    private long getPageForPut(int bucket) throws IgniteCheckedException {
        long[] tails = getBucket(bucket);

        if (tails == null)
            return addStripe(bucket);

        return tails[randomInt(tails.length)];
    }

    protected final void put(ReuseBag bag, ByteBuffer dataPageBuf, int bucket) throws IgniteCheckedException {
        assert bag == null ^ dataPageBuf == null;

        for (;;) {
            long tailId = getPageForPut(bucket);

            try (Page tail = page(tailId)) {
                Result res = bag != null ?
                    // Here we can always take pages from the bag to build our list.
                    writePage(tailId, tail, putReuseBag, bag, bucket) :
                    // Here we can use the data page to build list only if it is empty and
                    // it is being put into reuse bucket. Usually this will be true, but there is
                    // a case when there is no reuse bucket in the free list, but then deadlock
                    // on node page allocation from separate reuse list is impossible.
                    // If the data page is not empty it can not be put into reuse bucket and thus
                    // the deadlock is impossible as well.
                    writePage(tailId, tail, putDataPage, dataPageBuf, bucket);

                switch (res) {
                    case RETRY:
                        continue;

                    case OK:
                        return;

                    case SPLIT:

                }
            }
        }
    }

    /**
     * @return Removed page ID.
     */
    protected final long take(int bucket) {
        return 0L;
    }

    private static abstract class CheckingPageHandler<X> extends PageHandler<X, PagesListNodeIO, Result> {
        /** {@inheritDoc} */
        @Override public final Result run(long pageId, Page page, PagesListNodeIO io, ByteBuffer buf, X arg, int intArg)
            throws IgniteCheckedException {
            if (PageIO.getPageId(buf) != pageId)
                return RETRY;

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
        protected abstract Result run0(long pageId, Page page, ByteBuffer buf, PagesListNodeIO io, X arg, int intArg)
            throws IgniteCheckedException;
    }

    enum Result {
        OK, RETRY, SPLIT
    }
}
