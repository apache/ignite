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

package org.apache.ignite.internal.processors.cache.database.tree.util;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import sun.nio.ch.DirectBuffer;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

/**
 * Page handler.
 */
public abstract class PageHandler<X, R> {
    /** */
    private static final PageHandler<Void, Void> NOOP = new PageHandler<Void, Void>() {
        @Override public Void run(long pageId, Page page, PageIO io, ByteBuffer buf, Void arg, int intArg)
            throws IgniteCheckedException {
            return null;
        }
    };

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param io IO.
     * @param buf Page buffer.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public abstract R run(long pageId, Page page, PageIO io, ByteBuffer buf, X arg, int intArg)
        throws IgniteCheckedException;

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return {@code true} If release.
     */
    public boolean releaseAfterWrite(long pageId, Page page, X arg, int intArg) {
        return true;
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    public static <X, R> R readPage(
        long pageId,
        Page page,
        PageLockListener lockListener,
        PageHandler<X, R> h,
        X arg,
        int intArg
    ) throws IgniteCheckedException {
        lockListener.onBeforeReadLock(pageId, page);

        ByteBuffer buf = page.getForRead();

        try {
            lockListener.onReadLock(page, buf);

            PageIO io = PageIO.getPageIO(buf);

            return h.run(pageId, page, io, buf, arg, intArg);
        }
        finally {
            lockListener.onReadUnlock(page, buf);

            page.releaseRead();
        }
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    public static <X, R> R writePage(
        long pageId,
        Page page,
        PageLockListener lockListener,
        PageHandler<X, R> h,
        X arg,
        int intArg
    ) throws IgniteCheckedException {
        return writePage(pageId, page, lockListener, h, null, null, arg, intArg);
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param lockListener Lock listener.
     * @param init IO for new page initialization or {@code null} if it is an existing page.
     * @throws IgniteCheckedException If failed.
     */
    public static void initPage(
        long pageId,
        Page page,
        PageLockListener lockListener,
        PageIO init,
        IgniteWriteAheadLogManager wal
    ) throws IgniteCheckedException {
        writePage(pageId, page, lockListener, NOOP, init, wal, null, 0);
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param lockListener Lock listener.
     * @param h Handler.
     * @param init IO for new page initialization or {@code null} if it is an existing page.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    public static <X, R> R writePage(
        long pageId,
        Page page,
        PageLockListener lockListener,
        PageHandler<X, R> h,
        PageIO init,
        IgniteWriteAheadLogManager wal,
        X arg,
        int intArg
    ) throws IgniteCheckedException {
        lockListener.onBeforeWriteLock(pageId, page);

        R res;

        boolean ok = false;

        ByteBuffer buf = page.getForWrite();

        assert buf != null;

        try {
            lockListener.onWriteLock(page, buf);

            if (init != null) // It is a new page and we have to initialize it.
                doInitPage(pageId, page, buf, init, wal);
            else
                init = PageIO.getPageIO(buf);

            res = h.run(pageId, page, init, buf, arg, intArg);

            ok = true;
        }
        finally {
            assert PageIO.getCrc(buf) == 0; //TODO GG-11480

            if (h.releaseAfterWrite(pageId, page, arg, intArg)) {
                lockListener.onWriteUnlock(page, buf);

                page.releaseWrite(ok);
            }
        }

        return res;
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param buf Buffer.
     * @param init Initial IO.
     * @param wal Write ahead log.
     * @throws IgniteCheckedException If failed.
     */
    private static void doInitPage(
        long pageId,
        Page page,
        ByteBuffer buf,
        PageIO init,
        IgniteWriteAheadLogManager wal
    ) throws IgniteCheckedException {
        assert PageIO.getCrc(buf) == 0; //TODO GG-11480

        init.initNewPage(buf, pageId);

        // Here we should never write full page, because it is known to be new.
        page.fullPageWalRecordPolicy(FALSE);

        if (isWalDeltaRecordNeeded(wal, page))
            wal.log(new InitNewPageRecord(page.fullId().cacheId(), page.id(),
                init.getType(), init.getVersion(), pageId));
    }

    /**
     * @param page Page.
     * @param buf Buffer.
     */
    public static void checkPageId(Page page, ByteBuffer buf) {
        long pageId = PageIO.getPageId(buf);

        // Page ID must be 0L for newly allocated page, for reused page effective ID must remain the same.
        if (pageId != 0L && page.id() != PageIdUtils.effectivePageId(pageId))
            throw new IllegalStateException("Page ID: " + U.hexLong(pageId));
    }

    /**
     * @param wal Write ahead log.
     * @param page Page.
     * @return {@code true} If we need to make a delta WAL record for the change in this page.
     */
    public static boolean isWalDeltaRecordNeeded(IgniteWriteAheadLogManager wal, Page page) {
        // If the page is clean, then it is either newly allocated or just after checkpoint.
        // In both cases we have to write full page contents to WAL.
        return wal != null && !wal.isAlwaysWriteFullPages() && page.fullPageWalRecordPolicy() != TRUE &&
            (page.fullPageWalRecordPolicy() == FALSE || page.isDirty());
    }

    /**
     * @param src Source.
     * @param dst Destination.
     * @param srcOff Source offset in bytes.
     * @param dstOff Destination offset in bytes.
     * @param cnt Bytes count to copy.
     */
    public static void copyMemory(ByteBuffer src, ByteBuffer dst, long srcOff, long dstOff, long cnt) {
        byte[] srcArr = src.hasArray() ? src.array() : null;
        byte[] dstArr = dst.hasArray() ? dst.array() : null;
        long srcArrOff = src.hasArray() ? src.arrayOffset() + GridUnsafe.BYTE_ARR_OFF : 0;
        long dstArrOff = dst.hasArray() ? dst.arrayOffset() + GridUnsafe.BYTE_ARR_OFF : 0;

        long srcPtr = src.isDirect() ? ((DirectBuffer)src).address() : 0;
        long dstPtr = dst.isDirect() ? ((DirectBuffer)dst).address() : 0;

        GridUnsafe.copyMemory(srcArr, srcPtr + srcArrOff + srcOff, dstArr, dstPtr + dstArrOff + dstOff, cnt);
    }
}
