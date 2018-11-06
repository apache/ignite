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

package org.apache.ignite.internal.processors.cache.persistence.tree.util;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageSupport;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

/**
 * Page handler.
 */
public abstract class PageHandler<X, R> {
    /** */
    private static final PageHandler<Void, Boolean> NO_OP = new PageHandler<Void, Boolean>() {
        @Override public Boolean run(int cacheId, long pageId, long page, long pageAddr, PageIO io, Boolean walPlc,
            Void arg,
            int intArg) throws IgniteCheckedException {
            return Boolean.TRUE;
        }
    };

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page absolute pointer.
     * @param pageAddr Page address.
     * @param io IO.
     * @param walPlc Full page WAL record policy.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public abstract R run(
        int cacheId,
        long pageId,
        long page,
        long pageAddr,
        PageIO io,
        Boolean walPlc,
        X arg,
        int intArg)
        throws IgniteCheckedException;

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return {@code true} If release.
     */
    public boolean releaseAfterWrite(
        int cacheId,
        long pageId,
        long page,
        long pageAddr,
        X arg,
        int intArg) {
        return true;
    }

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param lsnr Lock listener.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    public static <X, R> R readPage(
        PageMemory pageMem,
        int cacheId,
        long pageId,
        PageLockListener lsnr,
        PageHandler<X, R> h,
        X arg,
        int intArg,
        R lockFailed
    ) throws IgniteCheckedException {
        long page = pageMem.acquirePage(cacheId, pageId);
        try {
            long pageAddr = readLock(pageMem, cacheId, pageId, page, lsnr);

            if (pageAddr == 0L)
                return lockFailed;
            try {
                PageIO io = PageIO.getPageIO(pageAddr);
                return h.run(cacheId, pageId, page, pageAddr, io, null, arg, intArg);
            }
            finally {
                readUnlock(pageMem, cacheId, pageId, page, pageAddr, lsnr);
            }
        }
        finally {
            pageMem.releasePage(cacheId, pageId, page);
        }
    }

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param lsnr Lock listener.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    public static <X, R> R readPage(
        PageMemory pageMem,
        int cacheId,
        long pageId,
        long page,
        PageLockListener lsnr,
        PageHandler<X, R> h,
        X arg,
        int intArg,
        R lockFailed
    ) throws IgniteCheckedException {
        long pageAddr = readLock(pageMem, cacheId, pageId, page, lsnr);

        if (pageAddr == 0L)
            return lockFailed;
        try {
            PageIO io = PageIO.getPageIO(pageAddr);
            return h.run(cacheId, pageId, page, pageAddr, io, null, arg, intArg);
        }
        finally {
            readUnlock(pageMem, cacheId, pageId, page, pageAddr, lsnr);
        }
    }

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param lsnr Lock listener.
     * @return Page address.
     */
    public static long readLock(
        PageMemory pageMem,
        int cacheId,
        long pageId,
        long page,
        PageLockListener lsnr) {
        lsnr.onBeforeReadLock(cacheId, pageId, page);

        long pageAddr = pageMem.readLock(cacheId, pageId, page);

        lsnr.onReadLock(cacheId, pageId, page, pageAddr);

        return pageAddr;
    }

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address (for-write pointer)
     * @param lsnr Lock listener.
     */
    public static void readUnlock(
        PageMemory pageMem,
        int cacheId,
        long pageId,
        long page,
        long pageAddr,
        PageLockListener lsnr) {
        lsnr.onReadUnlock(cacheId, pageId, page, pageAddr);

        pageMem.readUnlock(cacheId, pageId, page);
    }

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param init IO for new page initialization.
     * @param wal Write ahead log.
     * @param lsnr Lock listener.
     * @throws IgniteCheckedException If failed.
     */
    public static void initPage(
        PageMemory pageMem,
        int cacheId,
        long pageId,
        PageIO init,
        IgniteWriteAheadLogManager wal,
        PageLockListener lsnr
    ) throws IgniteCheckedException {
        Boolean res = writePage(pageMem, cacheId, pageId, lsnr, PageHandler.NO_OP, init, wal, null, null, 0, FALSE);

        assert res != FALSE;
    }

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param lsnr Lock listener.
     * @param h Handler.
     * @param init IO for new page initialization or {@code null} if it is an existing page.
     * @param wal Write ahead log.
     * @param walPlc Full page WAL record policy.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    public static <X, R> R writePage(
        PageMemory pageMem,
        int cacheId,
        final long pageId,
        PageLockListener lsnr,
        PageHandler<X, R> h,
        PageIO init,
        IgniteWriteAheadLogManager wal,
        Boolean walPlc,
        X arg,
        int intArg,
        R lockFailed
    ) throws IgniteCheckedException {
        boolean releaseAfterWrite = true;
        long page = pageMem.acquirePage(cacheId, pageId);
        try {
            long pageAddr = writeLock(pageMem, cacheId, pageId, page, lsnr, false);

            if (pageAddr == 0L)
                return lockFailed;

            boolean ok = false;

            try {
                if (init != null) {
                    // It is a new page and we have to initialize it.
                    doInitPage(pageMem, cacheId, pageId, page, pageAddr, init, wal);
                    walPlc = FALSE;
                }
                else
                    init = PageIO.getPageIO(pageAddr);

                R res = h.run(cacheId, pageId, page, pageAddr, init, walPlc, arg, intArg);

                ok = true;

                return res;
            }
            finally {
                assert PageIO.getCrc(pageAddr) == 0; //TODO GG-11480

                if (releaseAfterWrite = h.releaseAfterWrite(cacheId, pageId, page, pageAddr, arg, intArg))
                    writeUnlock(pageMem, cacheId, pageId, page, pageAddr, lsnr, walPlc, ok);
            }
        }
        finally {
            if (releaseAfterWrite)
                pageMem.releasePage(cacheId, pageId, page);
        }
    }

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param lsnr Lock listener.
     * @param h Handler.
     * @param init IO for new page initialization or {@code null} if it is an existing page.
     * @param wal Write ahead log.
     * @param walPlc Full page WAL record policy.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    public static <X, R> R writePage(
        PageMemory pageMem,
        int cacheId,
        long pageId,
        long page,
        PageLockListener lsnr,
        PageHandler<X, R> h,
        PageIO init,
        IgniteWriteAheadLogManager wal,
        Boolean walPlc,
        X arg,
        int intArg,
        R lockFailed
    ) throws IgniteCheckedException {
        long pageAddr = writeLock(pageMem, cacheId, pageId, page, lsnr, false);

        if (pageAddr == 0L)
            return lockFailed;

        boolean ok = false;

        try {
            if (init != null) {
                // It is a new page and we have to initialize it.
                doInitPage(pageMem, cacheId, pageId, page, pageAddr, init, wal);
                walPlc = FALSE;
            }
            else
                init = PageIO.getPageIO(pageAddr);

            R res = h.run(cacheId, pageId, page, pageAddr, init, walPlc, arg, intArg);

            ok = true;

            return res;
        }
        finally {
            assert PageIO.getCrc(pageAddr) == 0; //TODO GG-11480

            if (h.releaseAfterWrite(cacheId, pageId, page, pageAddr, arg, intArg))
                writeUnlock(pageMem, cacheId, pageId, page, pageAddr, lsnr, walPlc, ok);
        }
    }

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param lsnr Lock listener.
     * @param walPlc Full page WAL record policy.
     * @param dirty Page is dirty.
     */
    public static void writeUnlock(
        PageMemory pageMem,
        int cacheId,
        long pageId,
        long page,
        long pageAddr,
        PageLockListener lsnr,
        Boolean walPlc,
        boolean dirty) {
        lsnr.onWriteUnlock(cacheId, pageId, page, pageAddr);

        pageMem.writeUnlock(cacheId, pageId, page, walPlc, dirty);
    }

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param lsnr Lock listener.
     * @param tryLock Only try to lock without waiting.
     * @return Page address or {@code 0} if failed to lock due to recycling.
     */
    public static long writeLock(
        PageMemory pageMem,
        int cacheId,
        long pageId,
        long page,
        PageLockListener lsnr,
        boolean tryLock) {
        lsnr.onBeforeWriteLock(cacheId, pageId, page);

        long pageAddr = tryLock ? pageMem.tryWriteLock(cacheId, pageId, page) : pageMem.writeLock(cacheId, pageId, page);

        lsnr.onWriteLock(cacheId, pageId, page, pageAddr);

        return pageAddr;
    }

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param init Initial IO.
     * @param wal Write ahead log.
     * @throws IgniteCheckedException If failed.
     */
    private static void doInitPage(
        PageMemory pageMem,
        int cacheId,
        long pageId,
        long page,
        long pageAddr,
        PageIO init,
        IgniteWriteAheadLogManager wal) throws IgniteCheckedException {

        assert PageIO.getCrc(pageAddr) == 0; //TODO GG-11480

        init.initNewPage(pageAddr, pageId, pageMem.pageSize());

        // Here we should never write full page, because it is known to be new.
        if (isWalDeltaRecordNeeded(pageMem, cacheId, pageId, page, wal, FALSE))
            wal.log(new InitNewPageRecord(cacheId, pageId,
                init.getType(), init.getVersion(), pageId));
    }

    /**
     * @param pageMem Page memory.
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param wal Write ahead log.
     * @param walPlc Full page WAL record policy.
     * @return {@code true} If we need to make a delta WAL record for the change in this page.
     */
    public static boolean isWalDeltaRecordNeeded(
        PageSupport pageMem,
        int cacheId,
        long pageId,
        long page,
        IgniteWriteAheadLogManager wal,
        Boolean walPlc) {
        // If the page is clean, then it is either newly allocated or just after checkpoint.
        // In both cases we have to write full page contents to WAL.
        return wal != null && !wal.isAlwaysWriteFullPages() && walPlc != TRUE && !wal.disabled(cacheId) &&
            (walPlc == FALSE || pageMem.isDirty(cacheId, pageId, page));
    }

    /**
     * @param src Source.
     * @param srcOff Source offset in bytes.
     * @param dst Destination.
     * @param dstOff Destination offset in bytes.
     * @param cnt Bytes count to copy.
     */
    public static void copyMemory(ByteBuffer src, long srcOff, ByteBuffer dst, long dstOff, long cnt) {
        byte[] srcArr = src.hasArray() ? src.array() : null;
        byte[] dstArr = dst.hasArray() ? dst.array() : null;
        long srcArrOff = src.hasArray() ? src.arrayOffset() + GridUnsafe.BYTE_ARR_OFF : 0;
        long dstArrOff = dst.hasArray() ? dst.arrayOffset() + GridUnsafe.BYTE_ARR_OFF : 0;

        long srcPtr = src.isDirect() ? GridUnsafe.bufferAddress(src) : 0;
        long dstPtr = dst.isDirect() ? GridUnsafe.bufferAddress(dst) : 0;

        GridUnsafe.copyMemory(srcArr, srcPtr + srcArrOff + srcOff, dstArr, dstPtr + dstArrOff + dstOff, cnt);
    }

    /**
     * Will zero memory in buf
     * @param buf Buffer.
     * @param off Offset.
     * @param len Length.
     */
    public static void zeroMemory(ByteBuffer buf, int off, int len) {
        if (buf.isDirect())
            GridUnsafe.setMemory(GridUnsafe.bufferAddress(buf) + off, len, (byte)0);

        else {
            for (int i = off; i < off + len; i++)
                buf.put(i, (byte)0); //TODO Optimize!
        }
    }

    /**
     * @param srcAddr Source.
     * @param srcOff Source offset in bytes.
     * @param dstAddr Destination.
     * @param dstOff Destination offset in bytes.
     * @param cnt Bytes count to copy.
     */
    public static void copyMemory(long srcAddr, long srcOff, long dstAddr, long dstOff, long cnt) {
        GridUnsafe.copyMemory(null, srcAddr + srcOff, null, dstAddr + dstOff, cnt);
    }

    /**
     * @param addr Address.
     * @param off Offset.
     * @param len Length.
     */
    public static void zeroMemory(long addr, int off, int len) {
        GridUnsafe.setMemory(addr + off, len, (byte)0);
    }
}
