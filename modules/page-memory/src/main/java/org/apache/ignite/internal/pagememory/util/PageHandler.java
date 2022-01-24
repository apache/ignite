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

package org.apache.ignite.internal.pagememory.util;

import static java.lang.Boolean.FALSE;

import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Page handler.
 *
 * @param <X> Type of the arbitrary parameter.
 * @param <R> Type of the result.
 */
public interface PageHandler<X, R> {
    /** No-op page handler. */
    public static final PageHandler<Void, Boolean> NO_OP = (groupId, pageId, page, pageAddr, io, arg, intArg, statHolder) -> Boolean.TRUE;

    /**
     * Handles the page.
     *
     * @param groupId    Group ID.
     * @param pageId     Page ID.
     * @param page       Page pointer.
     * @param pageAddr   Page address.
     * @param io         IO.
     * @param arg        Argument.
     * @param intArg     Argument of type {@code int}.
     * @param statHolder Statistics holder to track IO operations.
     * @return Result.
     * @throws IgniteInternalCheckedException If failed.
     */
    public R run(
            int groupId,
            long pageId,
            long page,
            long pageAddr,
            PageIo io,
            X arg,
            int intArg,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException;

    /**
     * Checks whether write lock (and acquiring if applicable) should be released after handling.
     *
     * @param groupId  Group ID.
     * @param pageId   Page ID.
     * @param page     Page pointer.
     * @param pageAddr Page address.
     * @param arg      Argument.
     * @param intArg   Argument of type {@code int}.
     * @return {@code true} If release.
     */
    default boolean releaseAfterWrite(
            int groupId,
            long pageId,
            long page,
            long pageAddr,
            X arg,
            int intArg
    ) {
        return true;
    }

    /**
     * Executes handler under the read lock or returns {@code lockFailed} if lock failed.
     *
     * @param pageMem    Page memory.
     * @param groupId    Group ID.
     * @param pageId     Page ID.
     * @param lsnr       Lock listener.
     * @param h          Handler.
     * @param arg        Argument.
     * @param intArg     Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    public static <X, R> R readPage(
            PageMemory pageMem,
            int groupId,
            long pageId,
            PageLockListener lsnr,
            PageHandler<X, R> h,
            X arg,
            int intArg,
            R lockFailed,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        long page = pageMem.acquirePage(groupId, pageId, statHolder);

        try {
            return readPage(pageMem, groupId, pageId, page, lsnr, h, arg, intArg, lockFailed, statHolder);
        } finally {
            pageMem.releasePage(groupId, pageId, page);
        }
    }

    /**
     * Executes handler under the read lock or returns {@code lockFailed} if lock failed. Page must already be acquired.
     *
     * @param pageMem    Page memory.
     * @param groupId    Group ID.
     * @param pageId     Page ID.
     * @param page       Page pointer.
     * @param lsnr       Lock listener.
     * @param h          Handler.
     * @param arg        Argument.
     * @param intArg     Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    public static <X, R> R readPage(
            PageMemory pageMem,
            int groupId,
            long pageId,
            long page,
            PageLockListener lsnr,
            PageHandler<X, R> h,
            X arg,
            int intArg,
            R lockFailed,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        long pageAddr = 0L;

        try {
            if ((pageAddr = readLock(pageMem, groupId, pageId, page, lsnr)) == 0L) {
                return lockFailed;
            }

            PageIo io = pageMem.ioRegistry().resolve(pageAddr);

            return h.run(groupId, pageId, page, pageAddr, io, arg, intArg, statHolder);
        } finally {
            if (pageAddr != 0L) {
                readUnlock(pageMem, groupId, pageId, page, pageAddr, lsnr);
            }
        }
    }

    /**
     * Acquires the read lock on the page.
     *
     * @param pageMem Page memory.
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @param lsnr    Lock listener.
     * @return Page address or {@code 0} if acquiring failed.
     */
    public static long readLock(
            PageMemory pageMem,
            int groupId,
            long pageId,
            long page,
            PageLockListener lsnr
    ) {
        lsnr.onBeforeReadLock(groupId, pageId, page);

        long pageAddr = pageMem.readLock(groupId, pageId, page);

        lsnr.onReadLock(groupId, pageId, page, pageAddr);

        return pageAddr;
    }

    /**
     * Releases acquired read lock.
     *
     * @param pageMem  Page memory.
     * @param groupId  Group ID.
     * @param pageId   Page ID.
     * @param page     Page pointer.
     * @param pageAddr Page address.
     * @param lsnr     Lock listener.
     */
    public static void readUnlock(
            PageMemory pageMem,
            int groupId,
            long pageId,
            long page,
            long pageAddr,
            PageLockListener lsnr
    ) {
        lsnr.onReadUnlock(groupId, pageId, page, pageAddr);

        pageMem.readUnlock(groupId, pageId, page);
    }

    /**
     * Initializes a new page.
     *
     * @param pageMem    Page memory.
     * @param groupId    Group ID.
     * @param pageId     Page ID.
     * @param init       IO for new page initialization.
     * @param lsnr       Lock listener.
     * @param statHolder Statistics holder to track IO operations.
     * @throws IgniteInternalCheckedException If failed.
     * @see PageIo#initNewPage(long, long, int)
     */
    public static void initPage(
            PageMemory pageMem,
            int groupId,
            long pageId,
            PageIo init,
            PageLockListener lsnr,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        Boolean res = writePage(
                pageMem,
                groupId,
                pageId,
                lsnr,
                PageHandler.NO_OP,
                init,
                null,
                0,
                FALSE,
                statHolder
        );

        assert res != FALSE;
    }

    /**
     * Executes handler under the write lock or returns {@code lockFailed} if lock failed.
     *
     * @param pageMem    Page memory.
     * @param groupId    Group ID.
     * @param pageId     Page ID.
     * @param lsnr       Lock listener.
     * @param h          Handler.
     * @param init       IO for new page initialization or {@code null} if it is an existing page.
     * @param arg        Argument.
     * @param intArg     Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    //TODO IGNITE-16350 Consider splitting into two separate methods for init and regular locking.
    public static <X, R> R writePage(
            PageMemory pageMem,
            int groupId,
            final long pageId,
            PageLockListener lsnr,
            PageHandler<X, R> h,
            PageIo init,
            X arg,
            int intArg,
            R lockFailed,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        boolean releaseAfterWrite = true;
        long page = pageMem.acquirePage(groupId, pageId, statHolder);
        try {
            long pageAddr = writeLock(pageMem, groupId, pageId, page, lsnr, false);

            if (pageAddr == 0L) {
                return lockFailed;
            }

            boolean ok = false;

            try {
                if (init != null) {
                    // It is a new page and we have to initialize it.
                    doInitPage(pageMem, groupId, pageId, pageAddr, init);
                } else {
                    init = pageMem.ioRegistry().resolve(pageAddr);
                }

                R res = h.run(groupId, pageId, page, pageAddr, init, arg, intArg, statHolder);

                ok = true;

                return res;
            } finally {
                assert PageIo.getCrc(pageAddr) == 0;

                if (releaseAfterWrite = h.releaseAfterWrite(groupId, pageId, page, pageAddr, arg, intArg)) {
                    writeUnlock(pageMem, groupId, pageId, page, pageAddr, lsnr, ok);
                }
            }
        } finally {
            if (releaseAfterWrite) {
                pageMem.releasePage(groupId, pageId, page);
            }
        }
    }

    /**
     * Executes handler under the write lock or returns {@code lockFailed} if lock failed. Page must already be acquired.
     *
     * @param pageMem    Page memory.
     * @param groupId    Group ID.
     * @param pageId     Page ID.
     * @param page       Page pointer.
     * @param lsnr       Lock listener.
     * @param h          Handler.
     * @param init       IO for new page initialization or {@code null} if it is an existing page.
     * @param arg        Argument.
     * @param intArg     Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @param statHolder Statistics holder to track IO operations.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    public static <X, R> R writePage(
            PageMemory pageMem,
            int groupId,
            long pageId,
            long page,
            PageLockListener lsnr,
            PageHandler<X, R> h,
            PageIo init,
            X arg,
            int intArg,
            R lockFailed,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        long pageAddr = writeLock(pageMem, groupId, pageId, page, lsnr, false);

        if (pageAddr == 0L) {
            return lockFailed;
        }

        boolean ok = false;

        try {
            if (init != null) {
                // It is a new page and we have to initialize it.
                doInitPage(pageMem, groupId, pageId, pageAddr, init);
            } else {
                init = pageMem.ioRegistry().resolve(pageAddr);
            }

            R res = h.run(groupId, pageId, page, pageAddr, init, arg, intArg, statHolder);

            ok = true;

            return res;
        } finally {
            assert PageIo.getCrc(pageAddr) == 0;

            if (h.releaseAfterWrite(groupId, pageId, page, pageAddr, arg, intArg)) {
                writeUnlock(pageMem, groupId, pageId, page, pageAddr, lsnr, ok);
            }
        }
    }

    /**
     * Acquires the write lock on the page.
     *
     * @param pageMem Page memory.
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @param lsnr    Lock listener.
     * @param tryLock Only try to lock without waiting.
     * @return Page address or {@code 0} if failed to lock due to recycling.
     */
    public static long writeLock(
            PageMemory pageMem,
            int groupId,
            long pageId,
            long page,
            PageLockListener lsnr,
            boolean tryLock
    ) {
        lsnr.onBeforeWriteLock(groupId, pageId, page);

        long pageAddr = tryLock ? pageMem.tryWriteLock(groupId, pageId, page) : pageMem.writeLock(groupId, pageId, page);

        lsnr.onWriteLock(groupId, pageId, page, pageAddr);

        return pageAddr;
    }

    /**
     * Releases acquired write lock.
     *
     * @param pageMem  Page memory.
     * @param groupId  Group ID.
     * @param pageId   Page ID.
     * @param page     Page pointer.
     * @param pageAddr Page address.
     * @param lsnr     Lock listener.
     * @param dirty    Page is dirty.
     */
    public static void writeUnlock(
            PageMemory pageMem,
            int groupId,
            long pageId,
            long page,
            long pageAddr,
            PageLockListener lsnr,
            boolean dirty
    ) {
        lsnr.onWriteUnlock(groupId, pageId, page, pageAddr);

        pageMem.writeUnlock(groupId, pageId, page, dirty);
    }

    /**
     * Invokes {@link PageIo#initNewPage(long, long, int)} and does additional checks.
     */
    private static void doInitPage(
            PageMemory pageMem,
            int groupId,
            long pageId,
            long pageAddr,
            PageIo init
    ) throws IgniteInternalCheckedException {
        assert PageIo.getCrc(pageAddr) == 0;

        init.initNewPage(pageAddr, pageId, pageMem.realPageSize(groupId));
    }
}
