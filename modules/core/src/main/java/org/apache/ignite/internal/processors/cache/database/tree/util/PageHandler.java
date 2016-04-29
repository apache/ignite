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
import org.apache.ignite.internal.util.GridUnsafe;
import sun.nio.ch.DirectBuffer;

/**
 * Page handler.
 */
public abstract class PageHandler<X> {
    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param buf Page buffer.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public abstract int run(long pageId, Page page, ByteBuffer buf, X arg, int intArg) throws IgniteCheckedException;

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
    public static <X> int readPage(long pageId, Page page, PageHandler<X> h, X arg, int intArg)
        throws IgniteCheckedException {
        assert page != null;

        ByteBuffer buf = page.getForRead();

        assert buf != null;

        try {
            return h.run(pageId, page, buf, arg, intArg);
        }
        finally {
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
    public static <X> int writePage(long pageId, Page page, PageHandler<X> h, X arg, int intArg)
        throws IgniteCheckedException {
        assert page != null;

        int res;

        boolean ok = false;

        ByteBuffer buf = page.getForWrite();

        assert buf != null;

        try {
            res = h.run(pageId, page, buf, arg, intArg);

            ok = true;
        }
        finally {
            if (h.releaseAfterWrite(pageId, page, arg, intArg))
                page.releaseWrite(ok);
        }

        return res;
    }

    /**
     * @param src Source.
     * @param dst Destination.
     * @param srcOff Source offset in bytes.
     * @param dstOff Destination offset in bytes.
     * @param cnt Bytes count to copy.
     */
    public static void copyMemory(ByteBuffer src, ByteBuffer dst, long srcOff, long dstOff, long cnt) {
        assert src.isDirect();
        assert dst.isDirect();

        long srcPtr = ((DirectBuffer)src).address() + srcOff;
        long dstPtr = ((DirectBuffer)dst).address() + dstOff;

        GridUnsafe.copyMemory(srcPtr, dstPtr, cnt);
    }
}
