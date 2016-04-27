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

/**
 * Page handler. Can do {@link #readPage(Page, PageHandler, Object, int)}
 * and {@link #writePage(Page, PageHandler, Object, int)} operations.
 */
public abstract class PageHandler<X> {
    /**
     * @param page Page.
     * @param buf Page buffer.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public abstract int run(Page page, ByteBuffer buf, X arg, int intArg) throws IgniteCheckedException;

    /**
     * @param page Page.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return {@code true} If release.
     */
    public boolean releaseAfterWrite(Page page, X arg, int intArg) {
        return true;
    }

    /**
     * @param page Page.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    public static <X> int readPage(Page page, PageHandler<X> h, X arg, int intArg)
        throws IgniteCheckedException {
        assert page != null;

        ByteBuffer buf = page.getForRead();

        assert buf != null;

        try {
            return h.run(page, buf, arg, intArg);
        }
        finally {
            page.releaseRead();
        }
    }

    /**
     * @param page Page.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    public static <X> int writePage(Page page, PageHandler<X> h, X arg, int intArg)
        throws IgniteCheckedException {
        assert page != null;

        int res;

        boolean ok = false;

        ByteBuffer buf = page.getForWrite();

        assert buf != null;

        try {
            res = h.run(page, buf, arg, intArg);

            ok = true;
        }
        finally {
            if (h.releaseAfterWrite(page, arg, intArg))
                page.releaseWrite(ok);
        }

        return res;
    }
}
