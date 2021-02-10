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

package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.IncompleteObject;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.T_DATA;

/**
 *
 */
public class DataRowPersistenceAdapter extends DataRow {
    /**
     * @param partId Partition id.
     */
    public void partition(int partId) {
        part = partId;
    }

    /**
     * @param incomplete Incomplete object.
     * @param coctx Cache object context.
     * @param io Page IO.
     * @param readCacheId {@code true} If need to read cache ID.
     * @return Incomplete object.
     * @throws IgniteCheckedException If failed.
     */
    private IncompleteObject<?> readIncomplete(
        IncompleteObject<?> incomplete,
        CacheObjectContext coctx,
        ByteBuffer buff,
        int itemId,
        DataPageIO io,
        boolean readCacheId
    ) throws IgniteCheckedException {
        assert coctx != null;

        DataPagePayload data = io.readPayload(buff, itemId);
        long nextLink = data.nextLink();

        if (incomplete == null) {
            if (nextLink == 0) {
                // Fast path for a single page row.
                readFullRow(coctx, buff, data.offset(), readCacheId);

                return null;
            }
        }

        int off = data.offset();
        int payloadSize = data.payloadSize();

        buff.position(off);
        buff.limit(off + payloadSize);

        incomplete = readFragment(() -> coctx, buff, false, readCacheId, incomplete, false);

        if (incomplete != null)
            incomplete.setNextLink(nextLink);

        return incomplete;
    }

    /**
     * @param io Data page IO.
     * @param itemId Row item Id.
     * @throws IgniteCheckedException If failed.
     */
    public final void initFromPageBuffer(
        IgniteInClosure2X<Long, ByteBuffer> reader,
        CacheObjectContext coctx,
        ByteBuffer pageBuff,
        ByteBuffer fragmentBuff,
        DataPageIO io,
        int itemId,
        boolean readCacheId
    ) throws IgniteCheckedException {
        IncompleteObject<?> incomplete = readIncomplete(null, coctx, pageBuff, itemId, io, readCacheId);

        if (incomplete == null)
            return;

        long nextLink = incomplete.getNextLink();

        if (nextLink == 0)
            return;

        do {
            long pageId = pageId(nextLink);

            fragmentBuff.clear();

            reader.apply(pageId, fragmentBuff);

            try {
                DataPageIO io2 = PageIO.getPageIO(T_DATA, PageIO.getVersion(fragmentBuff));

                incomplete = readIncomplete(incomplete, coctx, fragmentBuff, itemId(nextLink), io2, readCacheId);

                if (incomplete == null)
                    return;

                nextLink = incomplete.getNextLink();
            }
            catch (Exception e) {
                throw new IgniteException("Error during reading DataRow [pageId=" + pageId + ']', e);
            }
        }
        while (nextLink != 0);

        assert isReady() : "ready";
    }

    /**
     * @param coctx Cache object context.
     * @param readCacheId {@code true} If need to read cache ID.
     * @throws IgniteCheckedException If failed.
     */
    protected void readFullRow(
        CacheObjectContext coctx,
        ByteBuffer buff,
        int offset,
        boolean readCacheId
    ) throws IgniteCheckedException {
        buff.position(offset);

        if (readCacheId)
            cacheId = buff.getInt();

        int len = buff.getInt();
        byte type = buff.get();

        byte[] bytes0 = new byte[len];
        buff.get(bytes0);

        key = coctx.kernalContext().cacheObjects().toKeyCacheObject(coctx, type, bytes0);

        len = buff.getInt();
        type = buff.get();

        byte[] bytes = new byte[len];
        buff.get(bytes);

        val = coctx.kernalContext().cacheObjects().toCacheObject(coctx, type, bytes);

        ver = CacheVersionIO.read(buff, false);

        verReady = true;

        expireTime = buff.getLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataRowPersistenceAdapter.class, this, super.toString());
    }
}
