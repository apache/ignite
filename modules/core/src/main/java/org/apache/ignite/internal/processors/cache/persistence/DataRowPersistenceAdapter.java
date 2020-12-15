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
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IncompleteObject;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.KEY_ONLY;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.LINK_WITH_HEADER;
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
     * @param sharedCtx Cache shared context.
     * @param coctx Cache object context.
     * @param io Page IO.
     * @param rowData Required row data.
     * @param readCacheId {@code true} If need to read cache ID.
     * @param skipVer Whether version read should be skipped.
     * @return Incomplete object.
     * @throws IgniteCheckedException If failed.
     */
    private IncompleteObject<?> readIncomplete(
        IncompleteObject<?> incomplete,
        GridCacheSharedContext<?, ?> sharedCtx,
        CacheObjectContext coctx,
        ByteBuffer buff,
        int itemId,
        DataPageIO io,
        RowData rowData,
        boolean readCacheId,
        boolean skipVer
    ) throws IgniteCheckedException {
        DataPagePayload data = io.readPayload(buff, itemId);

        long nextLink = data.nextLink();

        if (incomplete == null) {
            if (nextLink == 0) {
                // Fast path for a single page row.
                readFullRow(sharedCtx, coctx, buff, data.offset(), rowData, readCacheId, skipVer);

                return null;
            }

            if (rowData == LINK_WITH_HEADER)
                return null;
        }

        int off = data.offset();
        int payloadSize = data.payloadSize();

        buff.position(off);
        buff.limit(off + payloadSize);

        boolean keyOnly = rowData == RowData.KEY_ONLY;

        incomplete = readFragment(sharedCtx, coctx, buff, keyOnly, readCacheId, incomplete, skipVer);

        if (incomplete != null)
            incomplete.setNextLink(nextLink);

        return incomplete;
    }

    /**
     * @param io Data page IO.
     * @param itemId Row item Id.
     * @param grp Cache group.
     * @param sharedCtx Cache shared context.
     * @param rowData Required row data.
     * @param skipVer Whether version read should be skipped.
     * @throws IgniteCheckedException If failed.
     */
    public final void initFromPageBuffer(
        IgniteInClosure2X<Long, ByteBuffer> reader,
        DataPageIO io,
        ByteBuffer pageBuff,
        ByteBuffer fragmentBuff,
        int itemId,
        @Nullable CacheGroupContext grp,
        GridCacheSharedContext<?, ?> sharedCtx,
        RowData rowData,
        boolean skipVer
    ) throws IgniteCheckedException {
        CacheObjectContext coctx = grp != null ? grp.cacheObjectContext() : null;
        boolean readCacheId = grp == null || grp.storeCacheIdInDataPage();

        IncompleteObject<?> incomplete = readIncomplete(null, sharedCtx, coctx, pageBuff, itemId, io,
            rowData, readCacheId, skipVer);

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

                incomplete = readIncomplete(incomplete, sharedCtx, coctx,
                    fragmentBuff, itemId(nextLink), io2, rowData, readCacheId, skipVer);

                if (incomplete == null || (rowData == KEY_ONLY && key != null))
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
     * @param sharedCtx Cache shared context.
     * @param coctx Cache object context.
     * @param rowData Required row data.
     * @param readCacheId {@code true} If need to read cache ID.
     * @param skipVer Whether version read should be skipped.
     * @throws IgniteCheckedException If failed.
     */
    protected void readFullRow(
        GridCacheSharedContext<?, ?> sharedCtx,
        CacheObjectContext coctx,
        ByteBuffer buff,
        int offset,
        RowData rowData,
        boolean readCacheId,
        boolean skipVer
    ) throws IgniteCheckedException {
        buff.position(offset);

        if (rowData == LINK_WITH_HEADER)
            return;

        if (readCacheId)
            cacheId = buff.getInt();

        if (coctx == null)
            coctx = sharedCtx.cacheContext(cacheId).cacheObjectContext();

        int len = buff.getInt();
        byte keyType = buff.get();

        if (rowData != RowData.NO_KEY && rowData != RowData.NO_KEY_WITH_HINTS) {
            byte[] bytes = new byte[len];
            buff.get(bytes);

            key = coctx.kernalContext().cacheObjects().toKeyCacheObject(coctx, keyType, bytes);

            if (rowData == RowData.KEY_ONLY)
                return;
        }

        len = buff.getInt();
        byte valType = buff.get();

        byte[] bytes = new byte[len];
        buff.get(bytes);

        val = coctx.kernalContext().cacheObjects().toCacheObject(coctx, valType, bytes);

        if (skipVer) {
            ver = null;

            int verLen = CacheVersionIO.readSize(buff, false);
            buff.position(buff.position() + verLen);
        }
        else
            ver = CacheVersionIO.read(buff, false);

        verReady = true;

        expireTime = buff.getLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataRowPersistenceAdapter.class, this, super.toString());
    }
}
