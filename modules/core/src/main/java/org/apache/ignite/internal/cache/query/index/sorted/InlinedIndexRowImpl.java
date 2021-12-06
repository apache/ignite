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

package org.apache.ignite.internal.cache.query.index.sorted;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.jetbrains.annotations.Nullable;

/**
 * Represents as index row that uses inlined keys as cache proxy on underlying storage.
 */
public class InlinedIndexRowImpl extends IndexRowImpl {
    /** Whether the CacheDataRow was inited. */
    private boolean ready;

    /** Cache Group Context. */
    private final CacheGroupContext cgctx;

    /** Address of page with index row. */
    private final long pageAddr;

    /** Size of inline for this row. */
    private final int inlineSize;

    /** Offset to inlined keys for this row. */
    private final int inlineOffset;

    /** */
    private int lastOffset;

    /** */
    private int lastExtractedKeyIdx = -1;

    /** */
    public InlinedIndexRowImpl(
        CacheGroupContext cgctx,
        CacheDataRow row,
        InlineIndexRowHandler rowHnd,
        long pageAddr,
        int inlineOffset,
        int inlineSize) {
        super(rowHnd, row);

        this.cgctx = cgctx;
        this.inlineSize = inlineSize;
        this.pageAddr = pageAddr;
        this.inlineOffset = inlineOffset;
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow cacheDataRow() {
        if (ready)
            return super.cacheDataRow();

        CacheDataRowAdapter row = (CacheDataRowAdapter)super.cacheDataRow();

        try {
            row.initFromLink(cgctx, CacheDataRowAdapter.RowData.FULL, true);

            ready = true;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to init cache data row from index row. ", e);
        }

        return row;
    }

    /** {@inheritDoc} */
    @Override protected IndexKey extractKey(int idx) {
        IndexKey key = extractFromInline(idx);

        lastExtractedKeyIdx = idx;

        if (key != null)
            return key;

        // Key isn't inlined then need to extract it from CacheDataRow.
        if (!ready)
            cacheDataRow();

        return super.extractKey(idx);
    }

    /**
     * @return IndexKey from inline, or {@code null} if key is not inlined.
     */
    private @Nullable IndexKey extractFromInline(int keyIdx) {
        assert keyIdx == lastExtractedKeyIdx + 1 : "It's trying to extract key from inline in wrong order: " + keyIdx;

        List<InlineIndexKeyType> keyTypes = rowHandler().inlineIndexKeyTypes();

        try {
            if (keyIdx >= keyTypes.size())
                return null;

            int maxSize = inlineSize - lastOffset;

            InlineIndexKeyType keyType = keyTypes.get(keyIdx);

            // Do not extract from inline variable length keys.
            if (keyType.inlineSize() < 0)
                return null;

            IndexKey k = keyType.get(pageAddr, inlineOffset + lastOffset, maxSize);

            // No inlined keys anymore.
            if (k == null)
                return null;

            lastOffset += keyType.inlineSize(pageAddr, inlineOffset + lastOffset);

            return k;
        }
        catch (Exception e) {
            throw new IgniteException("Failed to get index row key.", e);
        }
    }
}
