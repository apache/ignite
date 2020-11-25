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

package org.apache.ignite.internal.cache.query.index.sorted.inline.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexSchema;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 * Inner page to store index rows with inlined keys.
 */
public abstract class AbstractInlineInnerIO extends BPlusInnerIO<IndexSearchRow> implements InlineIO {
    /**
     * Amount of bytes to store inlined index keys.
     *
     * We do not store schema there:
     * 1. IOs are shared between multiple indexes with the same inlineSize.
     * 2. For backward compatibility, to restore index from PDS.
     */
    private final int inlineSize;

    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param inlineSize size of calculated inline keys.
     */
    AbstractInlineInnerIO(short type, int ver, int inlineSize) {
        super(type, ver, true, 8 + inlineSize);

        this.inlineSize = inlineSize;
    }

    /**
     * Register IOs for every available {@link #inlineSize}.
     */
    public static void register() {
        short type = PageIO.T_H2_EX_REF_INNER_START;

        for (short payload = 1; payload <= PageIO.MAX_PAYLOAD_SIZE; payload++) {
            IOVersions<? extends AbstractInlineInnerIO> io =
                new IOVersions<AbstractInlineInnerIO>(
                    new InlineInnerIO((short)(type + payload - 1), payload));

            PageIO.registerH2ExtraInner(io, false);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public final void storeByOffset(long pageAddr, int off, IndexSearchRow row) {
        assert row.getLink() != 0 : row;

        int fieldOff = 0;

        SortedIndexSchema schema = ThreadLocalSchemaHolder.getSchema();

        for (int i = 0; i < schema.getInlineKeys().length; i++) {
            try {
                int maxSize = inlineSize - fieldOff;

                int size = schema.getInlineKeys()[i].getInlineType()
                    .put(pageAddr, off + fieldOff, row.getKey(i), maxSize);

                // Inline size has exceeded.
                if (size == 0)
                    break;

                fieldOff += size;

            } catch (Exception e) {
                throw new IgniteException("Failed to store new index row.", e);
            }
        }

        // Write link after all inlined idx keys.
        PageUtils.putLong(pageAddr, off + inlineSize, row.getLink());
    }

    /** {@inheritDoc} */
    @Override public final IndexSearchRow getLookupRow(BPlusTree<IndexSearchRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {

        long link = PageUtils.getLong(pageAddr, offset(idx) + inlineSize);

        assert link != 0;

        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        CacheGroupContext ctx = ((InlineIndexTree) tree).getContext();

        row.initFromLink(ctx, CacheDataRowAdapter.RowData.FULL, true);

        return new IndexRowImpl(ThreadLocalSchemaHolder.getSchema(), row);
    }

    /** {@inheritDoc} */
    @Override public final void store(long dstPageAddr, int dstIdx, BPlusIO<IndexSearchRow> srcIo, long srcPageAddr, int srcIdx) {
        int srcOff = srcIo.offset(srcIdx);

        byte[] payload = PageUtils.getBytes(srcPageAddr, srcOff, inlineSize);
        long link = PageUtils.getLong(srcPageAddr, srcOff + inlineSize);

        assert link != 0;

        int dstOff = offset(dstIdx);

        PageUtils.putBytes(dstPageAddr, dstOff, payload);
        PageUtils.putLong(dstPageAddr, dstOff + inlineSize, link);
    }

    /** {@inheritDoc} */
    @Override public long getLink(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + inlineSize);
    }
}
