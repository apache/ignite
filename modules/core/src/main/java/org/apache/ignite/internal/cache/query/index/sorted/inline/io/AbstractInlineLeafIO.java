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
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 * Leaf page to store index rows with inlined keys.
 */
public abstract class AbstractInlineLeafIO extends BPlusLeafIO<IndexRow> implements InlineIO {
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
    AbstractInlineLeafIO(short type, int ver, int inlineSize) {
        super(type, ver, 8 + inlineSize);

        this.inlineSize = inlineSize;
    }

    /**
     * Register IOs for every available {@link #inlineSize}.
     */
    public static void register() {
        short type = PageIO.T_H2_EX_REF_LEAF_START;

        for (short payload = 1; payload <= PageIO.MAX_PAYLOAD_SIZE; payload++) {
            IOVersions<? extends AbstractInlineLeafIO> io =
                new IOVersions<AbstractInlineLeafIO>(
                    new InlineLeafIO((short)(type + payload - 1), payload));

            PageIO.registerH2ExtraLeaf(io, false);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public final void storeByOffset(long pageAddr, int off, IndexRow row) {
        assert row.getLink() != 0 : row;

        int fieldOff = 0;

        InlineIndexRowHandler rowHnd = ThreadLocalRowHandlerHolder.getRowHandler();

        for (int i = 0; i < rowHnd.getInlineIndexKeyTypes().size(); i++) {
            try {
                int maxSize = inlineSize - fieldOff;

                InlineIndexKeyType keyType = rowHnd.getInlineIndexKeyTypes().get(i);

                int size = keyType.put(pageAddr, off + fieldOff, row.getKey(i), maxSize);

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
    @Override public final IndexRow getLookupRow(BPlusTree<IndexRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {

        long link = PageUtils.getLong(pageAddr, offset(idx) + inlineSize);

        assert link != 0;

        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        CacheGroupContext ctx = ((InlineIndexTree) tree).getContext().group();

        row.initFromLink(ctx, CacheDataRowAdapter.RowData.FULL, true);

        return new IndexRowImpl(ThreadLocalRowHandlerHolder.getRowHandler(), row);
    }

    /** {@inheritDoc} */
    @Override public final void store(long dstPageAddr, int dstIdx, BPlusIO<IndexRow> srcIo, long srcPageAddr, int srcIdx) {
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

    /** {@inheritDoc} */
    @Override public int getInlineSize() {
        return inlineSize;
    }

    /**
     * @param payload Payload size.
     * @return IOVersions for given payload.
     */
    public static IOVersions<? extends BPlusLeafIO<IndexRow>> getVersions(int payload) {
        assert payload >= 0 && payload <= PageIO.MAX_PAYLOAD_SIZE;

        if (payload == 0)
            return LeafIO.VERSIONS;
        else
            return (IOVersions<BPlusLeafIO<IndexRow>>)PageIO.getLeafVersions((short)(payload - 1), false);
    }
}
