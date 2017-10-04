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

package org.apache.ignite.internal.processors.query.h2.database.io;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.InlineIndexHelper;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.h2.result.SearchRow;

/**
 * Leaf page for H2 row references.
 */
public class H2ExtrasLeafIO extends BPlusLeafIO<SearchRow> {
    /** Payload size. */
    private final int payloadSize;

    /** */
    public static void register() {
        for (short payload = 1; payload <= PageIO.MAX_PAYLOAD_SIZE; payload++)
            PageIO.registerH2ExtraLeaf(getVersions((short)(PageIO.T_H2_EX_REF_LEAF_START + payload - 1), payload));
    }

    /**
     * @param payload Payload size.
     * @return IOVersions for given payload.
     */
    @SuppressWarnings("unchecked")
    public static IOVersions<? extends BPlusLeafIO<SearchRow>> getVersions(int payload) {
        assert payload >= 0 && payload <= PageIO.MAX_PAYLOAD_SIZE;

        if (payload == 0)
            return H2LeafIO.VERSIONS;
        else
            return (IOVersions<BPlusLeafIO<SearchRow>>)PageIO.getLeafVersions((short)(payload - 1));
    }

    /**
     * @param type Type.
     * @param payload Payload size.
     * @return Versions.
     */
    private static IOVersions<H2ExtrasLeafIO> getVersions(short type, short payload) {
        return new IOVersions<>(new H2ExtrasLeafIO(type, 1, payload));
    }

    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param payloadSize Payload size.
     */
    private H2ExtrasLeafIO(short type, int ver, int payloadSize) {
        super(type, ver, 8 + payloadSize);
        this.payloadSize = payloadSize;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public void storeByOffset(long pageAddr, int off, SearchRow row) {
        GridH2Row row0 = (GridH2Row)row;

        assert row0.link != 0;

        List<InlineIndexHelper> inlineIdxs = InlineIndexHelper.getCurrentInlineIndexes();

        assert inlineIdxs != null : "no inline index helpers";

        int fieldOff = 0;

        for (int i = 0; i < inlineIdxs.size(); i++) {
            InlineIndexHelper idx = inlineIdxs.get(i);

            int size = idx.put(pageAddr, off + fieldOff, row.getValue(idx.columnIndex()), payloadSize - fieldOff);

            if (size == 0)
                break;

            fieldOff += size;
        }

        PageUtils.putLong(pageAddr, off + payloadSize, row0.link);
    }

    /** {@inheritDoc} */
    @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<SearchRow> srcIo, long srcPageAddr, int srcIdx) {
        int srcOff = srcIo.offset(srcIdx);

        byte[] payload = PageUtils.getBytes(srcPageAddr, srcOff, payloadSize);
        long link = PageUtils.getLong(srcPageAddr, srcOff + payloadSize);

        assert link != 0;

        int dstOff = offset(dstIdx);

        PageUtils.putBytes(dstPageAddr, dstOff, payload);
        PageUtils.putLong(dstPageAddr, dstOff + payloadSize, link);
    }

    /** {@inheritDoc} */
    @Override public SearchRow getLookupRow(BPlusTree<SearchRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {
        long link = getLink(pageAddr, idx);

        return ((H2Tree)tree).getRowFactory().getRow(link);
    }

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Link to row.
     */
    private long getLink(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + payloadSize);
    }
}
