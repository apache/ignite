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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.query.h2.database.FastIndexHelper;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.h2.result.SearchRow;

/**
 * Inner page for H2 row references.
 */
public class H2ExtrasInnerIO extends BPlusInnerIO<SearchRow> {

    /** */
    public static final IOVersions<H2ExtrasInnerIO> VERSIONS_4 = new IOVersions<>(
        new H2ExtrasInnerIO(T_H2_EX4_REF_INNER, 1, 4)
    );

    /** */
    public static final IOVersions<H2ExtrasInnerIO> VERSIONS_8 = new IOVersions<>(
        new H2ExtrasInnerIO(T_H2_EX8_REF_INNER, 1, 8)
    );

    /** */
    public static final IOVersions<H2ExtrasInnerIO> VERSIONS_12 = new IOVersions<>(
        new H2ExtrasInnerIO(T_H2_EX12_REF_INNER, 1, 12)
    );

    /** */
    public static final IOVersions<H2ExtrasInnerIO> VERSIONS_16 = new IOVersions<>(
        new H2ExtrasInnerIO(T_H2_EX16_REF_INNER, 1, 16)
    );

    /** */
    public static final IOVersions<H2ExtrasInnerIO> VERSIONS_20 = new IOVersions<>(
        new H2ExtrasInnerIO(T_H2_EX20_REF_INNER, 1, 20)
    );

    /** */
    public static final IOVersions<H2ExtrasInnerIO> VERSIONS_24 = new IOVersions<>(
        new H2ExtrasInnerIO(T_H2_EX24_REF_INNER, 1, 24)
    );

    /** */
    public static final IOVersions<H2ExtrasInnerIO> VERSIONS_28 = new IOVersions<>(
        new H2ExtrasInnerIO(T_H2_EX28_REF_INNER, 1, 28)
    );

    /** */
    public static final IOVersions<H2ExtrasInnerIO> VERSIONS_32 = new IOVersions<>(
        new H2ExtrasInnerIO(T_H2_EX32_REF_INNER, 1, 32)
    );

    /** */
    private final int payloadSize;

    /**
     * @param ver Page format version.
     */
    private H2ExtrasInnerIO(short ref, int ver, int payloadSize) {
        super(ref, ver, true, 8 + payloadSize);
        this.payloadSize = payloadSize;
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(ByteBuffer buf, int off, SearchRow row) {
        GridH2Row row0 = (GridH2Row)row;

        assert row0.link != 0;

        H2Tree tree = H2TreeIndex.getCurrentTree();
        assert tree != null;
        List<FastIndexHelper> fastIdx = tree.fastIdxs();

        assert fastIdx != null;

        int fieldOff = 0;

        for (int i = 0; i < fastIdx.size(); i++) {
            FastIndexHelper idx = fastIdx.get(i);
            idx.put(buf, off + fieldOff, row.getValue(idx.columnIdx()));
            fieldOff += idx.size();
        }

        buf.putLong(off + payloadSize, row0.link);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(long pageAddr, int off, SearchRow row) {
        GridH2Row row0 = (GridH2Row)row;

        assert row0.link != 0;

        H2Tree tree = (H2Tree)H2TreeIndex.getCurrentTree();
        assert tree != null;
        List<FastIndexHelper> fastIdx = tree.fastIdxs();

        assert fastIdx != null;

        int fieldOff = 0;

        for (int i = 0; i < fastIdx.size(); i++) {
            FastIndexHelper idx = fastIdx.get(i);
            idx.put(pageAddr, off + fieldOff, row.getValue(idx.columnIdx()));
            fieldOff += idx.size();
        }

        PageUtils.putLong(pageAddr, off + payloadSize, row0.link);
    }

    /** {@inheritDoc} */
    @Override public SearchRow getLookupRow(BPlusTree<SearchRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {
        long link = getLink(pageAddr, idx);

        assert link != 0;

        GridH2Row r0 = ((H2Tree)tree).getRowFactory().getRow(link);

        return r0;
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

    /**
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Link to row.
     */
    private long getLink(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + payloadSize);
    }
}
