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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.InlineIndexHelper;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2SearchRow;

/**
 * Inner page for H2 row references.
 */
public abstract class AbstractH2ExtrasInnerIO extends BPlusInnerIO<GridH2SearchRow> implements H2RowLinkIO {
    /** Payload size. */
    protected final int payloadSize;

    /** */
    public static void register() {
        register(false);

        register(true);
    }

    /**
     * @param mvcc Mvcc flag.
     */
    private static void register(boolean mvcc) {
        short type = mvcc ? PageIO.T_H2_EX_REF_MVCC_INNER_START : PageIO.T_H2_EX_REF_INNER_START;

        for (short payload = 1; payload <= PageIO.MAX_PAYLOAD_SIZE; payload++) {
            IOVersions<? extends AbstractH2ExtrasInnerIO> io =
                getVersions((short)(type + payload - 1), payload, mvcc);

            PageIO.registerH2ExtraInner(io, mvcc);
        }
    }

    /**
     * @param payload Payload size.
     * @param mvccEnabled Mvcc flag.
     * @return IOVersions for given payload.
     */
    @SuppressWarnings("unchecked")
    public static IOVersions<? extends BPlusInnerIO<GridH2SearchRow>> getVersions(int payload, boolean mvccEnabled) {
        assert payload >= 0 && payload <= PageIO.MAX_PAYLOAD_SIZE;

        if (payload == 0)
            return mvccEnabled ? H2MvccInnerIO.VERSIONS : H2InnerIO.VERSIONS;
        else
            return (IOVersions<BPlusInnerIO<GridH2SearchRow>>)PageIO.getInnerVersions((short)(payload - 1), mvccEnabled);
    }

    /**
     * @param type Type.
     * @param payload Payload size.
     * @param mvcc Mvcc flag.
     * @return Instance of IO versions.
     */
    private static IOVersions<? extends AbstractH2ExtrasInnerIO> getVersions(short type, short payload, boolean mvcc) {
        return new IOVersions<>(mvcc ? new H2MvccExtrasInnerIO(type, 1, payload) : new H2ExtrasInnerIO(type, 1, payload));
    }

    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param itemSize Item size.
     * @param payloadSize Payload size.
     */
    AbstractH2ExtrasInnerIO(short type, int ver, int itemSize, int payloadSize) {
        super(type, ver, true, itemSize + payloadSize);

        this.payloadSize = payloadSize;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public final void storeByOffset(long pageAddr, int off, GridH2SearchRow row) {
        GridH2Row row0 = (GridH2Row)row;

        assert row0.link() != 0 : row0;

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

        H2IOUtils.storeRow(row0, pageAddr, off + payloadSize, storeMvccInfo());
    }

    /** {@inheritDoc} */
    @Override public final GridH2SearchRow getLookupRow(BPlusTree<GridH2SearchRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {
        long link = getLink(pageAddr, idx);

        assert link != 0;

        if (storeMvccInfo()) {
            long mvccCrdVer = getMvccCoordinatorVersion(pageAddr, idx);
            long mvccCntr = getMvccCounter(pageAddr, idx);
            int mvccOpCntr = getMvccOperationCounter(pageAddr, idx);

            return ((H2Tree)tree).createRowFromLink(link, mvccCrdVer, mvccCntr, mvccOpCntr);
        }

        return ((H2Tree)tree).createRowFromLink(link);
    }

    /** {@inheritDoc} */
    @Override public final void store(long dstPageAddr, int dstIdx, BPlusIO<GridH2SearchRow> srcIo, long srcPageAddr, int srcIdx) {
        int srcOff = srcIo.offset(srcIdx);

        byte[] payload = PageUtils.getBytes(srcPageAddr, srcOff, payloadSize);
        long link = PageUtils.getLong(srcPageAddr, srcOff + payloadSize);

        assert link != 0;

        int dstOff = offset(dstIdx);

        PageUtils.putBytes(dstPageAddr, dstOff, payload);

        H2IOUtils.store(dstPageAddr, dstOff + payloadSize, srcIo, srcPageAddr, srcIdx, storeMvccInfo());
    }

    /** {@inheritDoc} */
    @Override public final long getLink(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + payloadSize);
    }
}
