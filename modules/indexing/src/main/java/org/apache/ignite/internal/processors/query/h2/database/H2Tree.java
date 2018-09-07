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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2SearchRow;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 */
public abstract class H2Tree extends BPlusTree<GridH2SearchRow, GridH2Row> {
    /** */
    private final H2RowFactory rowStore;

    /** */
    private final int inlineSize;

    /** */
    private final List<InlineIndexHelper> inlineIdxs;

    /** */
    private final IndexColumn[] cols;

    /** */
    private final int[] columnIds;

    /** */
    private final boolean mvccEnabled;

    /** */
    private final Comparator<Value> comp = new Comparator<Value>() {
        @Override public int compare(Value o1, Value o2) {
            return compareValues(o1, o2);
        }
    };

    /** Row cache. */
    private final H2RowCache rowCache;

    /**
     * Constructor.
     *
     * @param name Tree name.
     * @param reuseList Reuse list.
     * @param grpId Cache group ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param rowStore Row data store.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @param rowCache Row cache.
     * @param mvccEnabled Mvcc flag.
     * @param failureProcessor if the tree is corrupted.
     * @throws IgniteCheckedException If failed.
     */
    protected H2Tree(
        String name,
        ReuseList reuseList,
        int grpId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        H2RowFactory rowStore,
        long metaPageId,
        boolean initNew,
        IndexColumn[] cols,
        List<InlineIndexHelper> inlineIdxs,
        int inlineSize,
        boolean mvccEnabled,
        @Nullable H2RowCache rowCache,
        @Nullable FailureProcessor failureProcessor
    ) throws IgniteCheckedException {
        super(name, grpId, pageMem, wal, globalRmvId, metaPageId, reuseList, failureProcessor);

        if (!initNew) {
            // Page is ready - read inline size from it.
            inlineSize = getMetaInlineSize();
        }

        this.inlineSize = inlineSize;
        this.mvccEnabled = mvccEnabled;

        assert rowStore != null;

        this.rowStore = rowStore;
        this.inlineIdxs = inlineIdxs;
        this.cols = cols;

        this.columnIds = new int[cols.length];

        for (int i = 0; i < cols.length; i++)
            columnIds[i] = cols[i].column.getColumnId();

        setIos(H2ExtrasInnerIO.getVersions(inlineSize, mvccEnabled), H2ExtrasLeafIO.getVersions(inlineSize, mvccEnabled));

        this.rowCache = rowCache;

        initTree(initNew, inlineSize);
    }

    /**
     * Create row from link.
     *
     * @param link Link.
     * @return Row.
     * @throws IgniteCheckedException if failed.
     */
    public GridH2Row createRowFromLink(long link) throws IgniteCheckedException {
        if (rowCache != null) {
            GridH2Row row = rowCache.get(link);

            if (row == null) {
                row = rowStore.getRow(link);

                if (row instanceof GridH2KeyValueRowOnheap)
                    rowCache.put((GridH2KeyValueRowOnheap)row);
            }

            return row;
        }
        else
            return rowStore.getRow(link);
    }

    /**
     * Create row from link.
     *
     * @param link Link.
     * @param mvccOpCntr
     * @return Row.
     * @throws IgniteCheckedException if failed.
     */
    public GridH2Row createRowFromLink(long link, long mvccCrdVer, long mvccCntr, int mvccOpCntr) throws IgniteCheckedException {
        if (rowCache != null) {
            GridH2Row row = rowCache.get(link);

            if (row == null) {
                row = rowStore.getMvccRow(link, mvccCrdVer, mvccCntr, mvccOpCntr);

                if (row instanceof GridH2KeyValueRowOnheap)
                    rowCache.put((GridH2KeyValueRowOnheap)row);
            }

            return row;
        }
        else
            return rowStore.getMvccRow(link, mvccCrdVer, mvccCntr, mvccOpCntr);
    }

    /** {@inheritDoc} */
    @Override public GridH2Row getRow(BPlusIO<GridH2SearchRow> io, long pageAddr, int idx, Object ignore)
        throws IgniteCheckedException {
        return (GridH2Row)io.getLookupRow(this, pageAddr, idx);
    }

    /**
     * @return Inline size.
     */
    private int inlineSize() {
        return inlineSize;
    }

    /**
     * @return Inline size.
     * @throws IgniteCheckedException If failed.
     */
    private int getMetaInlineSize() throws IgniteCheckedException {
        final long metaPage = acquirePage(metaPageId);

        try {
            long pageAddr = readLock(metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

                return io.getInlineSize(pageAddr);
            }
            finally {
                readUnlock(metaPageId, metaPage, pageAddr);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override protected int compare(BPlusIO<GridH2SearchRow> io, long pageAddr, int idx,
        GridH2SearchRow row) throws IgniteCheckedException {
        if (inlineSize() == 0)
            return compareRows(getRow(io, pageAddr, idx), row);
        else {
            int off = io.offset(idx);

            int fieldOff = 0;

            int lastIdxUsed = 0;

            for (int i = 0; i < inlineIdxs.size(); i++) {
                InlineIndexHelper inlineIdx = inlineIdxs.get(i);

                Value v2 = row.getValue(inlineIdx.columnIndex());

                if (v2 == null)
                    return 0;

                int c = inlineIdx.compare(pageAddr, off + fieldOff, inlineSize() - fieldOff, v2, comp);

                if (c == -2)
                    break;

                lastIdxUsed++;

                if (c != 0)
                    return c;

                fieldOff += inlineIdx.fullSize(pageAddr, off + fieldOff);

                if (fieldOff > inlineSize())
                    break;
            }

            if (lastIdxUsed == cols.length)
                return mvccCompare((H2RowLinkIO)io, pageAddr, idx, row);

            SearchRow rowData = getRow(io, pageAddr, idx);

            for (int i = lastIdxUsed, len = cols.length; i < len; i++) {
                IndexColumn col = cols[i];
                int idx0 = col.column.getColumnId();

                Value v2 = row.getValue(idx0);

                if (v2 == null) {
                    // Can't compare further.
                    return mvccCompare((H2RowLinkIO)io, pageAddr, idx, row);
                }

                Value v1 = rowData.getValue(idx0);

                int c = compareValues(v1, v2);

                if (c != 0)
                    return InlineIndexHelper.fixSort(c, col.sortType);
            }

            return mvccCompare((H2RowLinkIO)io, pageAddr, idx, row);
        }
    }

    /**
     * Compares two H2 rows.
     *
     * @param r1 Row 1.
     * @param r2 Row 2.
     * @return Compare result: see {@link Comparator#compare(Object, Object)} for values.
     */
    public int compareRows(GridH2SearchRow r1, GridH2SearchRow r2) {
        assert !mvccEnabled || r2.indexSearchRow() || MvccUtils.mvccVersionIsValid(r2.mvccCoordinatorVersion(), r2.mvccCounter()) : r2;
        if (r1 == r2)
            return 0;

        for (int i = 0, len = cols.length; i < len; i++) {
            int idx = columnIds[i];

            Value v1 = r1.getValue(idx);
            Value v2 = r2.getValue(idx);

            if (v1 == null || v2 == null) {
                // Can't compare further.
                return mvccCompare(r1, r2);
            }

            int c = compareValues(v1, v2);

            if (c != 0)
                return InlineIndexHelper.fixSort(c, cols[i].sortType);
        }

        return mvccCompare(r1, r2);
    }

    /**
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @param r2 Search row.
     * @return Comparison result.
     */
    private int mvccCompare(H2RowLinkIO io, long pageAddr, int idx, GridH2SearchRow r2) {
        if (!mvccEnabled || r2.indexSearchRow())
            return 0;

        long crd = io.getMvccCoordinatorVersion(pageAddr, idx);
        long cntr = io.getMvccCounter(pageAddr, idx);
        int opCntr = io.getMvccOperationCounter(pageAddr, idx);

        assert MvccUtils.mvccVersionIsValid(crd, cntr, opCntr);

        return -MvccUtils.compare(crd, cntr, opCntr, r2);  // descending order
    }

    /**
     * @param r1 First row.
     * @param r2 Second row.
     * @return Comparison result.
     */
    private int mvccCompare(GridH2SearchRow r1, GridH2SearchRow r2) {
        if (!mvccEnabled || r2.indexSearchRow())
            return 0;

        long crdVer1 = r1.mvccCoordinatorVersion();
        long crdVer2 = r2.mvccCoordinatorVersion();

        int c = -Long.compare(crdVer1, crdVer2);

        if (c != 0)
            return c;

        return -Long.compare(r1.mvccCounter(), r2.mvccCounter());
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return Comparison result.
     */
    public abstract int compareValues(Value v1, Value v2);

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2Tree.class, this, "super", super.toString());
    }
}
