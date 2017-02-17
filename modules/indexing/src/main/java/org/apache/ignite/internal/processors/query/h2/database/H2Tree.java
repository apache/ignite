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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.value.Value;

/**
 */
public abstract class H2Tree extends BPlusTree<SearchRow, GridH2Row> {
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

    /**
     * @param name Tree name.
     * @param reuseList Reuse list.
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param rowStore Row data store.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    protected H2Tree(
        String name,
        ReuseList reuseList,
        int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        H2RowFactory rowStore,
        long metaPageId,
        boolean initNew,
        IndexColumn[] cols,
        List<InlineIndexHelper> inlineIdxs,
        int inlineSize
    ) throws IgniteCheckedException {
        super(name, cacheId, pageMem, wal, globalRmvId, metaPageId, reuseList);

        if (!initNew) {
            // Page is ready - read inline size from it.
            inlineSize = getMetaInlineSize();
        }

        this.inlineSize = inlineSize;

        assert rowStore != null;

        this.rowStore = rowStore;
        this.inlineIdxs = inlineIdxs;
        this.cols = cols;

        this.columnIds = new int[cols.length];
        for (int i = 0; i < cols.length; i++)
            columnIds[i] = cols[i].column.getColumnId();

        setIos(H2ExtrasInnerIO.getVersions(inlineSize), H2ExtrasLeafIO.getVersions(inlineSize));

        initTree(initNew, inlineSize);
    }

    /**
     * @return Row store.
     */
    public H2RowFactory getRowFactory() {
        return rowStore;
    }

    /** {@inheritDoc} */
    @Override protected GridH2Row getRow(BPlusIO<SearchRow> io, long pageAddr, int idx)
        throws IgniteCheckedException {
        return (GridH2Row)io.getLookupRow(this, pageAddr, idx);
    }

    /**
     * @return Inline size.
     */
    public int inlineSize() {
        return inlineSize;
    }

    /**
     * @return Inline size.
     * @throws IgniteCheckedException If failed.
     */
    private int getMetaInlineSize() throws IgniteCheckedException {
        try (Page meta = page(metaPageId)) {
            long pageAddr = readLock(meta); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [page=" + meta + ", metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

                return io.getInlineSize(pageAddr);
            }
            finally {
                readUnlock(meta, pageAddr);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<SearchRow> io, long pageAddr, int idx,
        SearchRow row) throws IgniteCheckedException {
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

                Value v1 = inlineIdx.get(pageAddr, off + fieldOff, inlineSize() - fieldOff);

                if (v1 == null)
                    break;

                int c = compareValues(v1, v2, inlineIdx.sortType());

                if (!canRelyOnCompare(c, v1, v2, inlineIdx))
                    break;

                lastIdxUsed++;

                if (c != 0)
                    return c;

                fieldOff += inlineIdx.fullSize(pageAddr, off + fieldOff);

                if (fieldOff > inlineSize())
                    break;
            }

            if (lastIdxUsed == cols.length)
                return 0;

            SearchRow rowData = getRow(io, pageAddr, idx);

            for (int i = lastIdxUsed, len = cols.length; i < len; i++) {
                IndexColumn col = cols[i];
                int idx0 = col.column.getColumnId();

                Value v2 = row.getValue(idx0);
                if (v2 == null) {
                    // Can't compare further.
                    return 0;
                }

                Value v1 = rowData.getValue(idx0);

                int c = compareValues(v1, v2, col.sortType);
                if (c != 0)
                    return c;
            }

            return 0;
        }
    }

    /**
     * @param c Compare result.
     * @param shortVal Short value.
     * @param v2 Second value;
     * @param inlineIdx Index helper.
     * @return {@code true} if we can rely on compare result.
     */
    protected static boolean canRelyOnCompare(int c, Value shortVal, Value v2, InlineIndexHelper inlineIdx) {
        if (inlineIdx.type() == Value.STRING) {
            if (c == 0 && shortVal.getType() != Value.NULL && v2.getType() != Value.NULL)
                return false;

            if (shortVal.getType() != Value.NULL
                && v2.getType() != Value.NULL
                && ((c < 0 && inlineIdx.sortType() == SortOrder.ASCENDING) || (c > 0 && inlineIdx.sortType() == SortOrder.DESCENDING))
                && shortVal.getString().length() <= v2.getString().length()) {
                // Can't rely on compare, should use full string.
                return false;
            }
        }

        return true;
    }

    /**
     * Compare two rows.
     *
     * @param r1 Row 1.
     * @param r2 Row 2.
     * @return Compare result.
     */
    private int compareRows(GridH2Row r1, SearchRow r2) {
        if (r1 == r2)
            return 0;

        for (int i = 0, len = cols.length; i < len; i++) {
            int index = columnIds[i];
            Value v1 = r1.getValue(index);
            Value v2 = r2.getValue(index);
            if (v1 == null || v2 == null) {
                // can't compare further
                return 0;
            }
            int c = compareValues(v1, v2, cols[i].sortType);
            if (c != 0)
                return c;

        }
        return 0;
    }

    public abstract int compareValues(Value v1, Value v2, int order);


}


