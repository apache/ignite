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

package org.apache.ignite.internal.processors.query.h2.defragmentation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheData;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CacheDefragmentationContext;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.GridQueryIndexingDefragmentation;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.LinkMap;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.TreeIterator;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.InsertLast;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.InlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.AbstractInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.io.AbstractH2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.collection.IntMap;
import org.h2.index.Index;
import org.h2.value.Value;

/**
 *
 */
public class IndexingDefragmentation implements GridQueryIndexingDefragmentation {
    /** Indexing. */
    private final IgniteH2Indexing indexing;

    /** Constructor. */
    public IndexingDefragmentation(IgniteH2Indexing indexing) {
        this.indexing = indexing;
    }

    /** {@inheritDoc} */
    @Override public void defragmentate(
        CacheGroupContext grpCtx,
        CacheGroupContext newCtx,
        CacheDefragmentationContext defrgCtx,
        IntMap<LinkMap> mappingByPartition,
        IgniteLogger log
    ) throws IgniteCheckedException {
        int pageSize = grpCtx.cacheObjectContext().kernalContext().grid().configuration().getDataStorageConfiguration().getPageSize();

        TreeIterator treeIterator = new TreeIterator(pageSize);

        PageMemoryEx oldCachePageMem = (PageMemoryEx)grpCtx.dataRegion().pageMemory();

        PageMemory newCachePageMemory = defrgCtx.partitionsDataRegion().pageMemory();

        Collection<GridH2Table> tables = indexing.schemaManager().dataTables();

        for (GridH2Table table : tables) {
            GridH2RowDescriptor rowDescriptor = table.rowDescriptor();

            ArrayList<Index> indexes = table.getIndexes();
            H2TreeIndex index = (H2TreeIndex) indexes.get(2);

            GridCacheContext cctx = table.cacheContext();

            int segments = index.segmentsCount();

            H2Tree firstTree = index.treeForRead(0);

            H2TreeIndex newIndex = H2TreeIndex.createIndex(
                cctx,
                null,
                table,
                index.getName(),
                firstTree.getPk(),
                firstTree.getAffinityKey(),
                Arrays.asList(firstTree.cols()),
                Arrays.asList(firstTree.cols()),
                index.inlineSize(),
                segments,
                newCachePageMemory,
                (segIdx, treeName) -> newCtx.offheap().rootPageForIndex(cctx.cacheId(), treeName, segIdx),
                log
            );

            for (int i = 0; i < segments; i++) {
                H2Tree tree = index.treeForRead(i);

                treeIterator.iterate(tree, oldCachePageMem, (theTree, io, pageAddr, idx) -> {
                    BPlusIO<H2Row> h2IO = io;

                    if (h2IO instanceof H2ExtrasLeafIO)
                        h2IO = new H2LightweightExtrasLeafIO((H2ExtrasLeafIO) h2IO);
                    else if (h2IO instanceof H2LeafIO)
                        h2IO = new H2LightweightLeafIO((H2LeafIO) h2IO);
                    else if (h2IO instanceof H2MvccExtrasLeafIO)
                        h2IO = new H2LightweightMvccExtrasLeafIO((H2MvccExtrasLeafIO) h2IO);
                    else if (h2IO instanceof H2MvccLeafIO)
                        h2IO = new H2LightweightMvccLeafIO((H2MvccLeafIO) h2IO);

                    H2Row row = theTree.getRow(h2IO, pageAddr, idx);

                    if (row instanceof H2CacheRowWithIndex) {
                        H2CacheRowWithIndex h2CacheRow = (H2CacheRowWithIndex) row;

                        CacheDataRow cacheDataRow = h2CacheRow.getRow();

                        int partition = cacheDataRow.partition();

                        long link = h2CacheRow.link();

                        LinkMap map = mappingByPartition.get(partition);

                        long newLink = map.get(link);

                        H2CacheRowWithIndex newRow = H2CacheRowWithIndex.create(
                            rowDescriptor,
                            newLink,
                            h2CacheRow,
                            ((H2RowLinkIO) io).storeMvccInfo()
                        );

                        newIndex.putx(newRow);
                    }

                    return true;
                });
            }
        }
    }

    private static <T extends BPlusLeafIO<H2Row> & H2RowLinkIO> H2Row lookupRow(
        BPlusTree<H2Row, ?> tree,
        long pageAddr,
        int idx,
        T io
    ) throws IgniteCheckedException {
        long link = io.getLink(pageAddr, idx);

        List<InlineIndexColumn> inlineIdxs = ((H2Tree) tree).inlineIndexes();

        int off = io.offset(idx);

        List<Value> values = new ArrayList<>();

        if (inlineIdxs != null) {
            int fieldOff = 0;

            for (int i = 0; i < inlineIdxs.size(); i++) {
                AbstractInlineIndexColumn inlineIndexColumn = (AbstractInlineIndexColumn) inlineIdxs.get(i);

                Value value = inlineIndexColumn.get(pageAddr, off + fieldOff, ((AbstractH2ExtrasLeafIO) io).getPayloadSize() - fieldOff);

                fieldOff += inlineIndexColumn.inlineSizeOf(value);

                values.add(value);
            }
        }

        if (io.storeMvccInfo()) {
            long mvccCrdVer = io.getMvccCoordinatorVersion(pageAddr, idx);
            long mvccCntr = io.getMvccCounter(pageAddr, idx);
            int mvccOpCntr = io.getMvccOperationCounter(pageAddr, idx);

            H2CacheRow row = (H2CacheRow) ((H2Tree) tree).createMvccRow(link, mvccCrdVer, mvccCntr, mvccOpCntr, CacheDataRowAdapter.RowData.LINK_ONLY);

            return new H2CacheRowWithIndex(row.getDesc(), row.getRow(), values);
        }

        H2CacheRow row = (H2CacheRow) ((H2Tree) tree).createRow(link, false);

        return new H2CacheRowWithIndex(row.getDesc(), row.getRow(), values);
    }

    /**
     * Special version of H2ExtrasLeafIO which doesn't look up data partitions.
     */
    private static class H2LightweightExtrasLeafIO extends H2ExtrasLeafIO {
        /** Constructor. */
        public H2LightweightExtrasLeafIO(H2ExtrasLeafIO io) {
            super((short) io.getType(), io.getVersion(), io.getPayloadSize());
        }

        /** {@inheritDoc} */
        @Override public H2Row getLookupRow(BPlusTree<H2Row, ?> tree, long pageAddr, int idx) throws IgniteCheckedException {
            return lookupRow(tree, pageAddr, idx, this);
        }
    }

    private static class H2LightweightLeafIO extends H2LeafIO {

        public H2LightweightLeafIO(H2LeafIO io) {
            super(io.getVersion());
        }

        /** {@inheritDoc} */
        @Override public H2Row getLookupRow(BPlusTree<H2Row, ?> tree, long pageAddr, int idx) throws IgniteCheckedException {
            return lookupRow(tree, pageAddr, idx, this);
        }
    }

    private static class H2LightweightMvccLeafIO extends H2MvccLeafIO {

        public H2LightweightMvccLeafIO(H2MvccLeafIO io) {
            super(io.getVersion());
        }

        /** {@inheritDoc} */
        @Override public H2Row getLookupRow(BPlusTree<H2Row, ?> tree, long pageAddr, int idx) throws IgniteCheckedException {
            return lookupRow(tree, pageAddr, idx, this);
        }
    }

    private static class H2LightweightMvccExtrasLeafIO extends H2MvccExtrasLeafIO {

        public H2LightweightMvccExtrasLeafIO(H2MvccExtrasLeafIO io) {
            super((short) io.getType(), io.getVersion(), io.getPayloadSize());
        }

        /** {@inheritDoc} */
        @Override public H2Row getLookupRow(BPlusTree<H2Row, ?> tree, long pageAddr, int idx) throws IgniteCheckedException {
            return lookupRow(tree, pageAddr, idx, this);
        }
    }

    /**
     * H2CacheRow with stored index values
     */
    private static class H2CacheRowWithIndex extends H2CacheRow implements InsertLast {
        /** List of index values. */
        private final List<Value> values;

        /** Constructor. */
        public H2CacheRowWithIndex(GridH2RowDescriptor desc, CacheDataRow row, List<Value> values) {
            super(desc, row);
            this.values = values;
        }

        public static H2CacheRowWithIndex create(
            GridH2RowDescriptor desc,
            long newLink,
            H2CacheRowWithIndex oldValue,
            boolean storeMvcc
        ) {
            CacheDataRow row = oldValue.getRow();

            CacheDataRow newDataRow;

            if (storeMvcc) {
                newDataRow = new MvccDataRow(newLink);
                newDataRow.mvccVersion(row);
            } else
                newDataRow = new CacheDataRowAdapter(newLink);

            return new H2CacheRowWithIndex(desc, newDataRow, oldValue.values);
        }

        /** {@inheritDoc} */
        @Override public Value getValue(int col) {
            if (values.isEmpty())
                return null;

            return values.get(col);
        }

    }

}
