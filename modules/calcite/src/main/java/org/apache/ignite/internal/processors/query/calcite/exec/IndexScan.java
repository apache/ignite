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
package org.apache.ignite.internal.processors.query.calcite.exec;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexSearchRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InlineIO;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilterImpl;
import org.jetbrains.annotations.Nullable;

/**
 * Scan on index.
 */
public class IndexScan<Row> extends AbstractIndexScan<Row, IndexRow> {
    /** */
    private final GridKernalContext kctx;

    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final CacheTableDescriptor desc;

    /** */
    private final RowFactory<Row> factory;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final int[] parts;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    private volatile List<GridDhtLocalPartition> reserved;

    /** */
    private final ImmutableBitSet requiredColumns;

    /** */
    protected final InlineIndex idx;

    /** Mapping from index keys to row fields. */
    private final ImmutableIntList idxFieldMapping;

    /** Mapping from row fields to index keys. */
    private final int[] fieldIdxMapping;

    /** */
    private final ImmutableBitSet requiredInlineKeys;

    /** */
    private final InlineIndexKeyType[] inlineKeyTypes;

    /** Types of key fields stored in index. */
    private final Type[] fieldsStoreTypes;

    /**
     * @param ectx Execution context.
     * @param desc Table descriptor.
     * @param idxFieldMapping Mapping from index keys to row fields.
     * @param idx Physical index.
     * @param filters Additional filters.
     * @param ranges Index scan bounds.
     */
    public IndexScan(
        ExecutionContext<Row> ectx,
        CacheTableDescriptor desc,
        InlineIndex idx,
        ImmutableIntList idxFieldMapping,
        int[] parts,
        Predicate<Row> filters,
        RangeIterable<Row> ranges,
        Function<Row, Row> rowTransformer,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        this(ectx, desc, new TreeIndexWrapper(idx), idxFieldMapping, parts, filters, ranges, rowTransformer,
            requiredColumns);
    }

    /**
     * @param ectx Execution context.
     * @param desc Table descriptor.
     * @param idxFieldMapping Mapping from index keys to row fields.
     * @param treeIdx Physical index wrapper.
     * @param filters Additional filters.
     * @param ranges Index scan bounds.
     */
    protected IndexScan(
        ExecutionContext<Row> ectx,
        CacheTableDescriptor desc,
        TreeIndexWrapper treeIdx,
        ImmutableIntList idxFieldMapping,
        int[] parts,
        Predicate<Row> filters,
        RangeIterable<Row> ranges,
        Function<Row, Row> rowTransformer,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        super(
            ectx,
            desc.rowType(ectx.getTypeFactory(), requiredColumns),
            treeIdx,
            filters,
            ranges,
            rowTransformer
        );

        this.desc = desc;
        this.idx = treeIdx.idx;
        cctx = desc.cacheContext();
        kctx = cctx.kernalContext();

        factory = ectx.rowHandler().factory(ectx.getTypeFactory(), rowType);
        topVer = ectx.topologyVersion();
        this.parts = parts;
        mvccSnapshot = ectx.mvccSnapshot();
        this.requiredColumns = requiredColumns;
        this.idxFieldMapping = idxFieldMapping;

        RelDataType srcRowType = desc.rowType(ectx.getTypeFactory(), null);
        IgniteTypeFactory typeFactory = ectx.getTypeFactory();
        fieldsStoreTypes = new Type[srcRowType.getFieldCount()];

        for (int i = 0; i < srcRowType.getFieldCount(); i++)
            fieldsStoreTypes[i] = typeFactory.getResultClass(srcRowType.getFieldList().get(i).getType());

        // Check if we can use inlined index keys instead of cache row iteration.
        List<InlineIndexKeyType> inlinedKeys = idx.segment(0).rowHandler().inlineIndexKeyTypes();

        if (inlinedKeys.size() < (requiredColumns == null ? srcRowType.getFieldCount() : requiredColumns.cardinality())) {
            fieldIdxMapping = null;
            requiredInlineKeys = null;
            inlineKeyTypes = null;

            return;
        }

        ImmutableBitSet reqCols = requiredColumns == null ? ImmutableBitSet.range(0, srcRowType.getFieldCount()) :
            requiredColumns;

        int maxInlinedKey = -1;

        ImmutableBitSet.Builder requiredInlineKeysBuilder = ImmutableBitSet.builder();
        int [] locFieldIdxMapping = new int[rowType.getFieldCount()];

        for (int i = 0, j = reqCols.nextSetBit(0); j != -1; j = reqCols.nextSetBit(j + 1), i++) {
            // j = source field index, i = target field index.
            int keyIdx = idxFieldMapping.indexOf(j);

            if (keyIdx >= 0 && keyIdx < inlinedKeys.size()) {
                if (keyIdx > maxInlinedKey)
                    maxInlinedKey = keyIdx;

                requiredInlineKeysBuilder.set(keyIdx);
                locFieldIdxMapping[i] = keyIdx;
            }
            else {
                fieldIdxMapping = null;
                requiredInlineKeys = null;
                inlineKeyTypes = null;

                return;
            }
        }

        fieldIdxMapping = locFieldIdxMapping;
        requiredInlineKeys = requiredInlineKeysBuilder.build();
        inlineKeyTypes = inlinedKeys.subList(0, maxInlinedKey + 1).toArray(new InlineIndexKeyType[maxInlinedKey + 1]);
    }

    /** {@inheritDoc} */
    @Override public synchronized Iterator<Row> iterator() {
        reserve();

        try {
            return super.iterator();
        }
        catch (Exception e) {
            release();

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override protected IndexRow row2indexRow(Row bound) {
        if (bound == null)
            return null;

        InlineIndexRowHandler idxRowHnd = idx.segment(0).rowHandler();
        RowHandler<Row> rowHnd = ectx.rowHandler();

        IndexKey[] keys = new IndexKey[idxRowHnd.indexKeyDefinitions().size()];

        assert keys.length >= idxFieldMapping.size() : "Unexpected index keys [keys.length=" + keys.length +
            ", idxFieldMapping.size()=" + idxFieldMapping.size() + ']';

        boolean nullSearchRow = true;

        for (int i = 0; i < idxFieldMapping.size(); ++i) {
            int fieldIdx = idxFieldMapping.getInt(i);
            Object key = rowHnd.get(fieldIdx, bound);

            if (key != ectx.unspecifiedValue()) {
                key = TypeUtils.fromInternal(ectx, key, fieldsStoreTypes[fieldIdx]);

                keys[i] = IndexKeyFactory.wrap(key, idxRowHnd.indexKeyDefinitions().get(i).idxType(),
                    cctx.cacheObjectContext(), idxRowHnd.indexKeyTypeSettings());

                nullSearchRow = false;
            }
        }

        return nullSearchRow ? null : new IndexSearchRowImpl(keys, idxRowHnd);
    }

    /** {@inheritDoc} */
    @Override protected Row indexRow2Row(IndexRow row) throws IgniteCheckedException {
        if (row.indexSearchRow())
            return inlineIndexRow2Row(row);
        else
            return desc.toRow(ectx, row.cacheDataRow(), factory, requiredColumns);
    }

    /** */
    private Row inlineIndexRow2Row(IndexRow row) {
        RowHandler<Row> hnd = ectx.rowHandler();

        Row res = factory.create();

        for (int i = 0; i < fieldIdxMapping.length; i++)
            hnd.set(i, res, TypeUtils.toInternal(ectx, row.key(fieldIdxMapping[i]).key()));

        System.out.println(hnd.get(0, res));
        return res;
    }

    /** */
    @Override public void close() {
        release();
    }

    /** */
    private synchronized void reserve() {
        if (reserved != null)
            return;

        GridDhtPartitionTopology top = cctx.topology();
        top.readLock();

        GridDhtTopologyFuture topFut = top.topologyVersionFuture();

        boolean done = topFut.isDone();

        if (!done || !(topFut.topologyVersion().compareTo(topVer) >= 0
            && cctx.shared().exchange().lastAffinityChangedTopologyVersion(topFut.initialVersion()).compareTo(topVer) <= 0)) {
            top.readUnlock();

            throw new ClusterTopologyException("Topology was changed. Please retry on stable topology.");
        }

        List<GridDhtLocalPartition> toReserve;

        if (cctx.isReplicated()) {
            int partsCnt = cctx.affinity().partitions();
            toReserve = new ArrayList<>(partsCnt);
            for (int i = 0; i < partsCnt; i++)
                toReserve.add(top.localPartition(i));
        }
        else if (cctx.isPartitioned()) {
            assert parts != null;

            toReserve = new ArrayList<>(parts.length);
            for (int i = 0; i < parts.length; i++)
                toReserve.add(top.localPartition(parts[i]));
        }
        else
            toReserve = Collections.emptyList();

        reserved = new ArrayList<>(toReserve.size());

        try {
            for (GridDhtLocalPartition part : toReserve) {
                if (part == null || !part.reserve()) {
                    throw new ClusterTopologyException(
                        "Failed to reserve partition for query execution. Retry on stable topology."
                    );
                }
                else if (part.state() != GridDhtPartitionState.OWNING) {
                    part.release();

                    throw new ClusterTopologyException(
                        "Failed to reserve partition for query execution. Retry on stable topology."
                    );
                }

                reserved.add(part);
            }
        }
        catch (Exception e) {
            release();

            throw e;
        }
        finally {
            top.readUnlock();
        }
    }

    /** */
    private synchronized void release() {
        if (reserved == null)
            return;

        for (GridDhtLocalPartition part : reserved)
            part.release();

        reserved = null;
    }

    /** {@inheritDoc} */
    @Override protected IndexQueryContext indexQueryContext() {
        IndexingQueryFilter filter = new IndexingQueryFilterImpl(kctx, topVer, parts);

        InlineIndexRowFactory rowFactory = inlineKeyTypes == null || requiredInlineKeys == null ? null :
            new InlineIndexRowFactory(inlineKeyTypes, idx.segment(0).rowHandler(), requiredInlineKeys);

        return new IndexQueryContext(filter, null, rowFactory, mvccSnapshot);
    }

    /** */
    private static class InlineIndexRowFactory implements BPlusTree.TreeRowFactory<IndexRow, IndexRow> {
        /** Inline key types. */
        private final InlineIndexKeyType[] keyTypes;

        /** */
        private final InlineIndexRowHandler idxRowHnd;

        /** */
        private final ImmutableBitSet requiredKeys;

        /** */
        private InlineIndexRowFactory(
            InlineIndexKeyType[] keyTypes,
            InlineIndexRowHandler idxRowHnd,
            ImmutableBitSet requiredKeys
        ) {
            this.keyTypes = keyTypes;
            this.idxRowHnd = idxRowHnd;
            this.requiredKeys = requiredKeys;
        }

        /** {@inheritDoc} */
        @Override public IndexRow create(
            BPlusTree<IndexRow, IndexRow> tree,
            BPlusIO<IndexRow> io,
            long pageAddr,
            int idx
        ) throws IgniteCheckedException {
            int inlineSize = ((InlineIO)io).inlineSize();
            int rowOffset = io.offset(idx);
            int keyOffset = 0;

            // Check if all required keys is inlined before creating index row.
            for (int keyIdx = 0; keyIdx < keyTypes.length; keyIdx++) {
                InlineIndexKeyType keyType = keyTypes[keyIdx];

                if (!keyType.inlinedFullValue(pageAddr, rowOffset + keyOffset, inlineSize - keyOffset))
                    return io.getLookupRow(tree, pageAddr, idx);

                keyOffset += keyType.inlineSize(pageAddr, rowOffset + keyOffset);
            }

            // Second loop to create index row.
            IndexKey[] keys = new IndexKey[keyTypes.length];
            keyOffset = 0;

            for (int keyIdx = 0; keyIdx < keyTypes.length; keyIdx++) {
                InlineIndexKeyType keyType = keyTypes[keyIdx];

                if (requiredKeys.get(keyIdx))
                    keys[keyIdx] = keyType.get(pageAddr, rowOffset + keyOffset, inlineSize - keyOffset);

                keyOffset += keyType.inlineSize(pageAddr, rowOffset + keyOffset);
            }

            return new IndexSearchRowImpl(keys, idxRowHnd);
        }
    }

    /** */
    protected static class TreeIndexWrapper implements TreeIndex<IndexRow> {
        /** Underlying index. */
        protected final InlineIndex idx;

        /** */
        protected TreeIndexWrapper(InlineIndex idx) {
            this.idx = idx;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<IndexRow> find(
            IndexRow lower,
            IndexRow upper,
            boolean lowerInclude,
            boolean upperInclude,
            IndexQueryContext qctx
        ) {
            try {
                return idx.find(lower, upper, lowerInclude, upperInclude, qctx);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to find index rows", e);
            }
        }
    }
}
