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
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.BoundsValues;
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
    private final InlineIndex idx;

    /** Mapping from index keys to row fields. */
    private final ImmutableIntList idxFieldMapping;

    /** Types of key fields stored in index. */
    private final Type[] fieldsStoreTypes;

    /**
     * @param ectx Execution context.
     * @param desc Table descriptor.
     * @param idxFieldMapping Mapping from index keys to row fields.
     * @param idx Phisycal index.
     * @param filters Additional filters.
     * @param boundsValues Index scan bounds.
     */
    public IndexScan(
        ExecutionContext<Row> ectx,
        CacheTableDescriptor desc,
        InlineIndex idx,
        ImmutableIntList idxFieldMapping,
        int[] parts,
        Predicate<Row> filters,
        Iterable<BoundsValues<Row>> boundsValues,
        Function<Row, Row> rowTransformer,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        super(
            ectx,
            desc.rowType(ectx.getTypeFactory(), requiredColumns),
            new TreeIndexWrapper(idx),
            filters,
            boundsValues,
            rowTransformer
        );

        this.desc = desc;
        this.idx = idx;
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
        return desc.toRow(ectx, row.cacheDataRow(), factory, requiredColumns);
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
        return new IndexQueryContext(filter, null, mvccSnapshot);
    }

    /** */
    private static class TreeIndexWrapper implements TreeIndex<IndexRow> {
        /** Underlying index. */
        private final InlineIndex idx;

        /** */
        private TreeIndexWrapper(InlineIndex idx) {
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
