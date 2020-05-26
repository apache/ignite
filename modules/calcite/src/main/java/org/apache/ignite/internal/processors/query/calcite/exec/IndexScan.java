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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridIndex;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeFilterClosure;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilterImpl;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * Scan on index.
 */
public class IndexScan<Row> implements Iterable<Row> {
    /** */
    private final ExecutionContext<Row> ectx;

    /** */
    private final CacheObjectContext coCtx;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final GridCacheContext<?, ?> cacheCtx;

    /** */
    private final TableDescriptor desc;

    /** */
    private final RowFactory<Row> factory;

    /** */
    private final GridIndex<H2Row> idx;

    /** */
    private final AffinityTopologyVersion topVer;

    /** Additional filters. */
    private final Predicate<Row> filters;

    /** Lower index scan bound. */
    private final Row lowerBound;

    /** Upper index scan bound. */
    private final Row upperBound;

    /** */
    private final int[] partsArr;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    private List<GridDhtLocalPartition> partsToReserve;

    /**
     * @param ctx Cache context.
     * @param igniteIdx Index tree.
     * @param filters Additional filters.
     * @param lowerBound Lower index scan bound.
     * @param upperBound Upper index scan bound.
     */
    public IndexScan(
        ExecutionContext<Row> ctx,
        IgniteIndex igniteIdx,
        Predicate<Row> filters,
        Row lowerBound,
        Row upperBound
    ) {
        ectx = ctx;
        desc = igniteIdx.table().descriptor();
        idx = igniteIdx.index();
        this.filters = filters;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        cacheCtx = igniteIdx.table().descriptor().cacheContext();
        coCtx = cacheCtx.cacheObjectContext();
        this.ctx = coCtx.kernalContext();
        topVer = ctx.planningContext().topologyVersion();
        partsArr = ctx.partitions();
        mvccSnapshot = ctx.mvccSnapshot();

        RelDataType rowType = desc.selectRowType(ectx.getTypeFactory());
        factory = ectx.rowHandler().factory(ectx.getTypeFactory(), rowType);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> iterator() {
        H2TreeFilterClosure filterC = filterClosure();

        H2Row lower = lowerBound == null ? null : new CalciteH2Row<>(coCtx, ectx, lowerBound);
        H2Row upper = upperBound == null ? null : new CalciteH2Row<>(coCtx, ectx, upperBound);

        reservePartitions();

        GridCursor<H2Row> cur = idx.find(lower, upper, filterC);

        return new CursorIteratorWrapper(cur);
    }

    /** */
    public H2TreeFilterClosure filterClosure() {
        IndexingQueryFilter filter = new IndexingQueryFilterImpl(ctx, topVer, partsArr);
        IndexingQueryCacheFilter f = filter.forCache(cacheCtx.name());
        H2TreeFilterClosure filterC = null;

        if (f != null || mvccSnapshot != null )
            filterC = new H2TreeFilterClosure(f, mvccSnapshot, cacheCtx, ectx.planningContext().logger());

        return filterC;
    }

    /** */
    private void reservePartitions() {
        assert partsToReserve == null : partsToReserve;

        partsToReserve = gatherPartitions(cacheCtx, partsArr);

        for (GridDhtLocalPartition part : partsToReserve) {
            if (part == null || !part.reserve())
                throw reservationException();
            else if (part.state() != GridDhtPartitionState.OWNING) {
                part.release();

                throw reservationException();
            }
        }
    }

    /** */
    private List<GridDhtLocalPartition> gatherPartitions(GridCacheContext<?, ?> ctx, int[] arr ) {
        if (ctx.isReplicated()) {
            int partsCnt = ctx.affinity().partitions();
            GridDhtPartitionTopology top = ctx.topology();
            List<GridDhtLocalPartition> parts = new ArrayList<>(partsCnt);

            for (int i = 0; i < partsCnt; i++)
                parts.add(top.localPartition(i));

            return parts;
        }
        else if (ctx.isPartitioned()){
            assert arr != null;
            List<GridDhtLocalPartition> parts = new ArrayList<>(arr.length);
            GridDhtPartitionTopology top = ctx.topology();

            for (int i = 0; i < arr.length; i++)
                parts.add(top.localPartition(arr[i]));

            return parts;
        }
        else {
            assert ctx.isLocal();

            return Collections.emptyList();
        }
    }

    /** */
    private void releasePartitions() {
        assert partsToReserve != null;

        for (GridDhtLocalPartition part : partsToReserve)
            part.release();
    }

    /** */
    private IgniteSQLException reservationException() {
        return new IgniteSQLException("Failed to reserve partition for query execution. Retry on stable topology.");
    }

    /** */
    private class CursorIteratorWrapper extends GridCloseableIteratorAdapter<Row> {
        /** */
        private final GridCursor<H2Row> cursor;

        /** Next element. */
        private Row next;

        /**
         * @param cursor Cursor.
         */
        CursorIteratorWrapper(GridCursor<H2Row> cursor) {
            assert cursor != null;
            this.cursor = cursor;
        }

        /** {@inheritDoc} */
        @Override protected Row onNext() {
            if (next == null)
                throw new NoSuchElementException();

            Row res = next;

            next = null;

            return res;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            if (next != null)
                return true;

            while (next == null && cursor.next()) {
                H2Row h2Row = cursor.get();

                Row r = desc.toRow(ectx, (CacheDataRow)h2Row, factory);

                if (filters == null || filters.test(r))
                    next = r;
            }
            return next != null;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() {
            releasePartitions();
        }
    }

    /** */
    private static class CalciteH2Row<Row> extends H2Row {
        /** */
        private final Value[] values;

        /** */
        CalciteH2Row(CacheObjectValueContext coCtx, ExecutionContext<Row> ctx, Row row) {
            try {
                RowHandler<Row> rowHnd = ctx.rowHandler();

                int colCnt = rowHnd.columnCount(row);

                values = new Value[colCnt];

                for (int i = 0; i < colCnt; i++) {
                    Object o = rowHnd.get(i, row);

                    if (o != null) {
                        Value v = H2Utils.wrap(coCtx, o, DataType.getTypeFromClass(o.getClass()));

                        values[i] = v;
                    }
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to wrap object into H2 Value.", e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean indexSearchRow() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public int getColumnCount() {
            return values.length;
        }

        /** {@inheritDoc} */
        @Override public Value getValue(int idx) {
            return values[idx];
        }

        /** {@inheritDoc} */
        @Override public void setValue(int idx, Value v) {
            throw new AssertionError("Not supported.");
        }
    }
}
