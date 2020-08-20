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
import org.apache.ignite.internal.processors.query.h2.opt.H2PlainRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilterImpl;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;

/**
 * Scan on index.
 */
public class IndexScan<Row> implements Iterable<Row>, AutoCloseable {
    /** */
    private final GridKernalContext kctx;

    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final ExecutionContext<Row> ectx;

    /** */
    private final CacheObjectContext coCtx;

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
    private List<GridDhtLocalPartition> reserved;

    /**
     * @param ectx Cache context.
     * @param igniteIdx Index tree.
     * @param filters Additional filters.
     * @param lowerBound Lower index scan bound.
     * @param upperBound Upper index scan bound.
     */
    public IndexScan(
        ExecutionContext<Row> ectx,
        IgniteIndex igniteIdx,
        Predicate<Row> filters,
        Row lowerBound,
        Row upperBound
    ) {
        this.ectx = ectx;
        desc = igniteIdx.table().descriptor();
        cctx = desc.cacheContext();
        kctx = cctx.kernalContext();
        coCtx = cctx.cacheObjectContext();

        RelDataType rowType = desc.selectRowType(this.ectx.getTypeFactory());

        factory = this.ectx.rowHandler().factory(this.ectx.getTypeFactory(), rowType);
        idx = igniteIdx.index();
        topVer = ectx.planningContext().topologyVersion();
        this.filters = filters;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        partsArr = ectx.partitions();
        mvccSnapshot = ectx.mvccSnapshot();
    }

    /** {@inheritDoc} */
    @Override public synchronized Iterator<Row> iterator() {
        reserve();
        try {
            H2Row lower = lowerBound == null ? null : new H2PlainRow(values(coCtx, ectx, lowerBound));
            H2Row upper = upperBound == null ? null : new H2PlainRow(values(coCtx, ectx, upperBound));

            return new IteratorImpl(idx.find(lower, upper, filterClosure()));
        }
        catch (Exception e) {
            release();

            throw e;
        }
    }

    /** */
    @Override public void close() {
        release();
    }

    /** */
    private synchronized void reserve() {
        if (reserved != null)
            return;

        List<GridDhtLocalPartition> toReserve;

        if (cctx.isReplicated()) {
            int partsCnt = cctx.affinity().partitions();
            toReserve = new ArrayList<>(partsCnt);
            GridDhtPartitionTopology top = cctx.topology();
            for (int i = 0; i < partsCnt; i++)
                toReserve.add(top.localPartition(i));
        }
        else if (cctx.isPartitioned()) {
            assert partsArr != null;

            toReserve = new ArrayList<>(partsArr.length);
            GridDhtPartitionTopology top = cctx.topology();
            for (int i = 0; i < partsArr.length; i++)
                toReserve.add(top.localPartition(partsArr[i]));
        }
        else {
            assert cctx.isLocal();

            toReserve = Collections.emptyList();
        }

        reserved = new ArrayList<>(toReserve.size());

        try {
            for (GridDhtLocalPartition part : toReserve) {
                if (part == null || !part.reserve())
                    throw new IgniteSQLException("Failed to reserve partition for query execution. Retry on stable topology.");
                else if (part.state() != GridDhtPartitionState.OWNING) {
                    part.release();

                    throw new IgniteSQLException("Failed to reserve partition for query execution. Retry on stable topology.");
                }

                reserved.add(part);
            }
        }
        catch (Exception e) {
            release();

            throw e;
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

    /** */
    private H2TreeFilterClosure filterClosure() {
        IndexingQueryFilter filter = new IndexingQueryFilterImpl(kctx, topVer, partsArr);
        IndexingQueryCacheFilter f = filter.forCache(cctx.name());
        H2TreeFilterClosure filterC = null;

        if (f != null || mvccSnapshot != null )
            filterC = new H2TreeFilterClosure(f, mvccSnapshot, cctx, ectx.planningContext().logger());

        return filterC;
    }

    /** */
    private Value[] values(CacheObjectValueContext cctx, ExecutionContext<Row> ectx, Row row) {
        try {
            RowHandler<Row> rowHnd = ectx.rowHandler();
            int rowLen = rowHnd.columnCount(row);

            Value[] values = new Value[rowLen];
            for (int i = 0; i < rowLen; i++) {
                Object o = rowHnd.get(i, row);

                if (o != null)
                    values[i] = H2Utils.wrap(cctx, o, DataType.getTypeFromClass(o.getClass()));
            }

            return values;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wrap object into H2 Value.", e);
        }
    }

    /** */
    private class IteratorImpl extends GridIteratorAdapter<Row> {
        /** */
        private final GridCursor<H2Row> cursor;

        /** Next element. */
        private Row next;

        /** */
        public IteratorImpl(@NotNull GridCursor<H2Row> cursor) {
            this.cursor = cursor;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNextX() throws IgniteCheckedException {
            assert cursor != null;

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
        @Override public Row nextX() {
            assert cursor != null;

            if (next == null)
                throw new NoSuchElementException();

            Row res = next;

            next = null;

            return res;
        }

        /** {@inheritDoc} */
        @Override public void removeX() {
            throw new UnsupportedOperationException("Remove is not supported.");
        }
    }
}
