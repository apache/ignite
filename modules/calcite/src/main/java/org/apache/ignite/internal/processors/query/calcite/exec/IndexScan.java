/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Iterator;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeFilterClosure;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilterImpl;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;

/**
 * TODO: Add class description.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class IndexScan implements Iterable<Object[]> {
    /** */
    private final ExecutionContext ectx;

    /** */
    private final TableDescriptor desc;

    /** */
    private final Predicate<Object[]> filters;

    /** */
    private final Object[] lowerBound;

    /** */
    private final Object[] upperBound;

    /** */
    private final GridIndex<H2Row> idx;

    /** */
    private final CacheObjectContext coCtx;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final GridCacheContext cacheCtx;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final int[] partsArr;

    /** */
    private final MvccSnapshot mvccSnapshot;

    public IndexScan(
        ExecutionContext ctx,
        IgniteIndex igniteIdx,
        Predicate<Object[]> filters,
        Object[] lowerBound,
        Object[] upperBound
    ) {
        this.ectx = ctx;
        this.desc = igniteIdx.table().descriptor();
        this.idx = igniteIdx.index();
        this.filters = filters;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.cacheCtx = igniteIdx.table().descriptor().cacheContext();
        this.coCtx = cacheCtx.cacheObjectContext();
        this.ctx = coCtx.kernalContext();
        this.topVer = ctx.planningContext().topologyVersion();
        this.partsArr = ctx.partitions();
        this.mvccSnapshot = ctx.mvccSnapshot();

    }

    @NotNull @Override public Iterator<Object[]> iterator() {
        IndexingQueryFilter filter = new IndexingQueryFilterImpl(ctx, topVer, partsArr);
        IndexingQueryCacheFilter filter1 = filter.forCache(cacheCtx.name());
        H2TreeFilterClosure filterClosure =new H2TreeFilterClosure(filter1, mvccSnapshot, cacheCtx, ectx.planningContext().logger());

        H2Row lower = lowerBound == null ? null : new CalciteH2Row(coCtx, lowerBound);
        H2Row upper = upperBound == null ? null : new CalciteH2Row(coCtx, upperBound);

        GridCursor<H2Row> cur =  idx.find(lower, upper, filterClosure);
//
//        System.out.println("upperBound=" + Arrays.toString(upperBound));
//        System.out.println("lowerBound=" + Arrays.toString(lowerBound));
//        try {
//            //desc.toRow(ectx, (H2CacheRow)cur.get())
//            while (cur.next())
//                System.out.println(this + "next=" + cur.get());
//        }
//        catch (Exception e) {
//            System.out.println("Exc===" + e);
//            throw new RuntimeException(e);
//        }


        return new CursorIteratorWrapper(cur);
    }

    /** */
    private static class CalciteH2Row extends H2Row {
        /** */
        private final Value[] values;

        /** */
        CalciteH2Row(CacheObjectContext coCtx, Object[] row) {
            try {
                values = new Value[row.length];
                for (int i = 0; i < row.length; i++) {
                    Object o = row[i];
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

    /** */
    private class CursorIteratorWrapper implements Iterator<Object[]> {
        /** */
        private final GridCursor<H2Row> cursor;

        /** Next element. */
        private Object[] next;

        /**
         * @param cursor Cursor.
         */
        CursorIteratorWrapper(GridCursor<H2Row> cursor) {
            this.cursor = cursor;
            try {
                assert cursor != null;

                if (cursor.next()) {
                    H2Row h2Row = cursor.get();
                    next = desc.toRow(ectx, (CacheDataRow)h2Row);
                }
            }
            catch (IgniteCheckedException ex) {
                throw U.convertException(ex);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return next != null;
        }

        /** {@inheritDoc} */
        @Override public Object[] next() {
            try {
                Object[] res = next;

                if (cursor.next()) {
                    H2Row h2Row = cursor.get();
                    next = desc.toRow(ectx, (CacheDataRow)h2Row);
                }
                else
                    next = null;

                return res;
            }
            catch (IgniteCheckedException ex) {
                throw U.convertException(ex);
            }
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException("operation is not supported");
        }
    }
}
