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
import org.apache.ignite.internal.processors.query.GridIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.lang.GridCursor;
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
    private final int[] proj;

    /** */
    private final Object[] lowerBound;

    /** */
    private final Object[] upperBound;

    /** */
    private final GridIndex<H2Row> idx;

    public IndexScan(
        ExecutionContext ctx,
        IgniteTable tbl,
        Predicate<Object[]> filters,
        int[] projects,
        Object[] lowerBound,
        Object[] upperBound
    ) {
        this.ectx = ctx;
        this.desc = tbl.descriptor();
        this.idx = null;// tbl.index();
        this.filters = filters;
        this.proj = projects;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @NotNull @Override public Iterator<Object[]> iterator() {
        GridCursor<H2Row> cur =  idx.find(null, null, null); // TODO: CODE: implement.

        System.out.println(cur);

        return null;
    }

    private static class CalciteH2Row extends H2Row {

        public CalciteH2Row(ExecutionContext ectx, Object[] row) {
           //H2Utils.wrap(coCtx, o, DataType.getTypeFromClass(o.getClass());
        }

        @Override public boolean indexSearchRow() {
            return true;
        }

        @Override public int getColumnCount() {
            return 0; // TODO: CODE: implement.
        }

        @Override public Value getValue(int index) {
            return null; // TODO: CODE: implement.
        }

        @Override public void setValue(int index, Value v) {
            // TODO: CODE: implement.
        }
    }
}
