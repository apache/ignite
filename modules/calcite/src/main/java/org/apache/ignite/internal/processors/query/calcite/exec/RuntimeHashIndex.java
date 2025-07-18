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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Runtime hash index based on on-heap hash map.
 */
public class RuntimeHashIndex<Row> implements RuntimeIndex<Row> {
    /** */
    protected final ExecutionContext<Row> ectx;

    /** */
    private final RowHandler<Row> keysRowHnd;

    /** Rows. */
    private final HashMap<GroupKey<Row>, List<Row>> rows;

    /** Allow NULL values. */
    private final boolean allowNulls;

    /**
     *
     */
    public RuntimeHashIndex(
        ExecutionContext<Row> ectx,
        ImmutableBitSet keys,
        boolean allowNulls
    ) {
        this.ectx = ectx;
        this.allowNulls = allowNulls;

        assert !F.isEmpty(keys);

        keysRowHnd = new MappingRowHandler<>(ectx.rowHandler(), keys);
        rows = new HashMap<>();
    }

    /** {@inheritDoc} */
    @Override public void push(Row r) {
        GroupKey<Row> key = key(r);

        if (key == null)
            return;

        List<Row> eqRows = rows.computeIfAbsent(key, k -> new ArrayList<>());

        eqRows.add(r);
    }

    /** */
    @Override public void close() {
        rows.clear();
    }

    /** */
    public Iterable<Row> scan(Supplier<Row> searchRow) {
        return new IndexScan(searchRow);
    }

    /**
     * @return Group key for provided row. Can be {@code null} if key fields of row contain NULL values.
     * Since condition NULL=NULL in SQL should not satisfy the filter (but nulls are allowed for
     * IS NOT DISTINCT FROM condition).
     */
    private @Nullable GroupKey<Row> key(Row r) {
        if (!allowNulls) {
            for (int i = 0; i < keysRowHnd.columnCount(r); i++) {
                if (keysRowHnd.get(i, r) == null)
                    return null;
            }
        }

        return new GroupKey<>(r, keysRowHnd);
    }

    /**
     *
     */
    private class IndexScan implements Iterable<Row> {
        /** Search row. */
        private final Supplier<Row> searchRow;

        /**
         * @param searchRow Search row.
         */
        IndexScan(Supplier<Row> searchRow) {
            this.searchRow = searchRow;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Row> iterator() {
            GroupKey<Row> key = key(searchRow.get());

            if (key == null)
                return Collections.emptyIterator();

            List<Row> eqRows = rows.get(key);

            return eqRows == null ? Collections.emptyIterator() : eqRows.iterator();
        }
    }
}
