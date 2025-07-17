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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Runtime hash index based on on-heap hash map.
 */
public class RuntimeHashIndex<Row> implements RuntimeIndex<Row> {
    /**
     * Placeholder for keys containing NULL values. Used to skip rows with such keys, since condition NULL=NULL
     * should not satisfy the filter.
     */
    private static final GroupKey NULL_KEY = new GroupKey(X.EMPTY_OBJECT_ARRAY);

    /** */
    protected final ExecutionContext<Row> ectx;

    /** */
    private final ImmutableBitSet keys;

    /** Rows. */
    private final HashMap<GroupKey, ? extends Collection<Row>> rows;

    /** Allow NULL values. */
    private final boolean allowNulls;

    /** */
    private final Supplier<Collection<Row>> groupCollectionFactory;

    /**
     *
     */
    public RuntimeHashIndex(
        ExecutionContext<Row> ectx,
        ImmutableBitSet keys,
        boolean allowNulls
    ) {
        this.ectx = ectx;

        assert !F.isEmpty(keys);

        this.keys = keys;
        this.allowNulls = allowNulls;
        rows = new HashMap<>();
    }

    /** {@inheritDoc} */
    @Override public void push(Row r) {
        GroupKey key = key(r, keys);

        if (key == NULL_KEY)
            return;

        List<Row> eqRows = rows.computeIfAbsent(key, k -> new ArrayList<>());

        eqRows.add(r);
    }

    /** */
    @Override public void close() {
        rows.clear();
    }

    /** */
    public IndexScan scan(Supplier<Row> searchRow, @NotNull int[] keysToUse) {
        return new IndexScan(searchRow, keysToUse);
    }

    /** */
    private GroupKey key(Row r, ImmutableBitSet keys) {
        GroupKey.Builder b = GroupKey.builder(keys.cardinality());

        for (Integer field : keys) {
            Object fieldVal = ectx.rowHandler().get(field, r);

            if (fieldVal == null && !allowNulls)
                return NULL_KEY;

            b.add(fieldVal);
        }

        return b.build();
    }

    /**
     *
     */
    public class IndexScan implements Iterable<Row> {
        /** Search row. */
        private final Supplier<Row> searchRow;

        /** */
        private final ImmutableBitSet keysToUse;

        /**
         * @param searchRow Search row.
         * @param remappedKeys Actual keys to use. If {@code null}, default {@code keys} are used..
         */
        private IndexScan(Supplier<Row> searchRow, @Nullable int[] remappedKeys) {
            this.searchRow = searchRow;
            this.keysToUse = remappedKeys == null ? keys : ImmutableBitSet.of(remappedKeys);
        }

        /**  */
        public @Nullable Collection<Row> get() {
            GroupKey key = key(searchRow.get(), keysToUse);

            if (key == NULL_KEY)
                return null;

            return rows.get(key);
        }

        /** {@inheritDoc} */
        @Override public Iterator<Row> iterator() {
            Collection<Row> res = get();

            return res == null ? Collections.emptyIterator() : res.iterator();
        }
    }
}
