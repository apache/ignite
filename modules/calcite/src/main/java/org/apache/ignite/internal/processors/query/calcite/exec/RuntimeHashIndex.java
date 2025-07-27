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
import java.util.function.Supplier;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
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
    private final HashMap<GroupKey<Row>, Collection<Row>> rows;

    /** */
    private final Supplier<Collection<Row>> collectionFactory;

    /** Allow NULL values. */
    private final boolean allowNulls;

    /** Creates hash index with the default collection supplier. */
    public RuntimeHashIndex(ExecutionContext<Row> ectx, ImmutableBitSet keys, boolean allowNulls) {
        this(ectx, keys, allowNulls, -1, null);
    }

    /** */
    public RuntimeHashIndex(
        ExecutionContext<Row> ectx,
        ImmutableBitSet keys,
        boolean allowNulls,
        int initCapacity,
        @Nullable Supplier<Collection<Row>> collectionFactory
    ) {
        this(
            ectx,
            allowNulls,
            new MappingRowHandler<>(ectx.rowHandler(), keys),
            initCapacity >= 0 ? new HashMap<>(initCapacity) : new HashMap<>(),
            collectionFactory
        );
    }

    /** Fields setting constructor. */
    private RuntimeHashIndex(
        ExecutionContext<Row> ectx,
        boolean allowNulls,
        RowHandler<Row> keysRowHnd,
        HashMap<GroupKey<Row>, Collection<Row>> rows,
        @Nullable Supplier<Collection<Row>> collectionFactory
    ) {
        this.ectx = ectx;
        this.allowNulls = allowNulls;

        this.keysRowHnd = keysRowHnd;
        this.rows = rows;

        this.collectionFactory = collectionFactory == null ? ArrayList::new : collectionFactory;
    }

    /** {@inheritDoc} */
    @Override public void push(Row r) {
        GroupKey<Row> key = key(r);

        if (key == null)
            return;

        Collection<Row> eqRows = rows.computeIfAbsent(key, k -> collectionFactory.get());

        eqRows.add(r);
    }

    /** */
    @Override public void close() {
        rows.clear();
    }

    /** */
    public Collection<Collection<Row>> rowSets() {
        return Collections.unmodifiableCollection(rows.values());
    }

    /** */
    public IndexScan scan(Supplier<Row> searchRow) {
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

    /** */
    public RuntimeHashIndex<Row> remappedSearcher(ImmutableBitSet remappedKeys) {
        return new RemappedSearcher<>(this, remappedKeys);
    }

    /** */
    private static class RemappedSearcher<Row> extends RuntimeHashIndex<Row> {
        /** */
        private final RuntimeHashIndex<Row> origin;

        /** */
        private RemappedSearcher(RuntimeHashIndex<Row> origin, ImmutableBitSet remappedKeys){
            super(origin.ectx, origin.allowNulls, new MappingRowHandler<>(origin.ectx.rowHandler(), remappedKeys),
                origin.rows, origin.collectionFactory);

            this.origin = origin;
        }

        /** {@inheritDoc} */
        @Override public void push(Row r) {
            origin.push(r);
        }
    }

    /**
     *
     */
    public class IndexScan implements Iterable<Row> {
        /** Search row. */
        private final Supplier<Row> searchRow;

        /**
         * @param searchRow Search row.
         */
        private IndexScan(Supplier<Row> searchRow) {
            this.searchRow = searchRow;
        }

        /**  */
        public @Nullable Collection<Row> get() {
            GroupKey<Row> key = key(searchRow.get());

            if (key == null)
                return Collections.emptyList();

            return rows.get(key);
        }

        /** {@inheritDoc} */
        @Override public Iterator<Row> iterator() {
            Collection<Row> collection = get();

            return collection == null ? Collections.emptyIterator() : collection.iterator();
        }
    }
}
