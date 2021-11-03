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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.jetbrains.annotations.NotNull;

/**
 * Runtime hash index based on on-heap hash map.
 */
public class RuntimeHashIndex<RowT> implements RuntimeIndex<RowT> {
    /**
     *
     */
    protected final ExecutionContext<RowT> ectx;

    /**
     *
     */
    private final ImmutableBitSet keys;

    /** Rows. */
    private HashMap<GroupKey, List<RowT>> rows;

    /**
     *
     */
    public RuntimeHashIndex(
            ExecutionContext<RowT> ectx,
            ImmutableBitSet keys
    ) {
        this.ectx = ectx;

        assert !nullOrEmpty(keys);

        this.keys = keys;
        rows = new HashMap<>();
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT r) {
        List<RowT> eqRows = rows.computeIfAbsent(key(r), k -> new ArrayList<>());

        eqRows.add(r);
    }

    /**
     *
     */
    @Override
    public void close() {
        rows.clear();
    }

    /**
     *
     */
    public Iterable<RowT> scan(Supplier<RowT> searchRow) {
        return new IndexScan(searchRow);
    }

    /**
     *
     */
    private GroupKey key(RowT r) {
        GroupKey.Builder b = GroupKey.builder(keys.cardinality());

        for (Integer field : keys) {
            b.add(ectx.rowHandler().get(field, r));
        }

        return b.build();
    }

    /**
     *
     */
    private class IndexScan implements Iterable<RowT>, AutoCloseable {
        /** Search row. */
        private final Supplier<RowT> searchRow;

        /**
         * @param searchRow Search row.
         */
        IndexScan(Supplier<RowT> searchRow) {
            this.searchRow = searchRow;
        }

        /** {@inheritDoc} */
        @Override
        public void close() {
            // No-op.
        }

        /** {@inheritDoc} */
        @NotNull
        @Override
        public Iterator<RowT> iterator() {
            List<RowT> eqRows = rows.get(key(searchRow.get()));

            return eqRows == null ? Collections.emptyIterator() : eqRows.iterator();
        }
    }
}
