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
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

/**
 * Runtime hash index based on on-heap hash map.
 */
public class RuntimeHashIndex<Row> implements RuntimeIndex<Row> {
    /** */
    protected final ExecutionContext<Row> ectx;

    /** */
    private final ImmutableBitSet keys;

    /** Rows. */
    private HashMap<GroupKey, List<Row>> rows;

    /**
     *
     */
    public RuntimeHashIndex(
        ExecutionContext<Row> ectx,
        ImmutableBitSet keys
    ) {
        this.ectx = ectx;

        assert !nullOrEmpty(keys);

        this.keys = keys;
        rows = new HashMap<>();
    }

    /** {@inheritDoc} */
    @Override public void push(Row r) {
        List<Row> eqRows = rows.computeIfAbsent(key(r), k -> new ArrayList<>());

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

    /** */
    private GroupKey key(Row r) {
        GroupKey.Builder b = GroupKey.builder(keys.cardinality());

        for (Integer field : keys)
            b.add(ectx.rowHandler().get(field, r));

        return b.build();
    }

    /**
     *
     */
    private class IndexScan implements Iterable<Row>, AutoCloseable {
        /** Search row. */
        private final Supplier<Row> searchRow;

        /**
         * @param searchRow Search row.
         */
        IndexScan(Supplier<Row> searchRow) {
            this.searchRow = searchRow;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            // No-op.
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Row> iterator() {
            List<Row> eqRows = rows.get(key(searchRow.get()));

            return eqRows == null ? Collections.emptyIterator() : eqRows.iterator();
        }
    }
}
