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

import java.util.Comparator;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.GridIndex;

/**
 * Runtime sorted index.
 */
public abstract class AbstractRuntimeSortedIndex<Row> implements GridIndex<Row>, AutoCloseable {
    /** */
    protected final ExecutionContext<Row> ectx;

    /** */
    protected final Comparator<Row> comp;

    /**
     *
     */
    protected AbstractRuntimeSortedIndex(
        ExecutionContext<Row> ectx,
        Comparator<Row> comp
    ) {
        this.ectx = ectx;
        this.comp = comp;
    }

    /** */
    public abstract void push(Row r);

    /** */
    public Iterable<Row> scan(
        ExecutionContext<Row> ectx,
        RelDataType rowType,
        GridIndex<Row> idx,
        Predicate<Row> filters,
        Supplier<Row> lowerBound,
        Supplier<Row> upperBound
    ) {
        return new IndexScan(ectx, rowType, idx, filters, lowerBound, upperBound);
    }

    /**
     *
     */
    class IndexScan extends AbstractIndexScan<Row, Row> {
        /**
         * @param ectx Execution context.
         * @param rowType Row type.
         * @param idx Physical index.
         * @param filters Additional filters.
         * @param lowerBound Lower index scan bound.
         * @param upperBound Upper index scan bound.
         */
        public IndexScan(
            ExecutionContext<Row> ectx,
            RelDataType rowType,
            GridIndex<Row> idx,
            Predicate<Row> filters,
            Supplier<Row> lowerBound,
            Supplier<Row> upperBound) {
            super(ectx, rowType, idx, filters, lowerBound, upperBound, null);
        }

        /** */
        @Override protected Row row2indexRow(Row bound) {
            return bound;
        }

        /** */
        @Override protected Row indexRow2Row(Row row) throws IgniteCheckedException {
            return row;
        }

        /** */
        @Override protected BPlusTree.TreeRowClosure<Row, Row> filterClosure() {
            return null;
        }
    }
}
