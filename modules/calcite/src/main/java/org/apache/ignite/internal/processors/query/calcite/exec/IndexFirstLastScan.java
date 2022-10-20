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

import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.jetbrains.annotations.Nullable;

/**
 * Takes only first or last index value.
 */
public class IndexFirstLastScan<Row> extends IndexScan<Row> {
    /**
     * @param first {@code True} to take first index value. {@code False} to take last value.
     * @param ectx Execution context.
     * @param desc Table descriptor.
     * @param idxFieldMapping Mapping from index keys to row fields.
     * @param idx Phisycal index.
     */
    public IndexFirstLastScan(boolean first,
        ExecutionContext<Row> ectx,
        CacheTableDescriptor desc,
        InlineIndexImpl idx,
        ImmutableIntList idxFieldMapping,
        int[] parts,
        @Nullable ImmutableBitSet requiredColumns) {
        super(ectx, desc, new FirstLastIndexWrapper(idx, first), idxFieldMapping, parts, null, null, null,
            requiredColumns);
    }

    /** {@inheritDoc} */
    @Override protected IndexQueryContext indexQueryContext() {
        IndexQueryContext res = super.indexQueryContext();

        BPlusTree.TreeRowClosure<IndexRow, IndexRow> f = res.rowFilter();

        return new IndexQueryContext(res.cacheFilter(), new BPlusTree.TreeRowClosure<IndexRow, IndexRow>() {
            /**
             * {@inheritDoc}
             */
            @Override public boolean apply(BPlusTree<IndexRow, IndexRow> tree, BPlusIO<IndexRow> io, long pageAddr,
                int idx) throws IgniteCheckedException {
                if (f != null && !f.apply(tree, io, pageAddr, idx))
                    return false;

                return io.getLookupRow(tree, pageAddr, idx).key(0).type() != IndexKeyType.NULL;
            }
        }, res.mvccSnapshot());
    }

    /** */
    private static class FirstLastIndexWrapper extends IndexScan.TreeIndexWrapper {
        /** */
        private final boolean first;

        /**
         * @param idx   Index
         * @param first {@code True} to take first index value. {@code False} to take last value.
         */
        protected FirstLastIndexWrapper(InlineIndexImpl idx, boolean first) {
            super(idx);
            this.first = first;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<IndexRow> find(IndexRow lower, IndexRow upper, boolean lowerInclude,
            boolean upperInclude, IndexQueryContext qctx) {
            assert lower == null && upper == null;
            assert lowerInclude && upperInclude;

            try {
                return ((InlineIndexImpl)idx).takeFirstOrLast(qctx, first);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to take " + (lowerInclude ? "first" : "last") + " index row.", e);
            }
        }
    }
}
