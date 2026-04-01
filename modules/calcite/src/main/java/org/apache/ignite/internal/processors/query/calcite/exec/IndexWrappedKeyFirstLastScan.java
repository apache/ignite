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
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.jetbrains.annotations.Nullable;

/** Like {@link IndexFirstLastScan} but expanding {@link IndexWrappedKeyScan}. */
public class IndexWrappedKeyFirstLastScan<Row> extends IndexWrappedKeyScan<Row> {
    /** First or last field value. */
    private final boolean first;

    /** */
    public IndexWrappedKeyFirstLastScan(
        ExecutionContext<Row> ectx,
        CacheTableDescriptor desc,
        InlineIndex idx,
        ImmutableIntList idxFieldMapping,
        int[] parts,
        @Nullable ImmutableBitSet requiredColumns,
        boolean first
    ) {
        super(ectx, desc, idx, idxFieldMapping, parts, null, requiredColumns);

        this.first = first;
    }

    /** */
    @Override protected TreeIndex<IndexRow> treeIndex() {
        return new IndexFirstLastScan.FirstLastIndexWrapper(idx, indexQueryContext(), first);
    }

    /** */
    @Override protected IndexQueryContext indexQueryContext() {
        IndexQueryContext res = super.indexQueryContext();

        boolean checkExpired = !cctx.config().isEagerTtl();

        return new IndexQueryContext(
            res.cacheFilter(),
            createNotNullRowFilter(idx, checkExpired)
        );
    }
}
