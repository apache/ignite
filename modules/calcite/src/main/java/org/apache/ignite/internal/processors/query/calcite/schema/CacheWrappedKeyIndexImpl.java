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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexScan;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexWrappedKeyScan;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.jetbrains.annotations.Nullable;

/** Extension for column {@value QueryUtils#KEY_FIELD_NAME} in case of composite primary key. */
public class CacheWrappedKeyIndexImpl extends CacheIndexImpl {
    /** */
    private final RelCollation keyFieldCollation;

    /** */
    CacheWrappedKeyIndexImpl(RelCollation collation, String idxName, Index idx, IgniteCacheTable tbl) {
        super(collation, idxName, idx, tbl);

        keyFieldCollation = deriveKeyFieldIndexCollation(tbl);
    }

    /** */
    @Override protected @Nullable List<SearchBounds> buildSearchBounds(
        RelOptCluster cluster,
        @Nullable RexNode cond,
        RelDataType rowType,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        if (cond == null ||
            mapByRequireColumns(keyFieldCollation, rowType, requiredColumns).getFieldCollations().isEmpty())
            return null; // Empty index find predicate.

        return RexUtils.buildHashSearchBounds(cluster, cond, rowType, requiredColumns, true);
    }

    /** */
    @Override protected <Row> IndexScan<Row> createIndexScan(
        ExecutionContext<Row> ectx,
        ColocationGroup grp,
        RangeIterable<Row> ranges,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        return new IndexWrappedKeyScan<>(
            ectx,
            tbl.descriptor(),
            idx.unwrap(InlineIndex.class),
            collation.getKeys(),
            grp.partitions(ectx.localNodeId()),
            ranges,
            requiredColumns
        );
    }

    /** */
    @Override protected <Row> IndexScan<Row> createIndexFirstLastScan(
        boolean first,
        ExecutionContext<Row> ectx,
        ColocationGroup grp,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        throw new IgniteException(String.format("Should not be created for wrappped %s index", QueryUtils.KEY_FIELD_NAME));
    }

    /** */
    @Override protected CacheIndexImpl copyWithNewTable(IgniteCacheTable newTbl) {
        return new CacheWrappedKeyIndexImpl(collation, idxName, idx, newTbl);
    }

    /** */
    @Override protected CacheIndexImpl copyWithNewTableAndCollation(
        IgniteCacheTable newTbl,
        RelCollation newCollation
    ) {
        return new CacheWrappedKeyIndexImpl(collation, idxName, idx, newTbl);
    }

    /** */
    public RelCollation keyFieldCollation() {
        return keyFieldCollation;
    }

    /** */
    private static RelCollation deriveKeyFieldIndexCollation(IgniteCacheTable tbl) {
        ColumnDescriptor desc = tbl.descriptor().columnDescriptor(QueryUtils.KEY_FIELD_NAME);
        assert desc != null : String.format(
            "cacheName=%s, schemaName=%s, tableName=%s",
            tbl.descriptor().typeDescription().cacheName(),
            tbl.descriptor().typeDescription().schemaName(),
            tbl.descriptor().typeDescription().tableName()
        );

        int fieldIdx = desc.fieldIndex();

        return RelCollations.of(List.of(TraitUtils.createFieldCollation(fieldIdx, true)));
    }
}
