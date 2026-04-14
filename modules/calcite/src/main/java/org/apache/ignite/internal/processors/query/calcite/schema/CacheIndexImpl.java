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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexCountScan;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexFirstLastScan;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexScan;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite scannable cache index.
 */
public class CacheIndexImpl implements IgniteIndex {
    /** */
    protected final RelCollation collation;

    /** */
    protected final String idxName;

    /** */
    protected final @Nullable Index idx;

    /** */
    protected final IgniteCacheTable tbl;

    /** */
    public CacheIndexImpl(RelCollation collation, String name, @Nullable Index idx, IgniteCacheTable tbl) {
        this.collation = collation;
        idxName = name;
        this.idx = idx;
        this.tbl = tbl;
    }

    /** */
    @Override public RelCollation collation() {
        return collation;
    }

    /** */
    @Override public String name() {
        return idxName;
    }

    /** */
    @Override public IgniteTable table() {
        return tbl;
    }

    /** Underlying query index. */
    public Index queryIndex() {
        return idx;
    }

    /** */
    @Override public <Row> Iterable<Row> scan(
        ExecutionContext<Row> execCtx,
        ColocationGroup grp,
        RangeIterable<Row> ranges,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        UUID locNodeId = execCtx.localNodeId();
        if (grp.nodeIds().contains(locNodeId) && idx != null)
            return createIndexScan(execCtx, grp, ranges, requiredColumns);

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public <Row> Iterable<Row> firstOrLast(
        boolean first,
        ExecutionContext<Row> ectx,
        ColocationGroup grp,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        UUID locNodeId = ectx.localNodeId();
        if (grp.nodeIds().contains(locNodeId) && idx != null)
            return createIndexFirstLastScan(first, ectx, grp, requiredColumns);

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public <Row> Iterable<Row> count(ExecutionContext<Row> ectx, ColocationGroup grp, boolean notNull) {
        if (idx == null || !grp.nodeIds().contains(ectx.localNodeId()))
            return Collections.singletonList(ectx.rowHandler().factory(long.class).create(0L));

        int[] locParts = grp.partitions(ectx.localNodeId());

        InlineIndex iidx = idx.unwrap(InlineIndex.class);

        return new IndexCountScan<>(ectx, tbl.descriptor().cacheContext(), locParts, iidx, collation, notNull);
    }

    /** {@inheritDoc} */
    @Override public List<SearchBounds> toSearchBounds(
        RelOptCluster cluster,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        RelDataType rowType = tbl.getRowType(cluster.getTypeFactory());

        return buildSearchBounds(cluster, cond, rowType, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override public boolean isInlineScanPossible(@Nullable ImmutableBitSet requiredColumns) {
        if (idx == null)
            return false;

        // Since inline scan doesn't check expire time, allow it only if expired entries are eagerly removed.
        if (tbl.descriptor().cacheInfo() != null) {
            if (!tbl.descriptor().cacheInfo().config().isEagerTtl())
                return false;
        }

        if (requiredColumns == null)
            requiredColumns = ImmutableBitSet.range(tbl.descriptor().columnDescriptors().size());

        ImmutableIntList idxKeys = collation.getKeys();

        // All indexed keys should be inlined, all required colummns should be inlined.
        if (idxKeys.size() < requiredColumns.cardinality() || !ImmutableBitSet.of(idxKeys).contains(requiredColumns))
            return false;

        List<IndexKeyDefinition> keyDefs = new ArrayList<>(idx.indexDefinition().indexKeyDefinitions().values());

        for (InlineIndexKeyType keyType : InlineIndexKeyTypeRegistry.types(keyDefs, new IndexKeyTypeSettings())) {
            // Skip variable length keys and java objects (see comments about these limitations in IndexScan class).
            if (keyType.keySize() < 0 || keyType.type() == IndexKeyType.JAVA_OBJECT)
                return false;
        }

        return true;
    }

    /** */
    protected @Nullable List<SearchBounds> buildSearchBounds(
        RelOptCluster cluster,
        @Nullable RexNode cond,
        RelDataType rowType,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        RelCollation collation = mapByRequireColumns(this.collation, rowType, requiredColumns);

        if (collation.getFieldCollations().isEmpty())
            return null; // Empty index find predicate.

        return RexUtils.buildSortedSearchBounds(cluster, collation, cond, rowType, requiredColumns);
    }

    /** */
    protected <Row> IndexScan<Row> createIndexScan(
        ExecutionContext<Row> ectx,
        ColocationGroup grp,
        RangeIterable<Row> ranges,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        return new IndexScan<>(
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
    protected <Row> IndexScan<Row> createIndexFirstLastScan(
        boolean first,
        ExecutionContext<Row> ectx,
        ColocationGroup grp,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        return new IndexFirstLastScan<>(
            first,
            ectx,
            tbl.descriptor(),
            idx.unwrap(InlineIndexImpl.class),
            collation.getKeys(),
            grp.partitions(ectx.localNodeId()),
            requiredColumns
        );
    }

    /** */
    protected CacheIndexImpl copy(IgniteCacheTable newTbl) {
        return new CacheIndexImpl(collation, idxName, idx, newTbl);
    }

    /** */
    protected CacheIndexImpl copy(IgniteCacheTable newTbl, RelCollation newCollation) {
        return new CacheIndexImpl(newCollation, idxName, idx, newTbl);
    }

    /** */
    static RelCollation mapByRequireColumns(
        RelCollation collation,
        RelDataType rowType,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        if (requiredColumns != null)
            collation = collation.apply(Commons.mapping(requiredColumns, rowType.getFieldCount()));

        return collation;
    }
}
