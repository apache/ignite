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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexFirstLastScan;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexScan;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilterImpl;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite scannable cache index.
 */
public class CacheIndexImpl implements IgniteIndex {
    /** */
    private final RelCollation collation;

    /** */
    private final String idxName;

    /** */
    private final @Nullable Index idx;

    /** */
    private final IgniteCacheTable tbl;

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

    /** {@inheritDoc} */
    @Override public IgniteLogicalIndexScan toRel(
        RelOptCluster cluster,
        RelOptTable relOptTbl,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        return IgniteLogicalIndexScan.create(cluster, cluster.traitSet(), relOptTbl, idxName, proj, cond, requiredColumns);
    }

    /** */
    @Override public <Row> Iterable<Row> scan(
        ExecutionContext<Row> execCtx,
        ColocationGroup group,
        Predicate<Row> filters,
        RangeIterable<Row> ranges,
        Function<Row, Row> rowTransformer,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        UUID localNodeId = execCtx.localNodeId();
        if (group.nodeIds().contains(localNodeId) && idx != null) {
            return new IndexScan<>(execCtx, tbl.descriptor(), idx.unwrap(InlineIndex.class), collation.getKeys(),
                group.partitions(localNodeId), filters, ranges, rowTransformer, requiredColumns);
        }

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public <Row> Iterable<Row> firstOrLast(
        boolean first,
        ExecutionContext<Row> ectx,
        ColocationGroup grp,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        UUID localNodeId = ectx.localNodeId();

        if (grp.nodeIds().contains(localNodeId) && idx != null) {
            return new IndexFirstLastScan<>(
                first,
                ectx,
                tbl.descriptor(),
                idx.unwrap(InlineIndexImpl.class),
                collation.getKeys(),
                grp.partitions(localNodeId),
                requiredColumns
            );
        }

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public long count(ExecutionContext<?> ectx, ColocationGroup grp, boolean notNull) {
        long cnt = 0;

        if (idx != null && grp.nodeIds().contains(ectx.localNodeId())) {
            IndexingQueryFilter filter = new IndexingQueryFilterImpl(tbl.descriptor().cacheContext().kernalContext(),
                ectx.topologyVersion(), grp.partitions(ectx.localNodeId()));

            InlineIndex iidx = idx.unwrap(InlineIndex.class);

            BPlusTree.TreeRowClosure<IndexRow, IndexRow> rowFilter = null;

            boolean checkExpired = !tbl.descriptor().cacheContext().config().isEagerTtl();

            if (notNull) {
                boolean nullsFirst = collation.getFieldCollations().get(0).nullDirection ==
                    RelFieldCollation.NullDirection.FIRST;

                BPlusTree.TreeRowClosure<IndexRow, IndexRow> notNullRowFilter =
                    IndexScan.createNotNullRowFilter(iidx, checkExpired);

                AtomicBoolean skipCheck = new AtomicBoolean();

                rowFilter = new BPlusTree.TreeRowClosure<IndexRow, IndexRow>() {
                    @Override public boolean apply(
                        BPlusTree<IndexRow, IndexRow> tree,
                        BPlusIO<IndexRow> io,
                        long pageAddr,
                        int idx
                    ) throws IgniteCheckedException {
                        // If we have NULLS-FIRST collation, all values after first not-null value will be not-null,
                        // don't need to check it with notNullRowFilter.
                        // In case of NULL-LAST collation, all values after first null value will be null,
                        // don't need to check it too.
                        if (skipCheck.get() && !checkExpired)
                            return nullsFirst;

                        boolean res = notNullRowFilter.apply(tree, io, pageAddr, idx);

                        if (res == nullsFirst)
                            skipCheck.set(true);

                        return res;
                    }
                };
            }
            else if (checkExpired)
                rowFilter = IndexScan.createNotExpiredRowFilter();

            try {
                for (int i = 0; i < iidx.segmentsCount(); ++i)
                    cnt += iidx.count(i, new IndexQueryContext(filter, rowFilter, ectx.mvccSnapshot()));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Unable to count index records.", e);
            }
        }

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public List<SearchBounds> toSearchBounds(
        RelOptCluster cluster,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        RelCollation collation = this.collation;
        RelDataType rowType = tbl.getRowType(cluster.getTypeFactory());

        if (requiredColumns != null)
            collation = collation.apply(Commons.mapping(requiredColumns, rowType.getFieldCount()));

        if (!collation.getFieldCollations().isEmpty()) {
            return RexUtils.buildSortedSearchBounds(
                cluster,
                collation,
                cond,
                rowType,
                requiredColumns
            );
        }

        // Empty index find predicate.
        return null;
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

        List<IndexKeyDefinition> keyDefs = new ArrayList<>(idx.unwrap(InlineIndex.class).indexDefinition()
            .indexKeyDefinitions().values());

        for (InlineIndexKeyType keyType : InlineIndexKeyTypeRegistry.types(keyDefs, new IndexKeyTypeSettings())) {
            // Skip variable length keys and java objects (see comments about these limitations in IndexScan class).
            if (keyType.keySize() < 0 || keyType.type() == IndexKeyType.JAVA_OBJECT)
                return false;
        }

        return true;
    }
}
