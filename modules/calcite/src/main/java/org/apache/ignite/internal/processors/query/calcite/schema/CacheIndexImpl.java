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
import java.util.function.Function;
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
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.transactions.TransactionChanges;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexFirstLastScan;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexScan;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilterImpl;
import org.jetbrains.annotations.NotNull;
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
        ColocationGroup grp,
        RangeIterable<Row> ranges,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        UUID locNodeId = execCtx.localNodeId();
        if (grp.nodeIds().contains(locNodeId) && idx != null) {
            return new IndexScan<>(execCtx, tbl.descriptor(), idx.unwrap(InlineIndex.class), collation.getKeys(),
                grp.partitions(locNodeId), ranges, requiredColumns);
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
        UUID locNodeId = ectx.localNodeId();

        if (grp.nodeIds().contains(locNodeId) && idx != null) {
            return new IndexFirstLastScan<>(
                first,
                ectx,
                tbl.descriptor(),
                idx.unwrap(InlineIndexImpl.class),
                collation.getKeys(),
                grp.partitions(locNodeId),
                requiredColumns
            );
        }

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public long count(ExecutionContext<?> ectx, ColocationGroup grp, boolean notNull) {
        if (idx == null || !grp.nodeIds().contains(ectx.localNodeId()))
            return 0;

        int[] locParts = grp.partitions(ectx.localNodeId());

        InlineIndex iidx = idx.unwrap(InlineIndex.class);

        boolean[] skipCheck = new boolean[] {false};

        BPlusTree.TreeRowClosure<IndexRow, IndexRow> rowFilter = countRowFilter(skipCheck, notNull, iidx);

        long cnt = 0;

        if (!F.isEmpty(ectx.getQryTxEntries())) {
            TransactionChanges<CacheDataRow> txChanges = ectx.transactionChanges(
                iidx.indexDefinition().cacheInfo().cacheId(),
                locParts,
                Function.identity(),
                null
            );

            if (!txChanges.changedKeysEmpty()) {
                // This call will change `txChanges` content.
                // Removing found key from set more efficient so we break some rules here.
                rowFilter = transactionAwareCountRowFilter(rowFilter, txChanges);

                cnt = countTransactionRows(notNull, iidx, txChanges.newAndUpdatedEntries());
            }
        }

        try {
            IndexingQueryFilter filter = new IndexingQueryFilterImpl(tbl.descriptor().cacheContext().kernalContext(),
                ectx.topologyVersion(), locParts);

            for (int i = 0; i < iidx.segmentsCount(); ++i) {
                cnt += iidx.count(i, new IndexQueryContext(filter, rowFilter));

                skipCheck[0] = false;
            }

            return cnt;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Unable to count index records.", e);
        }
    }

    /** */
    private @Nullable BPlusTree.TreeRowClosure<IndexRow, IndexRow> countRowFilter(boolean[] skipCheck, boolean notNull, InlineIndex iidx) {
        boolean checkExpired = !tbl.descriptor().cacheContext().config().isEagerTtl();

        if (notNull) {
            boolean nullsFirst = collation.getFieldCollations().get(0).nullDirection == RelFieldCollation.NullDirection.FIRST;

            BPlusTree.TreeRowClosure<IndexRow, IndexRow> notNullRowFilter = IndexScan.createNotNullRowFilter(iidx, checkExpired);

            return new BPlusTree.TreeRowClosure<>() {
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
                    if (skipCheck[0] && !checkExpired)
                        return nullsFirst;

                    boolean res = notNullRowFilter.apply(tree, io, pageAddr, idx);

                    if (res == nullsFirst)
                        skipCheck[0] = true;

                    return res;
                }

                @Override public IndexRow lastRow() {
                    return (skipCheck[0] && !checkExpired)
                        ? null
                        : notNullRowFilter.lastRow();
                }
            };
        }

        return checkExpired ? IndexScan.createNotExpiredRowFilter() : null;
    }

    /** */
    private static @NotNull BPlusTree.TreeRowClosure<IndexRow, IndexRow> transactionAwareCountRowFilter(
        BPlusTree.TreeRowClosure<IndexRow, IndexRow> rowFilter,
        TransactionChanges<CacheDataRow> txChanges
    ) {
        return new BPlusTree.TreeRowClosure<>() {
            @Override public boolean apply(
                BPlusTree<IndexRow, IndexRow> tree,
                BPlusIO<IndexRow> io,
                long pageAddr,
                int idx
            ) throws IgniteCheckedException {
                if (rowFilter != null && !rowFilter.apply(tree, io, pageAddr, idx))
                    return false;

                if (txChanges.changedKeysEmpty())
                    return true;

                IndexRow row = rowFilter == null ? null : rowFilter.lastRow();

                if (row == null)
                    row = tree.getRow(io, pageAddr, idx);

                return !txChanges.remove(row.cacheDataRow().key());
            }
        };
    }

    /** */
    private static long countTransactionRows(boolean notNull, InlineIndex iidx, List<CacheDataRow> changedRows) {
        InlineIndexRowHandler rowHnd = iidx.segment(0).rowHandler();

        long cnt = 0;

        for (CacheDataRow txRow : changedRows) {
            if (rowHnd.indexKey(0, txRow) == NullIndexKey.INSTANCE && notNull)
                continue;

            cnt++;
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

        List<IndexKeyDefinition> keyDefs = new ArrayList<>(idx.indexDefinition().indexKeyDefinitions().values());

        for (InlineIndexKeyType keyType : InlineIndexKeyTypeRegistry.types(keyDefs, new IndexKeyTypeSettings())) {
            // Skip variable length keys and java objects (see comments about these limitations in IndexScan class).
            if (keyType.keySize() < 0 || keyType.type() == IndexKeyType.JAVA_OBJECT)
                return false;
        }

        return true;
    }
}
