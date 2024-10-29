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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilterImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** */
public class IndexCountScan<Row> extends AbstractCacheScan<Row> {
    /** */
    private final InlineIndex idx;

    /** */
    private final RelCollation collation;

    /** */
    private final boolean notNull;

    /** */
    public IndexCountScan(
        ExecutionContext<Row> ectx,
        GridCacheContext<?, ?> cctx,
        int[] parts,
        InlineIndex idx,
        RelCollation collation,
        boolean notNull
    ) {
        super(ectx, cctx, parts);

        this.idx = idx;
        this.collation = collation;
        this.notNull = notNull;
    }

    /** {@inheritDoc} */
    @Override protected Iterator<Row> createIterator() {
        boolean[] skipCheck = new boolean[] {false};

        BPlusTree.TreeRowClosure<IndexRow, IndexRow> rowFilter = countRowFilter(skipCheck, notNull, idx);

        long cnt = 0;

        if (!F.isEmpty(ectx.getQryTxEntries())) {
            IgniteBiTuple<Set<KeyCacheObject>, List<CacheDataRow>> txChanges = ectx.transactionChanges(
                cctx.cacheId(),
                parts,
                Function.identity()
            );

            if (!txChanges.get1().isEmpty()) {
                // This call will change `txChanges.get1()` content.
                // Removing found key from set more efficient so we break some rules here.
                rowFilter = transactionAwareCountRowFilter(rowFilter, txChanges.get1());

                cnt = countTransactionRows(notNull, idx, txChanges.get2());
            }
        }

        try {
            IndexingQueryFilter filter = new IndexingQueryFilterImpl(cctx.kernalContext(), topVer, parts);

            for (int i = 0; i < idx.segmentsCount(); ++i) {
                cnt += idx.count(i, new IndexQueryContext(filter, rowFilter));

                skipCheck[0] = false;
            }

            return Collections.singletonList(ectx.rowHandler().factory(long.class).create(cnt)).iterator();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Unable to count index records.", e);
        }
    }

    /** */
    private @Nullable BPlusTree.TreeRowClosure<IndexRow, IndexRow> countRowFilter(boolean[] skipCheck, boolean notNull, InlineIndex iidx) {
        boolean checkExpired = !cctx.config().isEagerTtl();

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
        Set<KeyCacheObject> skipKeys
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

                if (skipKeys.isEmpty())
                    return true;

                IndexRow row = rowFilter == null ? null : rowFilter.lastRow();

                if (row == null)
                    row = tree.getRow(io, pageAddr, idx);

                return !skipKeys.remove(row.cacheDataRow().key());
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
}
