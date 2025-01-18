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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.Function;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.transactions.TransactionChanges;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/** */
public class TableScan<Row> extends AbstractCacheColumnsScan<Row> {
    /** */
    public TableScan(
        ExecutionContext<Row> ectx,
        CacheTableDescriptor desc,
        int[] parts,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        super(ectx, desc, parts, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override protected Iterator<Row> createIterator() {
        return new IteratorImpl();
    }

    /**
     * Table scan iterator.
     */
    private class IteratorImpl extends GridIteratorAdapter<Row> {
        /** */
        private final Queue<GridDhtLocalPartition> parts;

        /** */
        private GridCursor<? extends CacheDataRow> cur;

        /** Transaction changes. */
        private final TransactionChanges<CacheDataRow> txChanges;

        /** */
        private Iterator<CacheDataRow> txIter = Collections.emptyIterator();

        /** */
        private Row next;

        /** */
        private IteratorImpl() {
            assert reservedParts != null;

            parts = new ArrayDeque<>(reservedParts);

            txChanges = F.isEmpty(ectx.getQryTxEntries())
                ? TransactionChanges.empty()
                : ectx.transactionChanges(
                    cctx.cacheId(),
                    // All partitions scaned for replication cache.
                    // See TableScan#reserve.
                    cctx.isReplicated() ? null : TableScan.this.parts,
                    Function.identity(),
                    null
                );
        }

        /** {@inheritDoc} */
        @Override public boolean hasNextX() throws IgniteCheckedException {
            advance();

            return next != null;
        }

        /** {@inheritDoc} */
        @Override public Row nextX() throws IgniteCheckedException {
            advance();

            if (next == null)
                throw new NoSuchElementException();

            Row next = this.next;

            this.next = null;

            return next;
        }

        /** {@inheritDoc} */
        @Override public void removeX() {
            throw new UnsupportedOperationException("Remove is not supported.");
        }

        /** */
        private void advance() throws IgniteCheckedException {
            assert parts != null;

            if (next != null)
                return;

            while (true) {
                if (cur == null) {
                    GridDhtLocalPartition part = parts.poll();
                    if (part == null)
                        break;

                    cur = part.dataStore().cursor(cctx.cacheId());

                    if (!txChanges.changedKeysEmpty()) {
                        // This call will change `txChanges` content.
                        // Removing found key from set more efficient so we break some rules here.
                        cur = new KeyFilteringCursor<>(cur, txChanges, CacheSearchRow::key);

                        txIter = F.iterator0(txChanges.newAndUpdatedEntries(), true, e -> e.key().partition() == part.id());
                    }
                }

                CacheDataRow row;

                if (cur.next())
                    row = cur.get();
                else
                    row = txIter.hasNext() ? txIter.next() : null;

                if (row != null) {
                    if (row.expireTime() > 0 && row.expireTime() <= U.currentTimeMillis())
                        continue;

                    if (!desc.match(row))
                        continue;

                    next = desc.toRow(ectx, row, factory, requiredColumns);

                    break;
                }
                else
                    cur = null;
            }
        }
    }
}
