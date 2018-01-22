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

package org.apache.ignite.internal.processors.query.h2.views;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;

/**
 * System view: transaction entries.
 */
public class GridH2SysViewImplTransactionEntries extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplTransactionEntries(GridKernalContext ctx) {
        super("TRANSACTION_ENTRIES", "Cache entries used by transaction", ctx, "XID",
            newColumn("XID"),
            newColumn("CACHE_NAME"),
            newColumn("OPERATION"),
            newColumn("IS_LOCKED", Value.BOOLEAN),
            newColumn("KEY_HASH_CODE", Value.INT),
            newColumn("KEY_PARTITION", Value.INT),
            newColumn("KEY_IS_INTERNAL", Value.BOOLEAN),
            newColumn("NODE_ID", Value.UUID)
            );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        // TODO: Check for thread safety

        Collection<IgniteInternalTx> txs = ctx.cache().context().tm().activeTransactions();

        ColumnCondition xidCond = conditionForColumn("XID", first, last);

        if (xidCond.isEquality()) {
            try {
                log.debug("Get transaction entities: filter by xid");

                final String xid = xidCond.getValue().getString();

                txs = F.view(txs, new IgnitePredicate<IgniteInternalTx>() {
                    @Override public boolean apply(IgniteInternalTx tx) {
                        return xid != null && xid.equals(tx.xid().toString());
                    }
                });

            }
            catch (Exception e) {
                log.warning("Failed to get transactions by xid: " + xidCond.getValue().getString(), e);

                txs = Collections.emptySet();
            }
        }
        else
            log.debug("Get transaction entities: transactions full scan");

        return new TxEntitiesIterable(ses, txs);
    }

    /**
     * Transaction entries iterable.
     */
    private class TxEntitiesIterable implements Iterable<Row> {
        /** Session. */
        private final Session ses;

        /** Transactions. */
        private final Iterable<IgniteInternalTx> txs;

        /**
         * @param ses Session.
         * @param txs Transactions.
         */
        public TxEntitiesIterable(Session ses, Iterable<IgniteInternalTx> txs) {
            this.ses = ses;
            this.txs = txs;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Row> iterator() {
            return new TxEntitiesIterator(ses, txs.iterator());
        }
    }

    /**
     * Transaction entries iterator.
     */
    private class TxEntitiesIterator implements Iterator<Row> {
        /** Session. */
        private final Session ses;

        /** Transaction iterator. */
        private final Iterator<IgniteInternalTx> txIter;

        /** Entry iterator. */
        private Iterator<IgniteTxEntry> entryIter;

        /** Counter. */
        private long rowCnt = 0;

        /** Next transaction. */
        private IgniteInternalTx nextTx;

        /** Next entry. */
        private IgniteTxEntry nextEntry;

        /**
         * @param txIter Transaction iterator.
         */
        public TxEntitiesIterator(Session ses, Iterator<IgniteInternalTx> txIter) {
            this.ses = ses;
            this.txIter = txIter;

            moveEntry();
        }

        /**
         * Move to next transaction.
         */
        private void moveTx() {
            nextTx = txIter.next();

            entryIter = nextTx.allEntries().iterator();
        }

        /**
         * Move to next entry.
         */
        private void moveEntry() {
            // First iteration.
            if (nextTx == null && txIter.hasNext())
                moveTx();

            // Empty transactions at first iteration.
            if (entryIter == null)
                return;

            while (entryIter.hasNext() || txIter.hasNext()) {
                if (entryIter.hasNext()) {
                    nextEntry = entryIter.next();
                    rowCnt++;

                    return;
                }
                else
                    moveTx();
            }

            nextEntry = null;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return nextEntry != null;
        }

        /** {@inheritDoc} */
        @Override public Row next() {
            if (nextEntry == null)
                return null;

            Row row = createRow(ses, rowCnt,
                nextTx.xid(),
                nextEntry.context().name(),
                nextEntry.op(),
                nextEntry.locked(),
                nextEntry.key().hashCode(),
                nextEntry.key().partition(),
                nextEntry.key().internal(),
                nextEntry.nodeId()
            );

            moveEntry();

            return row;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
