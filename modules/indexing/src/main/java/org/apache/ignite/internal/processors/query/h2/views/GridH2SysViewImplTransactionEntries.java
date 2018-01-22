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
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

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

        return new ParentChildRowIterable<IgniteInternalTx, IgniteTxEntry>(ses, txs,
            new IgniteClosure<IgniteInternalTx, Iterator<IgniteTxEntry>>() {
                @Override public Iterator<IgniteTxEntry> apply(IgniteInternalTx tx) {
                    return tx.allEntries().iterator();
                }
            },
            new IgniteBiClosure<IgniteInternalTx, IgniteTxEntry, Object[]>() {
                @Override public Object[] apply(IgniteInternalTx tx, IgniteTxEntry entry) {
                    return new Object[] {
                        tx.xid(),
                        entry.context().name(),
                        entry.op(),
                        entry.locked(),
                        entry.key().hashCode(),
                        entry.key().partition(),
                        entry.key().internal(),
                        entry.nodeId()
                    };
                }
            }
        );
    }
}
