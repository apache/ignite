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
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * Meta view: local transactions.
 */
public class IgniteSqlMetaViewLocalTransactions extends IgniteSqlMetaView {
    /**
     * @param ctx Grid context.
     */
    public IgniteSqlMetaViewLocalTransactions(GridKernalContext ctx) {
        super("LOCAL_TRANSACTIONS", "Current node active transactions", ctx,
            newColumn("XID"),
            newColumn("START_NODE_ID", Value.UUID),
            newColumn("START_THREAD_ID", Value.LONG),
            newColumn("START_TIME", Value.TIMESTAMP),
            newColumn("TIMEOUT", Value.TIME),
            newColumn("TIMEOUT_MILLIS", Value.LONG),
            newColumn("IS_TIMED_OUT", Value.BOOLEAN),
            newColumn("ISOLATION"),
            newColumn("CONCURRENCY"),
            newColumn("IMPLICIT"),
            newColumn("IS_INVALIDATE"),
            newColumn("STATE"),
            newColumn("SIZE", Value.INT),
            newColumn("STORE_ENABLED", Value.BOOLEAN),
            newColumn("STORE_WRITE_THROUGH", Value.BOOLEAN),
            newColumn("IO_POLICY", Value.BYTE),
            newColumn("IMPLICIT_SINGLE", Value.BOOLEAN),
            newColumn("IS_EMPTY", Value.BOOLEAN),
            newColumn("OTHER_NODE_ID", Value.UUID),
            newColumn("EVENT_NODE_ID", Value.UUID),
            newColumn("ORIGINATING_NODE_ID", Value.UUID),
            newColumn("IS_NEAR", Value.BOOLEAN),
            newColumn("IS_DHT", Value.BOOLEAN),
            newColumn("IS_COLOCATED", Value.BOOLEAN),
            newColumn("IS_LOCAL", Value.BOOLEAN),
            newColumn("IS_SYSTEM", Value.BOOLEAN),
            newColumn("IS_USER", Value.BOOLEAN),
            newColumn("SUBJECT_ID", Value.UUID),
            newColumn("IS_DONE", Value.BOOLEAN),
            newColumn("IS_INTERNAL", Value.BOOLEAN),
            newColumn("IS_ONE_PHASE_COMMIT", Value.BOOLEAN)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        Collection<IgniteInternalTx> txs = ctx.cache().context().tm().activeTransactions();

        if (log.isDebugEnabled())
            log.debug("Get transactions: full scan");

        for (IgniteInternalTx tx : txs) {
            rows.add(
                createRow(ses, rows.size(),
                    tx.xid(),
                    tx.nodeId(),
                    tx.threadId(),
                    valueTimestampFromMillis(tx.startTime()),
                    valueTimeFromMillis(tx.timeout()),
                    tx.timeout(),
                    tx.timedOut(),
                    tx.isolation(),
                    tx.concurrency(),
                    tx.implicit(),
                    tx.isInvalidate(),
                    tx.state(),
                    tx.size(),
                    tx.storeEnabled(),
                    tx.storeWriteThrough(),
                    tx.ioPolicy(),
                    tx.implicitSingle(),
                    tx.empty(),
                    tx.otherNodeId(),
                    tx.eventNodeId(),
                    tx.originatingNodeId(),
                    tx.near(),
                    tx.dht(),
                    tx.colocated(),
                    tx.local(),
                    tx.system(),
                    tx.user(),
                    tx.subjectId(),
                    tx.done(),
                    tx.internal(),
                    tx.onePhaseCommit()
                )
            );
        }

        return rows;
    }
}
