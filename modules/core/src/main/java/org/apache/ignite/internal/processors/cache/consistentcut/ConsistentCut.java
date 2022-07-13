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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;

/**
 * Describes current Consistent Cut.
 */
public class ConsistentCut {
    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    private final IgniteLogger log;

    /**
     * Sets to {@code true} after {@link ConsistentCutStartRecord} was written. It's volatile to provide happens-before
     * between this record and collecting of active transactions. It must write {@link ConsistentCutStartRecord} before
     * it collected transactions. There are two types of transactions to check:
     * 1. Transactions belong to the BEFORE side but committed after this record.
     * 2. Transactions belong to the AFTER side but committed before this record.
     *
     * Collecting all transactions after writing this record guarantees ({@link ConsistentCutManager}) that final collection
     * includes all such transactions to check. Also, there is no need to track all transactions belong to the AFTER side and
     * committed after this record.
     */
    @GridToStringInclude
    private volatile boolean started;

    /**
     * Collection of transactions belong to the BEFORE side, but optionally committed after {@link ConsistentCutStartRecord}.
     */
    @GridToStringInclude
    private Set<GridCacheVersion> beforeCut;

    /**
     * Collection of transactions belong to the AFTER side, but optionally committed before {@link ConsistentCutStartRecord}.
     */
    @GridToStringInclude
    private Set<GridCacheVersion> afterCut;

    /** */
    ConsistentCut(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;

        log = cctx.logger(ConsistentCut.class);
    }

    /**
     * Runs local Consistent Cut: prepares list of active transactions to check which side of Consistent Cut they belong to.
     *
     * @param ver Consistent Cut version.
     */
    protected void init(ConsistentCutVersion ver) throws IgniteCheckedException {
        beforeCut = ConcurrentHashMap.newKeySet();
        afterCut = ConcurrentHashMap.newKeySet();

        started = walLog(ver, new ConsistentCutStartRecord(ver));

        GridCompoundFutureSet<Void, Void> activeTxsFinishFut = new GridCompoundFutureSet<>();

        // `committingTxs` and `activeTxs` may have some duplicated txs - tx is firstly added to `committingTxs` and
        // only after that it is removed from `activeTxs`.
        listenTransactions(ver, cctx.tm().activeTransactions(), activeTxsFinishFut);
        listenTransactions(ver, cctx.consistentCutMgr().committingTxs(), activeTxsFinishFut);

        activeTxsFinishFut.markInitialized();

        activeTxsFinishFut.listen(activeFinished -> {
            try {
                walLog(ver, new ConsistentCutFinishRecord(beforeCut, afterCut));

                cctx.consistentCutMgr().onFinish(null);
            }
            catch (IgniteCheckedException e) {
                cctx.consistentCutMgr().onFinish(e);
            }
        });
    }

    /** */
    private void listenTransactions(
        ConsistentCutVersion ver, Collection
        <IgniteInternalTx> activeTxs,
        GridCompoundFutureSet<Void, Void> activeTxsFinishFut
    ) {
        for (IgniteInternalTx activeTx : activeTxs) {
            TransactionState txState = activeTx.state();

            // Avoid duplication of futures from different collections.
            if (activeTxsFinishFut.containsFuture(activeTx.finishFuture()))
                continue;

            // Checks COMMITTING / COMMITTED transactions due to concurrency with transactions: some active transactions
            // start committing after being grabbed.
            if (txState == PREPARING || txState == PREPARED || txState == COMMITTING || txState == COMMITTED) {
                long txCutVer;

                // Do not await transactions from the AFTER side.
                if ((txCutVer = ((ConsistentCutVersionAware)activeTx).txCutVersion()) != -1 && txCutVer == ver.version())
                    afterCut.add(activeTx.nearXidVersion());
                else {
                    IgniteInternalFuture<Void> txCheckFut = activeTx.finishFuture().chain(txFut -> {
                        IgniteInternalTx tx = txFut.result();

                        ConsistentCutVersionAware txCutVerAware = (ConsistentCutVersionAware)tx;

                        if (ver.version() > txCutVerAware.txCutVersion())
                            beforeCut.add(tx.nearXidVersion());
                        else
                            afterCut.add(tx.nearXidVersion());

                        return null;
                    });

                    activeTxsFinishFut.add(txCheckFut);
                }
            }
        }
    }

    /**
     * Logs Consistent Cut Record to WAL.
     */
    protected boolean walLog(ConsistentCutVersion cutVer, WALRecord record) throws IgniteCheckedException {
        if (cctx.wal() != null) {
            if (log.isDebugEnabled())
                log.debug("Writing Consistent Cut WAL record: " + record);

            cctx.wal().log(record);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        List<IgniteUuid> incl = null;
        List<IgniteUuid> excl = null;

        if (beforeCut != null)
            incl = beforeCut.stream()
                .map(GridCacheVersion::asIgniteUuid)
                .collect(Collectors.toList());

        if (afterCut != null)
            excl = afterCut.stream()
                .map(GridCacheVersion::asIgniteUuid)
                .collect(Collectors.toList());

        return "ConsistentCut [started=" + started + ", before=" + incl + ", after=" + excl + "]";
    }
}
