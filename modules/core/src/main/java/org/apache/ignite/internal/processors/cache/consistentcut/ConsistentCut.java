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

import java.util.Iterator;
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
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx.FinalizationStatus.RECOVERY_FINISH;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.MARKED_ROLLBACK;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

/**
 * Describes current Consistent Cut.
 */
public class ConsistentCut extends GridFutureAdapter<Boolean> {
    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    private final IgniteLogger log;

    /**
     * Marker that inits this cut.
     *
     * a) To guarantee happens-before between versions are received and sent after by the same node.
     * b) To guarantee that every transaction committed after the version update isn't cleaned from {@link #committingTxs}.
     */
    private final ConsistentCutMarker marker;

    /** Set of checked transactions belonging to the BEFORE side. */
    @GridToStringInclude
    private Set<GridCacheVersion> beforeCut;

    /** Set of checked transactions belonging to the AFTER side. */
    @GridToStringInclude
    private Set<GridCacheVersion> afterCut;

    /**
     * Collection of committing and committed transactions. Track them additionally to {@link IgniteTxManager#activeTransactions()}
     * due to concurrency between threads that remove transactions from the collection and a thread that parses it in
     * {@link #init()}.
     */
    private final Set<IgniteInternalFuture<IgniteInternalTx>> committingTxs = ConcurrentHashMap.newKeySet();

    /** */
    ConsistentCut(GridCacheSharedContext<?, ?> cctx, ConsistentCutMarker marker) {
        this.cctx = cctx;

        log = cctx.logger(ConsistentCut.class);

        this.marker = marker;
    }

    /** */
    public ConsistentCutMarker marker() {
        return marker;
    }

    /**
     * Inits local Consistent Cut: prepares list of active transactions to check which side of Consistent Cut they belong to.
     */
    protected void init() throws IgniteCheckedException {
        beforeCut = ConcurrentHashMap.newKeySet();
        afterCut = ConcurrentHashMap.newKeySet();

        GridCompoundFuture<Boolean, Boolean> checkFut = new GridCompoundFuture<>(CU.boolReducer());

        Iterator<IgniteInternalFuture<IgniteInternalTx>> finFutIt = cctx.tm().activeTransactions().stream()
            .filter(tx -> tx.state() != ACTIVE)
            .map(IgniteInternalTx::finishFuture)
            .iterator();

        // Invoke sequentially over two iterators:
        // 1. iterators are weakly consistent.
        // 2. we need a guarantee to handle `committingTxs` after `activeTxs` to avoid missed transactions.
        checkTransactions(finFutIt, checkFut);

        walLog(new ConsistentCutStartRecord(marker));

        checkTransactions(committingTxs.iterator(), checkFut);

        checkFut.markInitialized();

        checkFut.listen(finish -> {
            if (Boolean.FALSE.equals(finish.result()) || isDone()) {
                if (log.isDebugEnabled())
                    log.debug("Cut might be inconsistent for marker " + marker + ". Skip writing FinishRecord.");
            }
            else {
                try {
                    walLog(new ConsistentCutFinishRecord(beforeCut, afterCut));
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to write ConsistentCutFinishRecord to WAL for marker " + marker, e);

                    onDone(e);

                    return;
                }
            }

            onDone(finish.result());
        });
    }

    /**
     * Registers transaction before commit it, sets Consistent Cut Version if needed (for non-near nodes).
     * It invokes before committing transactions leave {@link IgniteTxManager#activeTransactions()}.
     *
     * @param txFinFut Transaction finish future.
     */
    public void addCommittingTransaction(IgniteInternalFuture<IgniteInternalTx> txFinFut) {
        if (!isDone())
            committingTxs.add(txFinFut);
    }

    /**
     * Checks active transactions - decides which side of Consistent Cut they belong to after they finished.
     *
     * @param activeTxFinFuts Collection of active transactions to check.
     * @param checkFut Compound future that reduces finishes of checked transactions.
     */
    private void checkTransactions(
        Iterator<IgniteInternalFuture<IgniteInternalTx>> activeTxFinFuts,
        GridCompoundFuture<Boolean, Boolean> checkFut
    ) {
        while (activeTxFinFuts.hasNext()) {
            IgniteInternalFuture<Boolean> txCheckFut = activeTxFinFuts.next().chain(txFut -> {
                // txFut never fails and always returns IgniteInternalTx.
                IgniteInternalTx tx = txFut.result();

                if (!(tx.state() == UNKNOWN
                    || tx.state() == MARKED_ROLLBACK
                    || tx.state() == ROLLED_BACK
                    || tx.state() == COMMITTED)) {
                    U.warn(log, "Transaction is in unexepcted state: " + tx.state() +
                        ". Cut might be inconsistent. Transaction: " + tx);

                    return false;
                }

                if (tx.state() == UNKNOWN) {
                    U.warn(log, "Transaction is in UNKNOWN state. Cut might be inconsistent. Transaction: " + tx);

                    return false;
                }

                // ROLLED_BACK transactions don't change data then don't care.
                if (tx.state() == ROLLED_BACK || tx.state() == MARKED_ROLLBACK)
                    return true;

                if (tx.finalizationStatus() == RECOVERY_FINISH) {
                    if (log.isDebugEnabled()) {
                        log.debug("Transaction committed after recovery process and CutVersion isn't defined. " +
                            "Cut might be inconsistent. Transaction: " + tx);
                    }

                    return false;
                }

                if (tx.marker() == null || tx.marker().compareTo(marker) < 0)
                    beforeCut.add(tx.nearXidVersion());
                else
                    afterCut.add(tx.nearXidVersion());

                return true;
            });

            checkFut.add(txCheckFut);
        }
    }

    /**
     * Logs Consistent Cut Record to WAL.
     */
    private boolean walLog(WALRecord record) throws IgniteCheckedException {
        if (cctx.wal() != null) {
            if (log.isDebugEnabled())
                log.debug("Writing Consistent Cut WAL record: " + record);

            cctx.wal().log(record);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        List<IgniteUuid> before = null;
        List<IgniteUuid> after = null;

        // Write IgniteUuid because it's more convenient for debug purposes than GridCacheVersion.
        if (beforeCut != null) {
            before = beforeCut.stream()
                .map(GridCacheVersion::asIgniteUuid)
                .collect(Collectors.toList());
        }

        if (afterCut != null) {
            after = afterCut.stream()
                .map(GridCacheVersion::asIgniteUuid)
                .collect(Collectors.toList());
        }

        return "ConsistentCut [before=" + before + ", after=" + after + "]";
    }
}
