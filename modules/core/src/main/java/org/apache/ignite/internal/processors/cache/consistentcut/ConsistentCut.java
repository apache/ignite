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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx.FinalizationStatus.RECOVERY_FINISH;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.MARKED_ROLLBACK;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

/**
 * Describes current Consistent Cut.
 */
public class ConsistentCut extends GridFutureAdapter<WALPointer> {
    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    private final IgniteLogger log;

    /** ID of Consistent Cut. */
    private final UUID id;

    /** Set of checked transactions belonging to the BEFORE side. */
    @GridToStringInclude
    private Set<GridCacheVersion> beforeCut;

    /** Set of checked transactions belonging to the AFTER side. */
    @GridToStringInclude
    private Set<GridCacheVersion> afterCut;

    /** Collection of transactions removed from {@link IgniteTxManager#activeTransactions()}. */
    private volatile Set<IgniteInternalFuture<IgniteInternalTx>> removedActive = ConcurrentHashMap.newKeySet();

    /** */
    ConsistentCut(GridCacheSharedContext<?, ?> cctx, UUID id) {
        this.cctx = cctx;
        this.id = id;

        log = cctx.logger(ConsistentCut.class);
    }

    /** */
    public UUID id() {
        return id;
    }

    /**
     * Inits local Consistent Cut: prepares list of active transactions to check which side of Consistent Cut they belong to.
     */
    protected void init() throws IgniteCheckedException {
        walLog(new ConsistentCutStartRecord(id), false);

        beforeCut = ConcurrentHashMap.newKeySet();
        afterCut = ConcurrentHashMap.newKeySet();

        GridCompoundFuture<Boolean, Boolean> checkFut = new GridCompoundFuture<>(CU.boolReducer());

        Iterator<IgniteInternalFuture<IgniteInternalTx>> finFutIt = cctx.tm().activeTransactions().stream()
            .filter(tx -> tx.state() != ACTIVE)
            .map(IgniteInternalTx::finishFuture)
            .iterator();

        // Invoke sequentially over two iterators:
        // 1. iterators are weakly consistent.
        // 2. we need a guarantee to handle `removedActive` after `activeTxs` to avoid missed transactions.
        checkTransactions(finFutIt, checkFut);
        checkTransactions(removedActive.iterator(), checkFut);

        removedActive = null;

        checkFut.markInitialized();

        checkFut.listen(finish -> {
            if (Boolean.FALSE.equals(finish.result()) || isDone()) {
                if (log.isDebugEnabled())
                    log.debug("Cut might be inconsistent for id " + id + ". Skip writing FinishRecord.");

                onDone(new IgniteCheckedException("Cut is inconsistent."));

                return;
            }

            try {
                WALPointer ptr = walLog(new ConsistentCutFinishRecord(id, beforeCut, afterCut), true);

                onDone(ptr);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to write ConsistentCutFinishRecord to WAL for id " + id, e);

                onDone(e);
            }
        });
    }

    /**
     * Collects a transaction before it is removed from {@link IgniteTxManager#activeTransactions()}.
     *
     * @param txFinFut Transaction finish future.
     */
    public void onRemoveActiveTransaction(IgniteInternalFuture<IgniteInternalTx> txFinFut) {
        Set<IgniteInternalFuture<IgniteInternalTx>> txs = removedActive;

        if (txs != null)
            txs.add(txFinFut);
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
                    U.warn(log, "Transaction committed after recovery process. Cut might be inconsistent. Transaction: " + tx);

                    return false;
                }

                if (id.equals(tx.cutId()))
                    afterCut.add(tx.nearXidVersion());
                else
                    beforeCut.add(tx.nearXidVersion());

                return true;
            });

            checkFut.add(txCheckFut);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable WALPointer res, @Nullable Throwable err, boolean cancel) {
        removedActive = null;

        return super.onDone(res, err, cancel);
    }

    /**
     * Logs Consistent Cut Record to WAL.
     *
     * @param record Record to write to WAL.
     * @param finalRec {@code false} for {@link ConsistentCutStartRecord}, {@code true} for {@link ConsistentCutFinishRecord}.
     * @return Pointer to the record in WAL, or {@code null} if WAL is disabled.
     */
    private @Nullable WALPointer walLog(WALRecord record, boolean finalRec) throws IgniteCheckedException {
        if (cctx.wal() != null) {
            if (log.isDebugEnabled())
                log.debug("Writing Consistent Cut WAL record: " + record);

            if (finalRec) {
                cctx.database().checkpointReadLock();

                try {
                    return cctx.wal().log(record, RolloverType.CURRENT_SEGMENT);
                }
                finally {
                    cctx.database().checkpointReadUnlock();
                }
            }
            else
                return cctx.wal().log(record);
        }

        return null;
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
