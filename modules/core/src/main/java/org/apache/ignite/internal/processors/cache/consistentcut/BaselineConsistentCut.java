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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx.FinalizationStatus.RECOVERY_FINISH;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.MARKED_ROLLBACK;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/** Describes Consistent Cut running on baseline nodes. */
class BaselineConsistentCut extends ConsistentCut {
    /** */
    private final IgniteLogger log;

    /** Set of checked transactions belong to the BEFORE set. */
    @GridToStringInclude
    private Set<GridCacheVersion> before;

    /** Set of checked transactions belong to the AFTER set. */
    @GridToStringInclude
    private Set<GridCacheVersion> after;

    /** Collection of transactions removed from {@link IgniteTxManager#activeTransactions()}. */
    private volatile Set<IgniteInternalFuture<IgniteInternalTx>> removedFromActive = ConcurrentHashMap.newKeySet();

    /** Consistent Cut future, completes with pointer to {@link ConsistentCutFinishRecord}. */
    private final GridFutureAdapter<WALPointer> fut = new GridFutureAdapter<>();

    /** */
    BaselineConsistentCut(GridCacheSharedContext<?, ?> cctx, UUID id) {
        super(cctx, id);

        log = cctx.logger(BaselineConsistentCut.class);

        fut.listen(r -> removedFromActive = null);
    }

    /** Inits local Consistent Cut: prepares list of active transactions to check which side of Consistent Cut they belong to. */
    protected void init() {
        try {
            cctx.wal().log(new ConsistentCutStartRecord(id));

            before = ConcurrentHashMap.newKeySet();
            after = ConcurrentHashMap.newKeySet();

            GridCompoundFuture<Boolean, Boolean> checkFut = new GridCompoundFuture<>(CU.boolReducer());

            Iterator<IgniteInternalFuture<IgniteInternalTx>> finFutIt = cctx.tm().activeTransactions().stream()
                .filter(tx -> tx.state() != ACTIVE)
                .map(IgniteInternalTx::finishFuture)
                .iterator();

            // Invoke sequentially over two iterators:
            // 1. Iterators are weakly consistent.
            // 2. We need a guarantee to handle `removedFromActive` after `activeTxs` to avoid missed transactions.
            checkTransactions(finFutIt, checkFut);

            Iterator<IgniteInternalFuture<IgniteInternalTx>> removedFromActiveIter = removedFromActive.iterator();
            removedFromActive = null;

            checkTransactions(removedFromActiveIter, checkFut);

            checkFut.markInitialized();

            checkFut.listen(finish -> {
                if (Boolean.FALSE.equals(finish.result())) {
                    fut.onDone(new IgniteCheckedException("Cut is inconsistent."));

                    return;
                }

                if (fut.isDone())
                    return;

                cctx.database().checkpointReadLock();

                try {
                    WALPointer ptr = cctx.wal().log(new ConsistentCutFinishRecord(id, before, after), RolloverType.CURRENT_SEGMENT);

                    fut.onDone(ptr);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to write ConsistentCutFinishRecord to WAL for [id= " + id + ']', e);

                    fut.onDone(e);
                }
                finally {
                    cctx.database().checkpointReadUnlock();
                }
            });
        }
        catch (IgniteCheckedException e) {
            fut.onDone(e);

            U.error(log, "Failed to init Consistent Cut: " + id, e);
        }
    }

    /**
     * Collects a transaction before it is removed from {@link IgniteTxManager#activeTransactions()} while committing.
     *
     * @param txFinFut Transaction finish future.
     */
    public void onCommit(IgniteInternalFuture<IgniteInternalTx> txFinFut) {
        Set<IgniteInternalFuture<IgniteInternalTx>> removedFromActive0 = removedFromActive;

        if (removedFromActive0 != null)
            removedFromActive0.add(txFinFut);
    }

    /**
     * Checks active transactions - decides which side of Consistent Cut they belong to after they finished.
     *
     * @param activeTxFinFuts Active transactions to check.
     * @param checkFut Compound future that reduces finishes of the checking transactions.
     */
    private void checkTransactions(
        Iterator<IgniteInternalFuture<IgniteInternalTx>> activeTxFinFuts,
        GridCompoundFuture<Boolean, Boolean> checkFut
    ) {
        while (activeTxFinFuts.hasNext()) {
            IgniteInternalFuture<Boolean> txCheckFut = activeTxFinFuts.next().chain(txFut -> {
                // The `txFut` never fails and always returns IgniteInternalTx.
                IgniteInternalTx tx = txFut.result();

                if (!(tx.state() == MARKED_ROLLBACK
                    || tx.state() == ROLLED_BACK
                    || tx.state() == COMMITTED)) {
                    U.warn(log, "Cut is inconsistent due to transaction is in unexepcted state: " + tx);

                    return false;
                }

                // ROLLED_BACK transactions don't change data then don't care.
                if (tx.state() == ROLLED_BACK || tx.state() == MARKED_ROLLBACK)
                    return true;

                if (tx.finalizationStatus() == RECOVERY_FINISH) {
                    U.warn(log, "Cut is inconsistent due to transaction committed after recovery process: " + tx);

                    return false;
                }

                if (id.equals(tx.cutId()))
                    after.add(tx.nearXidVersion());
                else
                    before.add(tx.nearXidVersion());

                return true;
            });

            checkFut.add(txCheckFut);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel(Throwable err) {
        fut.onDone(err);
    }

    /** {@inheritDoc} */
    @Override public boolean baseline() {
        return true;
    }

    /** Future that completes after {@link ConsistentCutFinishRecord} is written. */
    public IgniteInternalFuture<WALPointer> finishFuture() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BaselineConsistentCut.class, this);
    }
}
