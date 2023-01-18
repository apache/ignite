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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

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
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx.FinalizationStatus.RECOVERY_FINISH;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.MARKED_ROLLBACK;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/**
 * Consistent Cut is a distributed algorithm that defines two set of transactions - BEFORE and AFTER cut - on baseline nodes.
 * It guarantees that every transaction was included into BEFORE on one node also included into the BEFORE on every other node
 * participated in the transaction. It means that Ignite nodes can safely recover themselves to the consistent BEFORE
 * state without any coordination with each other.
 */
class ConsistentCutMarkWalFuture extends GridFutureAdapter<WALPointer> {
    /** Cache context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Consistent Cut ID. */
    private final UUID id;

    /** Logger. */
    private final IgniteLogger log;

    /** Set of checked transactions belong to the BEFORE set. */
    @GridToStringInclude
    private final Set<GridCacheVersion> included = ConcurrentHashMap.newKeySet();

    /** Set of checked transactions belong to the AFTER set. */
    @GridToStringInclude
    private final Set<GridCacheVersion> excluded = ConcurrentHashMap.newKeySet();

    /** Collection of transactions removed from {@link IgniteTxManager#activeTransactions()}. */
    private final Set<IgniteInternalFuture<IgniteInternalTx>> removedFromActive = ConcurrentHashMap.newKeySet();

    /** */
    ConsistentCutMarkWalFuture(GridCacheSharedContext<?, ?> cctx, UUID id) {
        this.cctx = cctx;
        this.id = id;

        log = cctx.logger(ConsistentCutMarkWalFuture.class);

        cctx.tm().onCommitCallback((tx) -> removedFromActive.add(tx.finishFuture()));
    }

    /** Inits the future: it prepares list of active transactions to check which side of Consistent Cut they belong to. */
    void init() {
        try {
            cctx.wal().log(new ConsistentCutStartRecord(id));

            GridCompoundFuture<Boolean, Boolean> checkFut = new GridCompoundFuture<>(CU.boolReducer());

            Iterator<IgniteInternalFuture<IgniteInternalTx>> finFutIt = cctx.tm().activeTransactions().stream()
                .filter(tx -> tx.state() != ACTIVE)
                .map(IgniteInternalTx::finishFuture)
                .iterator();

            // Invoke sequentially over two iterators:
            // 1. Iterators are weakly consistent.
            // 2. We need a guarantee to handle `removedFromActive` after `activeTxs` to avoid missed transactions.
            checkTransactions(finFutIt, checkFut);
            checkTransactions(removedFromActive.iterator(), checkFut);

            checkFut.markInitialized();

            checkFut.listen(finish -> {
                if (isDone())
                    return;

                if (Boolean.FALSE.equals(finish.result())) {
                    onDone(new IgniteCheckedException("Cut is inconsistent."));

                    return;
                }

                WALPointer ptr = null;
                IgniteCheckedException err = null;

                cctx.database().checkpointReadLock();

                try {
                    ptr = cctx.wal().log(new ConsistentCutFinishRecord(id, included, excluded), RolloverType.CURRENT_SEGMENT);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to write ConsistentCutFinishRecord to WAL for [id= " + id + ']', e);

                    err = e;
                }
                finally {
                    cctx.database().checkpointReadUnlock();
                }

                onDone(ptr, err);
            });
        }
        catch (IgniteCheckedException e) {
            onDone(e);

            U.error(log, "Failed to init Consistent Cut: " + id, e);
        }
        finally {
            cctx.tm().onCommitCallback(null);
        }
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

                // ROLLED_BACK transactions don't change data then don't care.
                if (tx.state() == ROLLED_BACK || tx.state() == MARKED_ROLLBACK)
                    return true;

                if (tx.state() != COMMITTED) {
                    U.warn(log, "Cut is inconsistent due to transaction is in unexpected state: " + tx);

                    return false;
                }

                if (tx.finalizationStatus() == RECOVERY_FINISH) {
                    U.warn(log, "Cut is inconsistent due to transaction committed after recovery process: " + tx);

                    return false;
                }

                if (id.equals(tx.cutId()))
                    excluded.add(tx.nearXidVersion());
                else
                    included.add(tx.nearXidVersion());

                return true;
            });

            checkFut.add(txCheckFut);
        }
    }
}
