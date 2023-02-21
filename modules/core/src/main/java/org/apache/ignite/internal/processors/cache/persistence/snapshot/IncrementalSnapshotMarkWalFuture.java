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
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
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
 * Incremental snapshot based on the Consistent Cut distributed algorithm that goal is to define a specific set of transactions.
 * It guarantees that every transaction was included into the set on one node also included into the same set on every other
 * node participated in the transaction. It means that Ignite nodes can safely recover themselves to the consistent state by applying
 * transactions from this set.
 */
class IncrementalSnapshotMarkWalFuture extends GridFutureAdapter<WALPointer> {
    /** Cache context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Incremental snapshot ID. */
    private final UUID id;

    /** Incremental snapshot topology version. */
    private final long topVer;

    /** Logger. */
    private final IgniteLogger log;

    /** Set of checked transactions included into the incremental snapshot. */
    @GridToStringInclude
    private final Set<GridCacheVersion> included = ConcurrentHashMap.newKeySet();

    /** Set of checked transactions excluded from the incremental snapshot. */
    @GridToStringInclude
    private final Set<GridCacheVersion> excluded = ConcurrentHashMap.newKeySet();

    /** Collection of transactions removed from {@link IgniteTxManager#activeTransactions()}. */
    private final Set<IgniteInternalFuture<IgniteInternalTx>> removedFromActive = ConcurrentHashMap.newKeySet();

    /** */
    IncrementalSnapshotMarkWalFuture(GridCacheSharedContext<?, ?> cctx, UUID id, long topVer) {
        this.cctx = cctx;
        this.id = id;
        this.topVer = topVer;

        log = cctx.logger(IncrementalSnapshotMarkWalFuture.class);

        cctx.tm().onCommitCallback((tx) -> removedFromActive.add(tx.finishFuture()));
    }

    /** Inits the future: it prepares list of active transactions to check whether they belong the incremental snapshot. */
    void init() {
        try {
            cctx.wal().log(new IncrementalSnapshotStartRecord(id));

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
                    onDone(new IgniteCheckedException("Incremental snapshot is inconsistent."));

                    return;
                }

                WALPointer ptr = null;
                IgniteCheckedException err = null;

                cctx.database().checkpointReadLock();

                try {
                    ptr = cctx.wal().log(new IncrementalSnapshotFinishRecord(id, included, excluded), RolloverType.CURRENT_SEGMENT);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to write IncrementalSnapshotFinishRecord to WAL for [id= " + id + ']', e);

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

            U.error(log, "Failed to init incremental snapshot: " + id, e);
        }
        finally {
            cctx.tm().onCommitCallback(null);
        }
    }

    /**
     * Checks active transactions - decides whether they belong to the incremental snapshot after they finished.
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
                    U.warn(log, "Incremental snapshot is inconsistent due to transaction is in unexpected state: " + tx);

                    return false;
                }

                if (tx.finalizationStatus() == RECOVERY_FINISH) {
                    U.warn(log, "Incremental snapshot is inconsistent due to transaction committed after recovery process: " + tx);

                    return false;
                }

                AffinityTopologyVersion txTopVer = tx.topologyVersionSnapshot();

                if (txTopVer == null) {
                    U.warn(log, "Incremental snapshot is inconsistent due to transaction doesn't map to topology: " + tx);

                    return false;
                }

                if (id.equals(tx.incSnpId()) || txTopVer.topologyVersion() > topVer)
                    excluded.add(tx.nearXidVersion());
                else
                    included.add(tx.nearXidVersion());

                return true;
            });

            checkFut.add(txCheckFut);
        }
    }
}
