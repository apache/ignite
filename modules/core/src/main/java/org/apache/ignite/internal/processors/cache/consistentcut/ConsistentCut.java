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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;

/**
 * Describes current Consistent Cut.
 *
 * @see ConsistentCutFinishRecord
 */
public class ConsistentCut {
    /**
     * How much time Consistent Cut awaits to be completed with error.
     */
    private static final int TIMEOUT = 5_000;

    /**
     * Consistent Cut Version.
     */
    private final ConsistentCutVersion ver;

    /**
     * Collection of active transactions to check.
     */
    private volatile Collection<IgniteInternalTx> activeTxs;

    /**
     * Future completes after writing {@link ConsistentCutStartRecord}.
     *
     * Every transaction committed before completion of this future belongs to the BEFORE side of this Consistent Cut.
     */
    private final GridFutureAdapter<Void> walWritten = new GridFutureAdapter<>();

    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    ConsistentCut(GridCacheSharedContext<?, ?> cctx, IgniteLogger log, ConsistentCutVersion ver) {
        this.ver = ver;

        this.cctx = cctx;
        this.log = log;
    }

    /**
     * Runs local Consistent Cut: writes {@link ConsistentCutStartRecord} and prepares the check-list of transactions.
     *
     * @param beforeCutRef Reference to mutable collection of transactions. All transactions from this collection belong
     *                     to the BEFORE side.
     * @return Future that completes after local Consistent Cut finished.
     */
    protected IgniteInternalFuture<?> run(Collection<IgniteInternalTx> beforeCutRef) throws IgniteCheckedException {
        activeTxs = new ArrayList<>(cctx.tm().activeTransactions());

        Set<GridCacheVersion> beforeCutTxs = new HashSet<>();

        GridCompoundFuture<IgniteInternalTx, Void> activeTxsFinishFut = new GridCompoundFuture<>();

        // It guarantees that there is no missed transactions for checking:
        // 1. `beforeCutRef` and `activeTxs` may have some duplicated txs - tx is firstly added to `beforeCutRef` and
        //    only after that it is removed from `activeTxs`.
        // 2. `beforeCutRef` collection stops filling after grabbing `activeTxs`. It still may receive some updates,
        //    but they duplicated with the `activeTxs` and will be cleaned before adding to the check-list.
        // 3. It prepares before writing StartRecord to WAL. Then it's safe to remove concurrently some txs after
        //    they were committed at `ConsistentCutManager#unregisterAfterCommit`.
        for (IgniteInternalTx tx: beforeCutRef) {
            beforeCutTxs.add(tx.nearXidVersion());

            activeTxsFinishFut.add(tx.finishFuture());
        }

        walLog(ver, new ConsistentCutStartRecord(ver));

        walWritten.onDone();

        for (IgniteInternalTx tx: activeTxs) {
            TransactionState txState = tx.state();

            // Checks COMMITTING / COMMITTED transactions due to concurrency with transactions: some active transactions
            // start committing after being grabbed.
            if (txState == PREPARING || txState == PREPARED || txState == COMMITTING || txState == COMMITTED) {
                if (!beforeCutTxs.contains(tx.nearXidVersion()))
                    activeTxsFinishFut.add(tx.finishFuture());
            }
        }

        activeTxsFinishFut.markInitialized();

        return activeTxsFinishFut.chain(activeFinished -> {
            Collection<IgniteInternalFuture<IgniteInternalTx>> finishedTxs = activeTxsFinishFut.futures();

            Set<GridCacheVersion> beforeCut = new HashSet<>();

            for (IgniteInternalFuture<IgniteInternalTx> finished: finishedTxs) {
                IgniteInternalTx tx = finished.result();

                ConsistentCutVersionAware txCutVerAware = (ConsistentCutVersionAware)tx;

                if (ver.compareTo(txCutVerAware.txCutVersion()) > 0)
                    beforeCut.add(tx.nearXidVersion());
            }

            GridFutureAdapter<Void> cutFinished = new GridFutureAdapter<>();

            try {
                walLog(ver, new ConsistentCutFinishRecord(beforeCut));

                cutFinished.onDone();
            }
            catch (IgniteCheckedException e) {
                cutFinished.onDone(e);
            }

            return cutFinished;
        });
    }

    /**
     * If specified transaction is on the AFTER side it's required to await while Consistent Cut has written to WAL.
     *
     * @param txCutVer Consistent Cut Version with that the transaction is signed.
     */
    void processTxBeforeCommit(long txCutVer) throws IgniteCheckedException {
        if (ver.compareTo(txCutVer) == 0 && !walWritten.isDone())
            walWritten.get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * @return {@code true} if Consistent Cut collected list of active transactions to check.
     */
    boolean activeTxCollectingFinished() {
        return activeTxs != null;
    }

    /**
     * @return {@code true} if specified transaction is on the BEFORE side of this Consistent Cut.
     */
    boolean txBeforeCut(long txCutVer) {
        return txCutVer != -1 && ver.compareTo(txCutVer) > 0;
    }

    /**
     * Logs Consistent Cut Record to WAL.
     */
    protected void walLog(ConsistentCutVersion cutVer, WALRecord record) throws IgniteCheckedException {
        if (cctx.wal() != null) {
            if (log.isDebugEnabled())
                log.debug("Write ConsistentCut[" + cutVer + "] record to WAL: " + record);

            cctx.wal().log(record);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder bld = new StringBuilder("ConsistentCut[");

        bld.append("ver=").append(ver).append(", ");

        bld.append("activeTxs").append("={");

        if (activeTxs == null)
            bld.append("}");
        else {
            for (IgniteInternalTx tx: activeTxs) {
                ConsistentCutVersionAware txCutVerAware = (ConsistentCutVersionAware)tx;

                bld
                    .append("id=")
                    .append(txCutVerAware.nearXidVersion().asIgniteUuid())
                    .append(", ")
                    .append("txCutVer=")
                    .append(txCutVerAware.txCutVersion());
            }
        }

        bld.append("}");

        return bld.append("]").toString();
    }
}
