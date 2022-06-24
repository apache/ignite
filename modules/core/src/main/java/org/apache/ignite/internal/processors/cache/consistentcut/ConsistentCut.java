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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
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
     * It is set to {@code true} after grabbing Consistent Cut grabbed collection of active transactions to check.
     *
     * Every transaction appeared after completion of this future belongs to the AFTER side of this Consistent Cut.
     */
    private volatile boolean grabbedTxs;

    /**
     * Future completes after writing {@link ConsistentCutStartRecord}.
     *
     * Every transaction committed before completion of this future belongs to the BEFORE side of this Consistent Cut.
     */
    private final GridFutureAdapter<Void> walWritten = new GridFutureAdapter<>();

    /**
     * Future completes after building final check-list {@link #check} of transactions.
     */
    private final GridFutureAdapter<Void> prepared = new GridFutureAdapter<>();

    /**
     * Whether local Consistent Cut procedure is finished: {@link #check} is empty and {@link ConsistentCutFinishRecord} was written.
     */
    private final AtomicBoolean finished = new AtomicBoolean();

    /**
     * Collection of transactions to include into the BEFORE side of this Consistent Cut. It contains ID of transactions
     * on near node, as this ID are written to WAL for every committed transaction in {@link TxRecord}.
     */
    private Set<GridCacheVersion> includeBefore;

    /**
     * The check-list of transactions to find which side of this Consistent Cut they belong to.
     */
    private Set<GridCacheVersion> check;

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
     * Prepares Consistent Cut: write {@link ConsistentCutStartRecord} and prepares the check-list {@link #check}.
     *
     * @param beforeCutRef Reference to mutable collection of transactions. All transactions from this collection belong
     *                     to the BEFORE side.
     */
    protected boolean prepare(Collection<IgniteInternalTx> beforeCutRef) throws IgniteCheckedException {
        Collection<IgniteInternalTx> activeTxs = new HashSet<>(cctx.tm().activeTransactions());

        grabbedTxs = true;

        Set<GridCacheVersion> beforeCutTxs = new HashSet<>();

        // It guarantees that there is no missed transactions for checking:
        // 1. `beforeCutRef` and `activeTxs` may have some duplicated txs - tx is firstly added to `beforeCutRef` and
        //    only after that it is removed from `activeTxs`.
        // 2. `beforeCutRef` collection stops filling after grabbing `activeTxs`. It still may receive some updates,
        //    but they duplicated with the `activeTxs` and will be cleaned before adding to the check-list.
        // 3. It prepares before writing StartRecord to WAL. Then it's safe to remove concurrently some txs after
        //    they were committed at `ConsistentCutManager#unregisterAfterCommit`.
        for (IgniteInternalTx tx: beforeCutRef)
            beforeCutTxs.add(tx.nearXidVersion());

        walLog(ver, new ConsistentCutStartRecord(ver));

        walWritten.onDone();

        Set<GridCacheVersion> checkTxs = new HashSet<>();

        for (IgniteInternalTx tx: activeTxs) {
            TransactionState txState = tx.state();

            // Checks COMMITTING / COMMITTED transactions due to concurrency with transactions: some active transactions
            // start committing after being grabbed.
            if (txState == PREPARING || txState == PREPARED || txState == COMMITTING || txState == COMMITTED) {
                if (!beforeCutTxs.contains(tx.nearXidVersion())) {
                    long txCutVer = ((ConsistentCutVersionAware)tx).txCutVersion();

                    // Adds transactions to the check-list iff they don't know their Consistent Cut version.
                    if (txCutVer == -1)
                        checkTxs.add(tx.xidVersion());
                    else if (ver.compareTo(txCutVer) > 0)
                        beforeCutTxs.add(tx.nearXidVersion());
                }
            }
        }

        includeBefore = ConcurrentHashMap.newKeySet();
        includeBefore.addAll(beforeCutTxs);

        check = ConcurrentHashMap.newKeySet();
        check.addAll(checkTxs);

        prepared.onDone();

        return tryFinish();
    }

    /**
     * If specified transaction is on the AFTER side it's required to await while Consistent Cut has written to WAL.
     *
     * @param txCutVer Consistent Cut Version with that the transaction is signed.
     */
    void processTxBeforeCommit(long txCutVer) throws IgniteCheckedException {
        if (ver.compareTo(txCutVer) == 0)
            walWritten.get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * If needed it checks committed transaction to find which side of Consistent Cut this transaction belongs to.
     *
     * @param needCheck {@code true} if check this transaction, otherwise {@code false}.
     * @param tx Transaction to check after commit.
     */
    boolean processTxAfterCommit(IgniteInternalTx tx, boolean needCheck) throws IgniteCheckedException {
        if (needCheck) {
            if (!prepared.isDone())
                prepared.get(TIMEOUT, TimeUnit.MILLISECONDS);

            checkTransaction(tx);
        }

        return prepared.isDone() && tryFinish();
    }

    /**
     * @return {@code true} if Consistent Cut grabbed list of transactions to check.
     */
    boolean grabTransactionsInProgress() {
        return !grabbedTxs;
    }

    /**
     * @return {@code true} if specified transaction is on the BEFORE side of this Consistent Cut.
     */
    boolean txBeforeCut(long txCutVer) {
        return txCutVer != -1 && ver.compareTo(txCutVer) > 0;
    }

    /**
     * Checks specified transaction: it finds which side of Consistent Cut it belongs to.
     *
     * @param tx Transaction to check.
     */
    private void checkTransaction(IgniteInternalTx tx) {
        ConsistentCutVersionAware aware = (ConsistentCutVersionAware)tx;

        if (check.contains(tx.xidVersion())) {
            if (ver.compareTo(aware.txCutVersion()) > 0)
                includeBefore.add(tx.nearXidVersion());

            check.remove(tx.xidVersion());
        }

        if (log.isDebugEnabled())
            log.debug("`checkTransaction` " + tx.xid() + " cutVer=" + aware.txCutVersion() + " " + this);
    }

    /**
     * Tries to finish local Consistent Cut.
     *
     * @return {@code true} if Consistent Cut finished, otherwise {@code false}.
     */
    private boolean tryFinish() throws IgniteCheckedException {
        if (check.isEmpty() && finished.compareAndSet(false, true)) {
            walLog(ver, new ConsistentCutFinishRecord(includeBefore));

            return true;
        }

        return finished.get();
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
        StringBuilder bld = new StringBuilder("ConsistentCutState[");

        bld.append("ver=").append(ver).append(", ");
        bld.append("finished=").append(finished).append(", ");

        setAppend(bld, "includeBefore", includeBefore);
        setAppend(bld, "check", check);

        return bld.append("]").toString();
    }

    /** */
    private void mapAppend(StringBuilder bld, String name, Map<GridCacheVersion, Long> m) {
        bld.append(name).append("={");

        if (m == null) {
            bld.append("}");

            return;
        }

        for (Map.Entry<GridCacheVersion, Long> e: m.entrySet()) {
            bld
                .append(e.getKey().asIgniteUuid())
                .append("=")
                .append(e.getValue())
                .append(", ");
        }

        bld.append("}, ");
    }

    /** */
    private void setAppend(StringBuilder bld, String name, Set<GridCacheVersion> s) {
        bld.append(name).append("={");

        if (s == null) {
            bld.append("}");

            return;
        }

        for (GridCacheVersion e: s) {
            bld
                .append(e.asIgniteUuid())
                .append(", ");
        }

        bld.append("}, ");
    }
}
