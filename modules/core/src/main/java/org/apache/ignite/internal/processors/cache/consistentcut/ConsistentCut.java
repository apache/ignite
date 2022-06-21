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
     * Future completes after grabbing collection of active transactions to check.
     *
     * Every transaction appeared after completion of this future belongs to the AFTER side of this Consistent Cut.
     */
    private final GridFutureAdapter<Void> grabTxs = new GridFutureAdapter<>();

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
    public ConsistentCut(GridCacheSharedContext<?, ?> cctx, IgniteLogger log, ConsistentCutVersion ver) {
        this.ver = ver;

        this.cctx = cctx;
        this.log = log;
    }

    /**
     * @return Consistent Cut Version.
     */
    public ConsistentCutVersion version() {
        return ver;
    }

    /**
     * Checks specified transaction: finds which side of Consistent Cut it belongs to.
     *
     * @return {@code true} if Consistent Cut was finished, otherwise {@code false}.
     */
    public boolean checkTransaction(IgniteInternalTx tx) throws IgniteCheckedException {
        if (!prepared.isDone())
            return false;

        ConsistentCutVersionAware aware = (ConsistentCutVersionAware)tx;

        if (check.remove(tx.xidVersion()) && aware.txCutVersion() < ver.version())
            includeBefore.add(tx.nearXidVersion());

        if (log.isDebugEnabled())
            log.debug("`checkTransaction` " + tx.xid() + " cutVer=" + aware.txCutVersion() + " " + this);

        return tryFinish();
    }

    /**
     * Prepares Consistent Cut: write {@link ConsistentCutStartRecord} and prepares the check-list {@link #check}.
     *
     * @param beforeCutRef Reference to mutable collection of transactions. All transactions from this collection belong
     *                     to the BEFORE side.
     */
    protected boolean prepare(Collection<IgniteInternalTx> beforeCutRef) throws IgniteCheckedException {
        Collection<IgniteInternalTx> activeTxs = new HashSet<>(cctx.tm().activeTransactions());

        grabTxs.onDone();

        Set<GridCacheVersion> incl = new HashSet<>();

        // It guarantees that there is no missed transactions for checking:
        // 1. `beforeCutRef` and `activeTxs` may have some duplicated txs - tx is addes to `beforeCutRef` and after that
        // is removed from `activeTxs`.
        // 2. `beforeCutRef` collection stops filling after grabbing `activeTxs`.
        // 3. It prepares before writing StartRecord to WAL. Then it's safe to remove concurrently some txs after
        // they were committed at `ConsistentCutManager#unregisterAfterCommit`.
        for (IgniteInternalTx tx: beforeCutRef)
            incl.add(tx.nearXidVersion());

        walLog(ver, new ConsistentCutStartRecord(ver));

        walWritten.onDone();

        Set<GridCacheVersion> checkTxs = new HashSet<>();

        for (IgniteInternalTx tx: activeTxs) {
            TransactionState txState = tx.state();

            // Checks COMMITTING / COMMITTED transactions due to concurrency with transactions: some active transactions
            // start to commit after grabbing it. To avoid misses we need to add them to the `check` and await while
            // they were unregistered at `ConsistentCutManager#unregisterAfterCommit`.
            if (txState == PREPARING || txState == PREPARED || txState == COMMITTING || txState == COMMITTED) {
                if (!incl.contains(tx.nearXidVersion()))
                    checkTxs.add(tx.xidVersion());
            }
        }

        includeBefore = ConcurrentHashMap.newKeySet();
        includeBefore.addAll(incl);

        check = ConcurrentHashMap.newKeySet();
        check.addAll(checkTxs);

        prepared.onDone();

        return tryFinish();
    }

    /**
     * Awaits while Consistent Cut prepared {@link #check} list of transactions.
     */
    public void awaitPrepared() throws IgniteCheckedException {
        prepared.get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * Awaits while Consistent Cut wrote {@link ConsistentCutStartRecord} to WAL.
     */
    public void awaitWALWritten() throws IgniteCheckedException {
        walWritten.get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * @return {@code true} if Consistent Cut grabbed list of transactions to check.
     */
    public boolean grabbedTransactions() {
        return grabTxs.isDone();
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
