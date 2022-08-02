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
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;
import static org.apache.ignite.transactions.TransactionState.UNKNOWN;

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
     * Set of checked transactions belonging to the BEFORE side.
     */
    @GridToStringInclude
    private Set<GridCacheVersion> beforeCut;

    /**
     * Set of checked transactions belonging to the AFTER side.
     */
    @GridToStringInclude
    private Set<GridCacheVersion> afterCut;

    /** */
    ConsistentCut(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;

        log = cctx.logger(ConsistentCut.class);
    }

    /**
     * Inits local Consistent Cut: prepares list of active transactions to check which side of Consistent Cut they belong to.
     *
     * @param ver Consistent Cut version.
     */
    protected void init(ConsistentCutVersion ver) throws IgniteCheckedException {
        beforeCut = ConcurrentHashMap.newKeySet();
        afterCut = ConcurrentHashMap.newKeySet();

        started = walLog(new ConsistentCutStartRecord(ver));

        Iterator<IgniteInternalTx> activeTxs = F.concat(
            cctx.tm().activeTransactions().iterator(),
            cctx.consistentCutMgr().committingTxs().iterator());

        GridCompoundFuture<Throwable, Throwable> checkFut = checkTransactions(ver, activeTxs);

        checkFut.listen(failedTx -> {
            if (failedTx.result() != null)
                U.error(log, "Skip writing ConsistentCutFinishRecord due to transaction failure. ", failedTx.result());
            else {
                try {
                    walLog(new ConsistentCutFinishRecord(beforeCut, afterCut));
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to write ConsistentCutFinishRecord to WAL for ver " + ver, e);
                }
            }

            cctx.consistentCutMgr().onFinish();
        });
    }

    /**
     * Checks active transactions - decides which side of Consistent Cut they belong to after they finished.
     *
     * @param ver Current Consistent Cut version.
     * @param activeTxs Collection of active transactions to check.
     * @return Compound future that completes after all active transactions were checked.
     */
    private GridCompoundFuture<Throwable, Throwable> checkTransactions(
        ConsistentCutVersion ver,
        Iterator<IgniteInternalTx> activeTxs
    ) {
        GridCompoundFuture<Throwable, Throwable> checkFut = new GridCompoundFuture<>(new FailedTxReducer());

        while (activeTxs.hasNext()) {
            IgniteInternalTx activeTx = activeTxs.next();

            if (needCheck(activeTx.state())) {
                IgniteInternalFuture<Throwable> txCheckFut = activeTx.finishFuture().chain(txFut -> {
                    // txFut never fails and always returns IgniteInternalTx.
                    IgniteInternalTx tx = txFut.result();

                    if (tx.state() == UNKNOWN) {
                        return new IgniteCheckedException(
                            "Transaction is in UNKNOWN state. Cluster might be inconsistent. Transaction: " + tx);
                    }

                    ConsistentCutVersionAware txCutVerAware = (ConsistentCutVersionAware)tx;

                    // txCutVersion may be NULL for some cases with transaction recovery - NULL means that transaction
                    // committed BEFORE this Cut on remote nodes.
                    if (ver.compareToNullable(txCutVerAware.txCutVersion()) > 0)
                        beforeCut.add(tx.nearXidVersion());
                    else
                        afterCut.add(tx.nearXidVersion());

                    return null;
                });

                checkFut.add(txCheckFut);
            }
        }

        return checkFut.markInitialized();
    }

    /**
     * Checks only transactions that might affect data consistency: are going to commit or failed (UNKNOWN state).
     */
    private boolean needCheck(TransactionState txState) {
        return txState == PREPARING
            || txState == PREPARED
            || txState == COMMITTING
            || txState == COMMITTED
            || txState == UNKNOWN;
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

        return "ConsistentCut [started=" + started + ", before=" + before + ", after=" + after + "]";
    }

    /** Checks finished transactions, in case of any failed return exception. */
    private static class FailedTxReducer implements IgniteReducer<Throwable, Throwable> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private volatile Throwable err;

        /** {@inheritDoc} */
        @Override public boolean collect(@Nullable Throwable throwable) {
            if (throwable != null) {
                err = throwable;

                return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public Throwable reduce() {
            return err;
        }
    }
}
