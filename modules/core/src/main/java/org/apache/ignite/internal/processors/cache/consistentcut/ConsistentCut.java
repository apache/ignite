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
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
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
     * Consistent Cut Version.
     */
    @GridToStringInclude
    private final ConsistentCutVersion ver;

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
     * @param committingTxs Reference to mutable collection of committing transactions.
     * @return Future that completes after local Consistent Cut finished.
     */
    protected IgniteInternalFuture<?> init(Collection<IgniteInternalTx> committingTxs) throws IgniteCheckedException {
        Collection<IgniteInternalTx> activeTxs = new ArrayList<>(cctx.tm().activeTransactions());

        walLog(ver, new ConsistentCutStartRecord(ver));

        GridCompoundFuture<IgniteInternalTx, Void> activeTxsFinishFut = new GridCompoundFuture<>();

        // It guarantees that there is no missed transactions for checking:
        // 1. `committingTxs` and `activeTxs` may have some duplicated txs - tx is firstly added to `committingTxs` and
        //     only after that it is removed from `activeTxs`.
        // 2. `committingTxs` collection stops filling after collecting `activeTxs` and stops cleaning when new cut version
        //     received.
        // 3. Transaction with new cut version appeared in `committingTxs` after this version started to handle.
        for (IgniteInternalTx tx: committingTxs)
            activeTxsFinishFut.add(tx.finishFuture());

        for (IgniteInternalTx tx: activeTxs) {
            TransactionState txState = tx.state();

            // Checks COMMITTING / COMMITTED transactions due to concurrency with transactions: some active transactions
            // start committing after being grabbed.
            if (txState == PREPARING || txState == PREPARED || txState == COMMITTING || txState == COMMITTED)
                activeTxsFinishFut.add(tx.finishFuture());
        }

        activeTxsFinishFut.markInitialized();

        return activeTxsFinishFut.chain(activeFinished -> {
            Collection<IgniteInternalFuture<IgniteInternalTx>> finishedTxs = activeTxsFinishFut.futures();

            Set<GridCacheVersion> beforeCut = new HashSet<>();
            Set<GridCacheVersion> afterCut = new HashSet<>();

            for (IgniteInternalFuture<IgniteInternalTx> finished: finishedTxs) {
                IgniteInternalTx tx = finished.result();

                committingTxs.remove(tx);

                ConsistentCutVersionAware txCutVerAware = (ConsistentCutVersionAware)tx;

                if (ver.compareTo(txCutVerAware.txCutVersion()) > 0)
                    beforeCut.add(tx.nearXidVersion());
                else
                    afterCut.add(tx.nearXidVersion());
            }

            GridFutureAdapter<Void> cutFinished = new GridFutureAdapter<>();

            try {
                walLog(ver, new ConsistentCutFinishRecord(beforeCut, afterCut));

                cutFinished.onDone();
            }
            catch (IgniteCheckedException e) {
                cutFinished.onDone(e);
            }

            return cutFinished;
        });
    }

    /**
     * Logs Consistent Cut Record to WAL.
     */
    protected boolean walLog(ConsistentCutVersion cutVer, WALRecord record) throws IgniteCheckedException {
        if (cctx.wal() != null) {
            if (log.isDebugEnabled())
                log.debug("Write ConsistentCut[" + cutVer + "] record to WAL: " + record);

            cctx.wal().log(record);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConsistentCut.class, this);
    }
}
