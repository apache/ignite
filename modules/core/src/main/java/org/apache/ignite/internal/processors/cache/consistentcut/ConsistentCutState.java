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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

/**
 * Describes current Consistent Cut state.
 *
 * @see ConsistentCutFinishRecord
 */
public class ConsistentCutState {
    /**
     * Consistent Cut Version. It's a timestamp of starting the Consistent Cut algorithm on Ignite coordinator node.
     */
    private final long ver;

    /**
     * Previous Consistent Cut Version.
     */
    private final long prevVer;

    /**
     * Whether local Consistent Cut procedure is finished. It means that all transactions in {@link #check}
     * analyzed, and it's known which transactions are part of this Consistent Cut.
     */
    private final AtomicBoolean finished = new AtomicBoolean();

    /**
     * Whether it's allowed to finish Consistent Cut. After publishing this state it's still possible to add some
     * transactions to {@link #includeBefore}, as it may receieve FinishRequest concurrently with preparing this state.
     * Then after it's prohibited to finish until all transactions aren't handled.
     */
    private final AtomicBoolean allowFinish = new AtomicBoolean();

    /**
     * Whether it's safe to start new Consistent Cut procedure. It means that it's {@link #finished()} and all
     * transactions in {@link #includeAfter} committed.
     */
    private final AtomicBoolean ready = new AtomicBoolean();

    /**
     * Set of transactions IDs on near node to include into this Consistent Cut.
     */
    private final Set<GridCacheVersion> includeBefore = ConcurrentHashMap.newKeySet();

    /**
     * Collection of transactions to exclude from this Consistent Cut (to include to the global AFTER state).
     * Key is transaction ID, value is related transaction.
     */
    private final Map<GridCacheVersion, IgniteInternalTx> includeAfter = new ConcurrentHashMap<>();

    /**
     * Map of transactions that bound to specific Consistent Cut Version.
     * Key is transaction ID. Value is the latest finished Consistent Cut Version AFTER which this transaction committed.
     */
    private final Map<GridCacheVersion, Long> txCutVers = new ConcurrentHashMap<>();

    /**
     * Collection of transactions that are required to be checked whether to include them to this Consistent Cut. Such
     * transactions are waiting for notifications from other nodes. Key is transaction ID. Value is related transaction.
     */
    private final Map<GridCacheVersion, IgniteInternalTx> check = new ConcurrentHashMap<>();

    /** */
    public ConsistentCutState(long ver, long prevVer) {
        this.ver = ver;
        this.prevVer = prevVer;
    }

    /**
     * @return Consistent Cut Version.
     */
    public long version() {
        return ver;
    }

    /**
     * @return Previous Consistent Cut Version.
     */
    public long prevVersion() {
        return prevVer;
    }

    /**
     * Includes a transaction (optionally committed after Consistent Cut WAL records) to Consistent Cut.
     *
     * @param nearTxVer Transaction version on near node.
     */
    public boolean includeBeforeCut(GridCacheVersion txVer, GridCacheVersion nearTxVer) {
        includeBefore.add(nearTxVer);

        return tryFinish(txVer);
    }

    /**
     * Excludes a transaction from Consistent Cut.
     *
     * @param tx Local transaction.
     */
    public void includeAfterCut(IgniteInternalTx tx) {
        includeAfter.put(tx.xidVersion(), tx);
    }

    /**
     * @param nearTxVer Transaction version on near node.
     * @return {@code true} whether specified transaction is included to Consistent Cut.
     */
    public boolean beforeCut(GridCacheVersion nearTxVer) {
        return includeBefore.contains(nearTxVer);
    }

    /**
     * @param txVer Transaction version.
     * @return {@code true} whether specified transaction is excluded from Consistent Cut.
     */
    public boolean afterCut(GridCacheVersion txVer) {
        return includeAfter.containsKey(txVer);
    }

    /**
     * Sets the latest finished Consistent Cut Version AFTER which specified transaction committed.
     *
     * @param txVer Transaction version.
     * @param cutVer Consistent Cut Version.
     */
    public void txCutVersion(GridCacheVersion txVer, long cutVer) {
        txCutVers.put(txVer, cutVer);
    }

    /**
     * Gets the latest finished Consistent Cut Version AFTER which specified transaction committed.
     *
     * @param txVer Transaction version.
     * @return Consistent Cut Version.
     */
    public Long txCutVersion(GridCacheVersion txVer) {
        return txCutVers.remove(txVer);
    }

    /**
     * Adds transaction to the check-list that awaits notifications from other nodes to decide whether to include specified
     * transaction to this Consistent Cut, or not.
     *
     * @param tx Transaction.
     */
    public void addForCheck(IgniteInternalTx tx) {
        GridCacheVersion txVer = tx.xidVersion();

        check.put(txVer, tx);
    }

    /**
     * Excludes specified transaction from this Consistent Cut.
     *
     * @param txVer Transaction version.
     */
    public boolean checkExclude(GridCacheVersion txVer) {
        IgniteInternalTx tx = check.get(txVer);

        if (tx != null) {
            if (tx.state() != TransactionState.COMMITTED)
                includeAfter.put(txVer, tx);

            return tryFinish(txVer);
        }

        return false;
    }

    /**
     * Includes specified transaction to this Consistent Cut.
     *
     * @param txVer Transaction version.
     * @param nearTxVer Transaction version on near node.
     */
    public boolean checkInclude(GridCacheVersion txVer, GridCacheVersion nearTxVer) {
        IgniteInternalTx tx = check.get(txVer);

        if (tx != null) {
            includeBefore.add(nearTxVer);

            return tryFinish(txVer);
        }

        return false;
    }

    /**
     * Tries finishing local Consistent Cut after checking specified transaction.
     *
     * @param txVer Transaction version, optional.
     * @return Whether local Consistent Cut has finished.
     */
    public boolean tryFinish(@Nullable GridCacheVersion txVer) {
        if (txVer != null)
            check.remove(txVer);

        return check.isEmpty() && finish();
    }

    /**
     * For cases when node has multiple participations (e.g. near and backup) it's possible to clean check-list and
     * after-list before receiving messages from remote nodes.
     */
    public void beforePublish(IgniteTxManager tm) {
        if (check.isEmpty())
            return;

        for (GridCacheVersion tx : includeBefore) {
            GridCacheVersion txVer = tm.mappedVersion(tx);

            txVer = txVer == null ? tx : txVer;

            check.remove(txVer);
        }

        for (GridCacheVersion tx : includeAfter.keySet())
            check.remove(tx);
    }

    /**
     * Finish local Consistent Cut.
     */
    private boolean finish() {
        return allowFinish.get() && finished.compareAndSet(false, true);
    }

    /**
     * Allows finish local Consistent Cut.
     */
    public void allowFinish() {
        allowFinish.set(true);
    }

    /**
     * Whether local Consistent Cut is finished.
     */
    public boolean finished() {
        return finished.get();
    }

    /**
     * Notifies about specified transaction committed.
     *
     * @param txVer Transaction version.
     */
    public void onCommit(GridCacheVersion txVer) {
        includeAfter.remove(txVer);
    }

    /**
     * Checks whether it's safe to start new Consistent Cut procedure.
     *
     * @return {@code true} if it's safe to start new Consistent Cut procedure.
     */
    public boolean checkReady() {
        return finished.get() && includeAfter.isEmpty() && ready.compareAndSet(false, true);
    }

    /**
     * @return {@code true} if it's safe to start new Consistent Cut procedure.
     */
    public boolean ready() {
        return ready.get();
    }

    /** */
    public ConsistentCutFinishRecord buildFinishRecord() {
        return new ConsistentCutFinishRecord(includeBefore);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder bld = new StringBuilder("ConsistentCutState[");

        bld.append("ver=").append(ver).append(", ");
        bld.append("prevVer=").append(prevVer).append(", ");
        bld.append("allowFinish=").append(allowFinish).append(", ");
        bld.append("finished=").append(finished).append(", ");
        bld.append("ready=").append(ready).append(", ");

        setAppend(bld, "includeBefore", includeBefore);
        setAppend(bld, "includeAfter", includeAfter.keySet());
        setAppend(bld, "check", check.keySet());

        mapAppend(bld, "txCutVers", txCutVers);

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
