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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.transactions.TransactionState;

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
     * Whether it's safe to start new Consistent Cut procedure. It means that it's {@link #finished} and all
     * transactions in {@link #includeBefore} committed.
     */
    private final AtomicBoolean ready = new AtomicBoolean();

    /**
     * Collection of transactions to include to this Consistent Cut (to include to the global BEFORE state). It contains
     * ID of transaction on near node (as exactly this ID is written to WAL).
     */
    private final Set<GridCacheVersion> includeBefore = ConcurrentHashMap.newKeySet();

    /**
     * Collection of transactions to exclude from this Consistent Cut (to include to the global AFTER state). It contains
     * ID of local transaction (not ID on near node like in {@link #includeBefore}).
     */
    private final Set<GridCacheVersion> includeAfter = new HashSet<>();

    /**
     * Collection of transactions to await before notify Consistent Cut coordinator.
     */
    private final Map<GridCacheVersion, IgniteInternalTx> readyAwait = new ConcurrentHashMap<>();

    /**
     * Collection of transactions that are required to be checked whether to include them to this Consistent Cut. Such
     * transactions are waiting for notifications from other nodes.
     */
    private final Set<GridCacheVersion> check = ConcurrentHashMap.newKeySet();

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
     * Excludes a transaction from BEFORE side, and move it to AFTER side.
     *
     * @param tx Local transaction.
     */
    public void includeAfterCut(IgniteInternalTx tx) {
        includeAfter.add(tx.xidVersion());
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
        return includeAfter.contains(txVer);
    }

    /**
     * Adds transaction to the check-list that awaits notifications from other nodes to decide whether to include specified
     * transaction to this Consistent Cut, or not.
     *
     * @param tx Transaction.
     */
    public void addForCheck(IgniteInternalTx tx) {
        check.add(tx.xidVersion());
    }

    /**
     * Excludes specified transaction from this Consistent Cut.
     *
     * @param txVer Transaction version.
     */
    public boolean checkExclude(GridCacheVersion txVer) {
        return tryFinish(txVer);
    }

    /**
     * Includes specified transaction to this Consistent Cut.
     *
     * @param tx Transaction to include.
     */
    public boolean checkInclude(IgniteInternalTx tx) {
        GridCacheVersion txVer = tx.xidVersion();

        includeBefore.add(tx.nearXidVersion());

        // Transaction can be COMMITTING or COMMITTED here. In case of COMMITTED no need to await it.
        if (tx != null && tx.state() == TransactionState.COMMITTING)
            readyAwait.put(txVer, tx);

        return tryFinish(txVer);
    }

    /**
     * Tries finishing local Consistent Cut after checking specified transaction.
     *
     * @return Whether local Consistent Cut has finished.
     */
    public boolean tryFinish() {
        return check.isEmpty() && finished.compareAndSet(false, true);
    }

    /**
     * Tries finishing local Consistent Cut after checking specified transaction.
     *
     * @param txVer Transaction version, optional.
     * @return Whether local Consistent Cut has finished.
     */
    public boolean tryFinish(GridCacheVersion txVer) {
        if (txVer != null)
            check.remove(txVer);

        return tryFinish();
    }

    /**
     * Notifies about specified transaction committed.
     *
     * @param txVer Transaction version.
     */
    public void onCommit(GridCacheVersion txVer) {
        readyAwait.remove(txVer);
    }

    /**
     * Checks whether this node is ready for new Consistent Cut.
     *
     * @return {@code true} if it's ready to start new Consistent Cut.
     */
    public boolean tryReady() {
        return finished.get() && readyAwait.isEmpty() && ready.compareAndSet(false, true);
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
        bld.append("finished=").append(finished).append(", ");
        bld.append("ready=").append(ready).append(", ");

        setAppend(bld, "includeBefore", includeBefore);
        setAppend(bld, "includeAfter", includeAfter);
        setAppend(bld, "check", check);
        setAppend(bld, "await", readyAwait.keySet());

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
