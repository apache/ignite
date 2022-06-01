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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;

/**
 * Describes current Consistent Cut state.
 *
 * @see ConsistentCutStartRecord
 * @see ConsistentCutFinishRecord
 */
class ConsistentCutState {
    /**
     * Consistent Cut Version. It's a timestamp of starting the Consistent Cut algorithm on Ignite coordinator node.
     */
    private final long ver;

    /**
     * Previous Consistent Cut Version.
     */
    private final long prevVer;

    /**
     * ID of node that coordinates Consistent Cut procedure.
     */
    private final UUID crdNodeId;

    /**
     * Whether local Consistent Cut procedure is finished. It means that all transactions in {@link #check}
     * analyzed, and it's known which transactions are part of this Consistent Cut.
     */
    private final AtomicBoolean finished = new AtomicBoolean();

    /**
     * Whether it's safe to start new Consistent Cut procedure. It means that it's {@link #finished()} and all
     * transactions in {@link #includeAfter} committed.
     */
    private final AtomicBoolean ready = new AtomicBoolean();

    /**
     * Set of transactions to include into this Consistent Cut.
     */
    private final Set<GridCacheVersion> includeBefore = ConcurrentHashMap.newKeySet();

    /**
     * Set of transactions to exclude from this Consistent Cut (to include to the global AFTER state).
     */
    private final Map<GridCacheVersion, IgniteInternalTx> includeAfter = new ConcurrentHashMap<>();

    /**
     * Map of transactions that bound to specific Consistent Cut Version.
     * Key is a transaction version. Value is the latest Consistent Cut Version AFTER which this transaction committed.
     */
    private final Map<GridCacheVersion, Long> txCutVers = new ConcurrentHashMap<>();

    /**
     * Map of transactions that are required to be checked whether to include them to this Consistent Cut. Such transactions
     * are waiting for notifications from other nodes.
     *
     * Key is a transaction version. Value is Consistent Cut Version.
     */
    private final Map<GridCacheVersion, T2<IgniteInternalTx, Long>> check = new ConcurrentHashMap<>();

    /**
     * Set of transactions from {@link #check} to include into this Consistent Cut.
     */
    private final Set<GridCacheVersion> checkIncludeBefore = new HashSet<>();

    /** */
    public ConsistentCutState(UUID crdNodeId, long ver, long prevVer) {
        this.crdNodeId = crdNodeId;
        this.ver = ver;
        this.prevVer = prevVer;
    }

    /**
     * @return Consistent Cut coordinator node ID.
     */
    public UUID crdNodeId() {
        return crdNodeId;
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
     * @return Collection of transactions awaited for check.
     */
    public Collection<IgniteInternalTx> checkList() {
        if (check.isEmpty())
            return Collections.emptyList();

        return check.values().stream().map(T2::get1).collect(Collectors.toSet());
    }

    /**
     * @return Collection of transactions to be excluded from this Consistent Cut.
     */
    public Collection<IgniteInternalTx> afterList() {
        if (includeAfter.isEmpty())
            return Collections.emptyList();

        return new ArrayList<>(includeAfter.values());
    }

    /**
     * Includes a transaction (optionally committed after Consistent Cut WAL records) to Consistent Cut.
     *
     * @param nearTxVer Transaction version on an originated node.
     */
    public void includeBeforeCut(GridCacheVersion nearTxVer) {
        includeBefore.add(nearTxVer);
    }

    /**
     * Excludes a transaction from Consistent Cut.
     *
     * @param tx Local transaction.
     */
    public void includeAfterCut(IgniteInternalTx tx) {
        includeAfter.put(tx.nearXidVersion(), tx);
    }

    /**
     * @param nearTxVer Transaction version on an originated node.
     * @return {@code true} whether specified transaction is included to Consistent Cut.
     */
    public boolean beforeCut(GridCacheVersion nearTxVer) {
        return includeBefore.contains(nearTxVer);
    }

    /**
     * @param nearTxVer Transaction version on an originated node.
     * @return {@code true} whether specified transaction is excluded from Consistent Cut.
     */
    public boolean afterCut(GridCacheVersion nearTxVer) {
        return includeAfter.containsKey(nearTxVer);
    }

    /**
     * Sets the latest Consistent Cut Version AFTER which specified transaction committed.
     *
     * @param nearTxVer Transaction version on an originated node.
     * @param cutVer Consistent Cut Version.
     */
    public void txCutVersion(GridCacheVersion nearTxVer, long cutVer) {
        txCutVers.put(nearTxVer, cutVer);
    }

    /**
     * Gets the latest Consistent Cut Version AFTER which specified transaction committed.
     *
     * @param nearTxVer Transaction version on an originated node.
     * @return Consistent Cut Version.
     */
    public Long txCutVersion(GridCacheVersion nearTxVer) {
        return txCutVers.remove(nearTxVer);
    }

    /**
     * Adds transaction to the check-list that awaits notifications from other nodes to decide whether to include specified
     * transaction to this Consistent Cut, or not.
     *
     * @param tx Local transaction.
     * @param cutVer Consistent Cut Version.
     */
    public void addForCheck(IgniteInternalTx tx, long cutVer) {
        GridCacheVersion nearTxVer = tx.nearXidVersion();

        check.put(nearTxVer, new T2<>(tx, cutVer));

        checkIncludeBefore.add(nearTxVer);
    }

    /**
     * Verifies whether specified transaction in the check-list.
     *
     * @param nearTxVer Transaction version on an originated node.
     * @return Consistent Cut Version.
     */
    public Long needCheck(GridCacheVersion nearTxVer) {
        T2<IgniteInternalTx, Long> tx = check.get(nearTxVer);

        return tx == null ? null : tx.get2();
    }

    /**
     * Excludes specified transaction from this Consistent Cut.
     *
     * @param nearTxVer Transaction version on an originated node.
     */
    public void exclude(GridCacheVersion nearTxVer) {
        checkIncludeBefore.remove(nearTxVer);

        IgniteInternalTx tx = check.get(nearTxVer).get1();

        includeAfter.put(tx.nearXidVersion(), tx);
    }

    /**
     * Tries finishing local Consistent Cut after checking specified transaction.
     *
     * @param nearTxVer Transaction version on an originated node.
     * @return Whether local Consistent Cut has finished.
     */
    public boolean tryFinish(GridCacheVersion nearTxVer) {
        return check.remove(nearTxVer) != null && check.isEmpty() && finish();
    }

    /**
     * Tries finishing local Consistent Cut. It crosses sets of included and awaited transactions. For cases when node
     * has multiple participations (e.g. near and backup) it's possible to remove some transactions from the check-list.
     */
    public void tryFinish() {
        if (check.isEmpty())
            finish();
        else {
            for (GridCacheVersion tx: includeBefore) {
                if (check.containsKey(tx))
                    tryFinish(tx);
            }

            for (GridCacheVersion tx: includeAfter.keySet()) {
                if (check.containsKey(tx)) {
                    tryFinish(tx);

                    checkIncludeBefore.remove(tx);
                }
            }
        }
    }

    /**
     * Finish local Consistent Cut.
     */
    protected boolean finish() {
        return finished.compareAndSet(false, true);
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
     * @param nearTxVer Transaction version on an originated node.
     */
    public void onCommit(GridCacheVersion nearTxVer) {
        includeAfter.remove(nearTxVer);
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
    public ConsistentCutStartRecord buildStartRecord() {
        return new ConsistentCutStartRecord(ver, includeBefore, check.keySet());
    }

    /** */
    public ConsistentCutFinishRecord buildFinishRecord() {
        ConsistentCutFinishRecord rec = new ConsistentCutFinishRecord(checkIncludeBefore);

        checkIncludeBefore.clear();

        return rec;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder bld = new StringBuilder("ConsistentCutState[");

        bld.append("ver=").append(ver).append(", ");
        bld.append("prevVer=").append(prevVer).append(", ");
        bld.append("finished=").append(finished).append(", ");
        bld.append("ready=").append(ready).append(", ");
        bld.append("crd=").append(crdNodeId).append(", ");

        setAppend(bld, "includeBefore", includeBefore);
        setAppend(bld, "includeAfter", includeAfter.keySet());

        Map<GridCacheVersion, Long> chk = new HashMap<>();
        for (Map.Entry<GridCacheVersion, T2<IgniteInternalTx, Long>> e: check.entrySet())
            chk.put(e.getKey(), e.getValue().get2());

        mapAppend(bld, "check", chk);
        setAppend(bld, "checkInclude", checkIncludeBefore);
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
