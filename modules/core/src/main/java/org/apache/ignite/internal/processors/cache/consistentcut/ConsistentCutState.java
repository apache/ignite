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
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Describes current Consistent Cut state.
 */
public class ConsistentCutState {
    /**
     * Consistent Cut Version. It's a timestamp of start Consistent Cut algorithm on Ignite coordinator node.
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
     * Set of transactions to include into this Consistent Cut.
     */
    private final Set<GridCacheVersion> include = ConcurrentHashMap.newKeySet();

    /**
     * Map of transactions that are originated and prepared on local node.
     *
     * Key is a transaction version. Value is the latest Consistent Cut Version that doesn't include this transaction.
     * It is used to notify remote nodes whether to include those transactions to this Consistent Cut or not.
     * The notification is sent with finish requests.
     *
     * @see GridNearTxFinishRequest
     * @see GridDhtTxFinishRequest
     */
    private final Map<GridCacheVersion, Long> nearPrepare = new ConcurrentHashMap<>();

    /**
     * Map of transactions that are required to be checked whether to include them to this Consistent Cut. Such transactions
     * are waiting for notifications from other nodes.
     *
     * Key is a transaction version. Value is Consistent Cut Version.
     */
    private final Map<GridCacheVersion, Long> check = new ConcurrentHashMap<>();

    /**
     * Set of transactions from {@link #check} to include to this CutVersion.
     */
    private final Set<GridCacheVersion> checkInclude = new HashSet<>();

    /** */
    public ConsistentCutState(long ver, long prevVer) {
        this.ver = ver;
        this.prevVer = prevVer;
    }

    /**
     * @return Consistent Cut Version.
     */
    public long ver() {
        return ver;
    }

    /**
     * @return Previous Consistent Cut Version.
     */
    public long prevVer() {
        return prevVer;
    }

    /**
     * Includes a transaction (optionally committed AFTER) to Consistent Cut.
     *
     * @param nearTxVer Transaction version on an originated node.
     */
    public void include(GridCacheVersion nearTxVer) {
        include.add(nearTxVer);
    }

    /**
     * @param nearTxVer Transaction version on an originated node.
     * @return {@code true} whether specified transaction included to Consistent Cut.
     */
    public boolean includes(GridCacheVersion nearTxVer) {
        return include.contains(nearTxVer);
    }

    /**
     * Sets the latest Consistent Cut Version that doesn't include specified transaction.
     *
     * @param nearTxVer Transaction version on an originated node.
     * @param cutVer Consistent Cut Version.
     */
    public void txCutVer(GridCacheVersion nearTxVer, long cutVer) {
        nearPrepare.put(nearTxVer, cutVer);
    }

    /**
     * Gets the latest Consistent Cut Version that doesn't include specified transaction.
     *
     * @param nearTxVer Transaction version on an originated node.
     * @return Consistent Cut Version.
     */
    public Long txCutVer(GridCacheVersion nearTxVer) {
        return nearPrepare.remove(nearTxVer);
    }

    /**
     * Adds transaction to the check-list that awaits notifications from other nodes to decide whether to include specified
     * transaction to this Consistent Cut, or not.
     *
     * @param nearTxVer Transaction version on an originated node.
     * @param cutVer Consistent Cut Version.
     */
    public void addForCheck(GridCacheVersion nearTxVer, long cutVer) {
        check.put(nearTxVer, cutVer);

        checkInclude.add(nearTxVer);
    }

    /**
     * Verifies whether specified transaction in the check-list.
     *
     * @param nearTxVer Transaction version on an originated node.
     * @return Consistent Cut Version.
     */
    public Long needCheck(GridCacheVersion nearTxVer) {
        return check.get(nearTxVer);
    }

    /**
     * @return Actual check-list of transactions that are awaited notifications from remote nodes.
     */
    public Set<GridCacheVersion> checkList() {
        return new HashSet<>(check.keySet());
    }

    /**
     * Excludes specified transaction from this Consistent Cut.
     *
     * @param nearTxVer Transaction version on an originated node.
     */
    public void exclude(GridCacheVersion nearTxVer) {
        checkInclude.remove(nearTxVer);
    }

    /**
     * Tries to finish Consistent Cut after checking specified transaction.
     *
     * @param nearTxVer Transaction version on an originated node.
     * @return Whether Consistent Cut finished.
     */
    public boolean tryFinish(GridCacheVersion nearTxVer) {
        return check.remove(nearTxVer) != null && check.isEmpty() && finish();
    }

    /**
     * Tries to finish Consistent Cut after crossing sets of included and awaited transactions. For cases when node
     * has multiple participations (e.g. near and backup) it's possible to remove some transactions from the check-list.
     */
    public void tryFinish() {
        if (check.isEmpty())
            finish();
        else {
            for (GridCacheVersion inclTx: include)
                tryFinish(inclTx);
        }
    }

    /**
     * Finish local Consistent Cut.
     */
    protected boolean finish() {
        return finished.compareAndSet(false, true);
    }

    /**
     * Whether local Consistent Cut finished.
     */
    public boolean finished() {
        return finished.get();
    }

    /** */
    public ConsistentCutStartRecord buildStartRecord() {
        return new ConsistentCutStartRecord(ver, include, check.keySet());
    }

    /** */
    public ConsistentCutFinishRecord buildFinishRecord() {
        return new ConsistentCutFinishRecord(ver, checkInclude);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder bld = new StringBuilder();

        bld.append("ver=").append(ver).append(", ");

        setAppend(bld, "include", include);
        mapAppend(bld, "check", check);
        setAppend(bld, "checkInclude", checkInclude);
        mapAppend(bld, "nearPrepare", nearPrepare);

        return bld.toString();
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
