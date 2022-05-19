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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Describes current Consistent Cut status.
 */
public class ConsistentCutState {
    /**
     * Version, actually it's a timestamp of started Consistent Cut.
     */
    private final long ver;

    /**
     * Whether local Consistent Cut procedure is fully completed.
     */
    private final AtomicBoolean completed = new AtomicBoolean();

    /**
     * Set of transactions to include into this CutVersion.
     */
    private final Set<GridCacheVersion> include = ConcurrentHashMap.newKeySet();

    /**
     * Set of transactions to include into next CutVersion.
     */
    private final Set<GridCacheVersion> includeNext = ConcurrentHashMap.newKeySet();

    /**
     * Map of transactions that were prepared on local node.
     *
     * Key is transaction version on near node.
     * Value is CutVersion that used to notify remote nodes whether to include those transactions to specific CutVersion
     * or not. Notification is sent with `GridTxFinishRequest`.
     *
     * @see GridNearTxFinishRequest
     * @see GridDhtTxFinishRequest
     */
    private final Map<GridCacheVersion, Long> nearPrepare = new ConcurrentHashMap<>();

    /**
     * Map of transactions that was included in CutVersion but still waiting for check.
     *
     * Key is transaction version on near node.
     * Value is CutVersion of this Consistent Cut.
     */
    private final Map<GridCacheVersion, Long> cutAwait = new ConcurrentHashMap<>();

    /**
     * Set of transactions from {@link #cutAwait} to include to this CutVersion.
     */
    private final Set<GridCacheVersion> checkInclude = new HashSet<>();

    /** */
    public ConsistentCutState(long ver) {
        this.ver = ver;
    }

    /** Excludes specified transaction from this CutVersion. */
    public void exclude(GridCacheVersion txId) {
        checkInclude.remove(txId);

        includeNext.add(txId);
    }

    /**
     * Collection of 2PC transactions that originated on local node.
     */
    public Map<GridCacheVersion, Long> nearPrepare() {
        return nearPrepare;
    }

    /** */
    public void nearPrepare(GridCacheVersion txId, long lastVer) {
        nearPrepare.put(txId, lastVer);
    }

    /**
     * Collection of transactions to await to decide which CutVersion they belong to.
     */
    public Map<GridCacheVersion, Long> cutAwait() {
        return cutAwait;
    }

    /** */
    public void cutAwait(GridCacheVersion txId, long lastVer) {
        cutAwait.put(txId, lastVer);
        checkInclude.add(txId);
    }

    /**
     * Set of transactions to include to CutVersion (incl. {@link #cutAwait()}).
     */
    public Set<GridCacheVersion> include() {
        return include;
    }

    /** */
    public void include(GridCacheVersion txId) {
        include.add(txId);
    }

    /**
     * Set of transactions to include to next CutVersion.
     */
    public Set<GridCacheVersion> includeNext() {
        return includeNext;
    }

    /** */
    public void includeNext(GridCacheVersion txId) {
        includeNext.add(txId);
    }

    /**
     * Set of transactions to include to CutVersion (after checking them).
     */
    public Set<GridCacheVersion> checkInclude() {
        return checkInclude;
    }

    /**
     * Consistent Cut Version.
     */
    public long ver() {
        return ver;
    }

    /**
     * Complete local Consistent Cut procedure.
     */
    public boolean complete() {
        return completed.compareAndSet(false, true);
    }

    /**
     * Complete local Consistent Cut procedure.
     */
    public boolean complete(GridCacheVersion tx) {
        return includeNext.remove(tx);
    }

    /**
     * Whether local Consistent Cut procedure is completed.
     */
    public boolean completed() {
        return completed.get() && includeNext.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder bld = new StringBuilder();

        bld.append("ver=").append(ver).append(", ");

        mapAppend(bld, "nearPrepare", nearPrepare);
        mapAppend(bld, "cutAwait", cutAwait);
        setAppend(bld, "include", include);
        setAppend(bld, "checkInclude", checkInclude);
        setAppend(bld, "includeNext", includeNext);

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
