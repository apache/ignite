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

package org.apache.ignite.internal.processors.clock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryTopologySnapshot;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Snapshot of time deltas for given topology.
 */
public class GridClockDeltaSnapshot {
    /** Time delta version. */
    private final GridClockDeltaVersion ver;

    /** Deltas between coordinator and nodes by node ID. */
    private final Map<UUID, Long> deltas;

    /** Pending delta values. */
    @GridToStringExclude
    private final Map<UUID, DeltaAverage> pendingDeltas;

    /**
     * @param ver Snapshot version.
     * @param locNodeId Local node ID.
     * @param discoSnap Discovery snapshot.
     * @param avgSize Average size.
     */
    public GridClockDeltaSnapshot(
        GridClockDeltaVersion ver,
        UUID locNodeId,
        GridDiscoveryTopologySnapshot discoSnap,
        int avgSize
    ) {
        assert ver.topologyVersion() == discoSnap.topologyVersion();

        this.ver = ver;

        deltas = new HashMap<>(discoSnap.topologyNodes().size(), 1.0f);

        pendingDeltas = new HashMap<>(discoSnap.topologyNodes().size(), 1.0f);

        for (ClusterNode n : discoSnap.topologyNodes()) {
            if (!locNodeId.equals(n.id()))
                pendingDeltas.put(n.id(), new DeltaAverage(avgSize));
        }
    }

    /**
     * @param ver Snapshot version.
     * @param deltas Deltas map.
     */
    public GridClockDeltaSnapshot(GridClockDeltaVersion ver, Map<UUID, Long> deltas) {
        this.ver = ver;
        this.deltas = deltas;

        pendingDeltas = Collections.emptyMap();
    }

    /**
     * @return Version.
     */
    public GridClockDeltaVersion version() {
        return ver;
    }

    /**
     * @return Map of collected deltas.
     */
    public Map<UUID, Long> deltas() {
        return Collections.unmodifiableMap(deltas);
    }

    /**
     * Awaits either until snapshot is ready or timeout elapses.
     *
     * @param timeout Timeout to wait.
     * @throws IgniteInterruptedCheckedException If wait was interrupted.
     */
    public synchronized void awaitReady(long timeout) throws IgniteInterruptedCheckedException {
        long start = System.currentTimeMillis();

        try {
            while (!ready()) {
                long now = System.currentTimeMillis();

                if (start + timeout - now <= 0)
                    return;

                wait(start + timeout - now);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Callback invoked when time delta is received from remote node.
     *
     * @param nodeId Node ID.
     * @param timeDelta Calculated time delta.
     * @return {@code True} if more samples needed from that node.
     */
    public synchronized boolean onDeltaReceived(UUID nodeId, long timeDelta) {
        DeltaAverage avg = pendingDeltas.get(nodeId);

        if (avg != null) {
            avg.onValue(timeDelta);

            if (avg.ready()) {
                pendingDeltas.remove(nodeId);

                deltas.put(nodeId, avg.average());

                if (ready())
                    notifyAll();

                return false;
            }

            return true;
        }

        return false;
    }

    /**
     * Callback invoked when node left.
     *
     * @param nodeId Left node ID.
     */
    public synchronized void onNodeLeft(UUID nodeId) {
        pendingDeltas.remove(nodeId);

        deltas.put(nodeId, 0L);

        if (ready())
            notifyAll();
    }

    /**
     * @return {@code True} if snapshot is ready.
     */
    public synchronized boolean ready() {
        return pendingDeltas.isEmpty();
    }

    /**
     * @return Collection of node IDs for which response was not received so far.
     */
    public synchronized Collection<UUID> pendingNodeIds() {
        // Must return copy.
        return new HashSet<>(pendingDeltas.keySet());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClockDeltaSnapshot.class, this);
    }

    /**
     * Delta average.
     */
    private static class DeltaAverage {
        /** Delta values. */
        private long[] vals;

        /** Current index. */
        private int idx;

        /**
         * @param size Accumulator size.
         */
        private DeltaAverage(int size) {
            vals = new long[size];
        }

        /**
         * Adds value to accumulator.
         *
         * @param val Value to add.
         */
        public void onValue(long val) {
            if (idx < vals.length)
                vals[idx++] = val;
        }

        /**
         * Whether this average is complete.
         *
         * @return {@code True} if enough values is collected.
         */
        public boolean ready() {
            return idx == vals.length;
        }

        /**
         * @return Average delta.
         */
        public long average() {
            long sum = 0;

            for (long val : vals)
                sum += val;

            return sum / vals.length;
        }
    }
}