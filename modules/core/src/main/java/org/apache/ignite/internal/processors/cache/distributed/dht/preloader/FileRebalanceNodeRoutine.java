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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/** */
public class FileRebalanceNodeRoutine extends GridFutureAdapter<Boolean> {
    /** Context. */
    protected GridCacheSharedContext cctx;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private long rebalanceId;

    /** */
    @GridToStringInclude
    private Map<Integer, Set<Integer>> assigns;

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private Map<Integer, Set<Integer>> remaining;

    /** */
    private Map<Integer, AtomicInteger> received;

    /** */
    private final ClusterNode node;

    /** */
    private final FileRebalanceFuture mainFut;

    /** Cache group rebalance order. */
    private final int rebalanceOrder;

    /** Node snapshot future. */
    private volatile IgniteInternalFuture<Boolean> snapFut;

    /**
     * Default constructor for the dummy future.
     */
    public FileRebalanceNodeRoutine() {
        this(null, null, null, null, 0, 0, Collections.emptyMap(), null);

        onDone();
    }

    /**
     * @param node Supplier node.
     * @param rebalanceId Rebalance id.
     * @param assigns Map of assignments to request from remote.
     * @param topVer Topology version.
     */
    public FileRebalanceNodeRoutine(
        GridCacheSharedContext cctx,
        FileRebalanceFuture mainFut,
        IgniteLogger log,
        ClusterNode node,
        int rebalanceOrder,
        long rebalanceId,
        Map<Integer, Set<Integer>> assigns,
        AffinityTopologyVersion topVer
    ) {
        this.cctx = cctx;
        this.mainFut = mainFut;
        this.log = log;
        this.node = node;
        this.rebalanceOrder = rebalanceOrder;
        this.rebalanceId = rebalanceId;
        this.assigns = assigns;
        this.topVer = topVer;

        remaining = new ConcurrentHashMap<>(assigns.size());
        received = new ConcurrentHashMap<>(assigns.size());

        for (Map.Entry<Integer, Set<Integer>> entry : assigns.entrySet()) {
            Set<Integer> parts = entry.getValue();
            int grpId = entry.getKey();

            assert !remaining.containsKey(grpId);

            remaining.put(grpId, new GridConcurrentHashSet<>(entry.getValue()));
            received.put(grpId, new AtomicInteger());
        }
    }

    /**
     * @return Rebalancing order.
     */
    public int order() {
        return rebalanceOrder;
    }

    /**
     * @return Supplier node ID.
     */
    public UUID nodeId() {
        return node.id();
    }

    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        return onDone(false, null, true);
    }

    private void onCacheGroupDone(int grpId, Map<Integer, Long> maxCntrs) {
        Set<Integer> parts = remaining.remove(grpId);

        if (parts == null)
            return;

        assert maxCntrs.size() == assigns.get(grpId).size() : "expect=" + assigns.get(grpId).size() + ", actual=" + maxCntrs.size();

        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        assert !grp.localWalEnabled() : "grp=" + grp.cacheOrGroupName();

        GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(rebalanceId, topVer, grpId);

        // For historical rebalancing partitions should be ordered.
        Map<Integer, T2<Long, Long>> histParts = new TreeMap<>();

        for (Map.Entry<Integer, Long> e : maxCntrs.entrySet()) {
            int partId = e.getKey();

            long initCntr = grp.topology().localPartition(partId).initialUpdateCounter();
            long maxCntr = e.getValue();

            assert maxCntr >= initCntr : "from=" + initCntr + ", to=" + maxCntr;

            if (initCntr != maxCntr) {
                histParts.put(partId, new T2<>(initCntr, maxCntr));

                continue;
            }

            if (log.isDebugEnabled())
                log.debug("No need for WAL rebalance [grp=" + grp.cacheOrGroupName() + ", p=" + partId + "]");
        }

        for (Map.Entry<Integer, T2<Long, Long>> e : histParts.entrySet())
            msg.partitions().addHistorical(e.getKey(), e.getValue().get1(), e.getValue().get2(), histParts.size());

        mainFut.onCacheGroupDone(grpId, nodeId(), msg);

        if (remaining.isEmpty() && !isDone())
            onDone(true);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
        if (isDone())
            return false;

        try {
            if (log.isDebugEnabled())
                log.debug("Stopping file rebalance routine [local=" + cctx.localNodeId() + ", remote=" + nodeId() + "]");

            if (snapFut != null && !snapFut.isDone()) {
                if (log.isDebugEnabled())
                    log.debug("Cancelling snapshot creation [remote=" + nodeId() + "]");

                snapFut.cancel();
            }
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to finish file rebalancing node routine", e);
        }

        if (super.onDone(res, err, cancel)) {
            mainFut.onNodeDone(this, res, err, cancel);

            return true;
        }

        return false;
    }

    /**
     * Request a remote snapshot of partitions.
     */
    public void requestPartitions() {
        try {
            snapFut = cctx.snapshotMgr().createRemoteSnapshot(node.id(), assigns);

            if (log.isInfoEnabled())
                log.info("Start partitions preloading [from=" + node.id() + ", snapshot=" + snapFut + ", fut=" + this + ']');
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to create remote snapshot [from=" + node.id() + ", assigns=" + assigns + "]", e);

            onDone(e);
        }
    }

    public void onPartitionSnapshotReceived(int grpId, int partId) {
        AtomicInteger receivedCntr = received.get(grpId);

        int receivedCnt = receivedCntr.incrementAndGet();

        Set<Integer> parts = remaining.get(grpId);

        if (receivedCnt != parts.size())
            return;

        cctx.filePreloader().switchPartitions(grpId, parts, this)
            .listen(fut -> {
                try {
                    Map<Integer, Long> cntrs = fut.get();

                    assert cntrs != null;

                    cctx.kernalContext().closure().runLocalSafe(() -> onCacheGroupDone(grpId, cntrs));
                }
                catch (IgniteCheckedException e) {
                    log.error("Unable to restore partition snapshot [cache=" +
                        cctx.cache().cacheGroup(grpId) + ", p=" + partId + "]", e);

                    onDone(e);
                }
            });
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        for (Map.Entry<Integer, Set<Integer>> entry : new HashMap<>(remaining).entrySet()) {
            buf.append("grp=").append(cctx.cache().cacheGroup(entry.getKey()).cacheOrGroupName()).
                append(" parts=").append(entry.getValue()).append("; received=").
                append(received.get(entry.getKey()).get()).append("; ");
        }

        return "finished=" + isDone() + ", failed=" + isFailed() + ", cancelled=" + isCancelled() + ", node=" +
            node.id() + ", remain=[" + buf + "]";
    }
}
