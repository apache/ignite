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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.NotNull;
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

    private Map<Integer, Map<Integer, Long>> reinitialized;

    /** */
    private Map<Integer, Set<PartCounters>> remainingHist;

    /** {@code True} if the initial demand request has been sent. */
    private AtomicBoolean initReq = new AtomicBoolean();

    /** */
    private final ClusterNode node;

    /** */
    private final FileRebalanceFuture mainFut;

    /** Cache group rebalance order. */
    private final int rebalanceOrder;

    /** Node snapshot name. */
    private volatile IgniteInternalFuture<Boolean> snapFut;

    /** */
//    public IgniteInternalFuture<Boolean> snapshotFuture() {
//        return snapFut;
//    }

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
        remainingHist = new ConcurrentHashMap<>(assigns.size());
        reinitialized = new ConcurrentHashMap<>(assigns.size());

        for (Map.Entry<Integer, Set<Integer>> entry : assigns.entrySet()) {
            Set<Integer> parts = entry.getValue();
            int grpId = entry.getKey();

            assert !remaining.containsKey(grpId);

            remaining.put(grpId, new GridConcurrentHashSet<>(entry.getValue()));
            reinitialized.put(grpId, new ConcurrentHashMap<>());
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

    /**
     * @param grpId Cache group id to search.
     * @param partId Cache partition to remove;
     */
    public void onPartitionRestored(int grpId, int partId, long min, long max) {
        Set<Integer> parts = remaining.get(grpId);

        assert parts != null : "Unexpected group identifier: " + grpId;

        remainingHist.computeIfAbsent(grpId, v -> new ConcurrentSkipListSet<>())
            .add(new PartCounters(partId, min, max));

        if (log.isDebugEnabled()) {
            log.debug("Partition done [grp=" + cctx.cache().cacheGroup(grpId).cacheOrGroupName() +
                ", p=" + partId + ", remaining=" + parts.size() + "]");
        }

        boolean rmvd = parts.remove(partId);

        assert rmvd : "Partition not found: " + partId;

        if (parts.isEmpty())
            onGroupRestored(grpId);
    }

    private void onGroupRestored(int grpId) {
        Set<Integer> parts = remaining.remove(grpId);

        if (parts == null)
            return;

        Set<PartCounters> histParts = remainingHist.remove(grpId);

        assert histParts.size() == assigns.get(grpId).size() : "expect=" + assigns.get(grpId).size() + ", actual=" + histParts.size();

        CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

        GridDhtPartitionDemandMessage msg = new GridDhtPartitionDemandMessage(rebalanceId, topVer, grpId);

        for (PartCounters desc : histParts) {
            assert desc.toCntr >= desc.fromCntr : "from=" + desc.fromCntr + ", to=" + desc.toCntr;

            if (desc.fromCntr != desc.toCntr) {
                if (log.isDebugEnabled()) {
                    log.debug("Prepare to request historical rebalancing [cache=" + grp.cacheOrGroupName() + ", p=" +
                        desc.partId + ", from=" + desc.fromCntr + ", to=" + desc.toCntr + "]");
                }

                // todo histParts.size incorrect
                msg.partitions().addHistorical(desc.partId, desc.fromCntr, desc.toCntr, histParts.size());

                continue;
            }

            log.debug("Skipping historical rebalancing [p=" +
                desc.partId + ", from=" + desc.fromCntr + ", to=" + desc.toCntr + "]");

            // No historical rebalancing required  -can own partition.
            if (grp.localWalEnabled()) {
                boolean owned = grp.topology().own(grp.topology().localPartition(desc.partId));

                assert owned : "part=" + desc.partId + ", grp=" + grp.cacheOrGroupName();
            }
        }

        mainFut.onCacheGroupDone(grpId, nodeId(), msg);

        if (remaining.isEmpty() && !isDone())
            onDone(true);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
        if (isDone())
            return false;

        boolean r = super.onDone(res, err, cancel);

        try {
            if (log.isDebugEnabled())
                log.debug("Stopping file rebalance routine: " + cctx.localNodeId() + " -> " + nodeId());

            if (snapFut != null && !snapFut.isDone()) {
                if (log.isInfoEnabled())
                    log.info("Cancelling snapshot creation: " + nodeId());

                snapFut.cancel();
            }
            else if (snapFut != null && log.isDebugEnabled())
                log.debug("snapFut already done: " + nodeId());
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to finish file rebalancing node routine", e);
        }

        mainFut.onNodeDone(this, res, err, cancel);

        return r;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        for (Map.Entry<Integer, Set<Integer>> entry : new HashMap<>(remaining).entrySet()) {
            buf.append("grp=").append(cctx.cache().cacheGroup(entry.getKey()).cacheOrGroupName()).
                append(" parts=").append(entry.getValue()).append("; ");
        }

        return "finished=" + isDone() + ", node=" + node.id() + ", remain=[" + buf + "]";
    }

    private long reinitPartition(int grpId, int partId, File src) throws IgniteCheckedException {
        FilePageStore pageStore = ((FilePageStore)((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId));

        try {
            File dest = new File(pageStore.getFileAbsolutePath());

            if (log.isDebugEnabled()) {
                log.debug("Moving downloaded partition file [from=" + src +
                    " , to=" + dest + " , size=" + src.length() + "]");
            }

            assert !cctx.pageStore().exists(grpId, partId) : "Partition file exists [cache=" +
                cctx.cache().cacheGroup(grpId).cacheOrGroupName() + ", p=" + partId + "]";

            // todo change to "move" when all issues with page memory will be resolved.
            Files.copy(src.toPath(), dest.toPath());
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to move file [source=" + src +
                ", target=" + pageStore.getFileAbsolutePath() + "]", e);
        }

        GridDhtLocalPartition part = cctx.cache().cacheGroup(grpId).topology().localPartition(partId);

        // todo seems we don't need to store this value - just use initial update counter for historical rebalance (lwm in future).
        return part.dataStore().store(false).reinit();
    }

    public void onPartitionSnapshotReceived(File file, int grpId, int partId) throws IgniteCheckedException {
        long initialCntr = reinitPartition(grpId, partId, file);

        // todo check lwm counter, just use counter
        Map<Integer, Long> parts = reinitialized.get(grpId);

        parts.put(partId, initialCntr);

        if (parts.size() == remaining.get(grpId).size())
            cctx.filePreloader()
                .switchPartitions(grpId, parts, this)
                .listen(f -> {
                    try {
                        Map<Integer, T2<Long, Long>> cntrs = f.get();

                        assert cntrs != null;

                        cctx.kernalContext().closure().runLocalSafe(() -> {
                            for (Map.Entry<Integer, T2<Long, Long>> entry : cntrs.entrySet())
                                onPartitionRestored(grpId, entry.getKey(), entry.getValue().get1(), entry.getValue().get2());
                        });
                    }
                    catch (IgniteCheckedException e) {
                        log.error("Unable to restore partition snapshot [cache=" +
                            cctx.cache().cacheGroup(grpId) + ", p=" + partId, e);

                        onDone(e);
                    }
                });
    }

    private static class PartCounters implements Comparable {
        /** Partition id. */
        final int partId;

        /** From counter. */
        final long fromCntr;

        /** To counter. */
        final long toCntr;

        public PartCounters(int partId, long fromCntr, long toCntr) {
            this.partId = partId;
            this.fromCntr = fromCntr;
            this.toCntr = toCntr;
        }

        @Override public int compareTo(@NotNull Object o) {
            PartCounters otherDesc = (PartCounters)o;

            if (partId > otherDesc.partId)
                return 1;

            if (partId < otherDesc.partId)
                return -1;

            return 0;
        }
    }
}

