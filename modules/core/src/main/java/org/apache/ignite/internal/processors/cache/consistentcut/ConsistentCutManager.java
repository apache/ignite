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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedBaseMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CONSISTENT_CUT_CREATE;

/**
 * Processes all stuff related to Consistent Cut.
 * <p>
 * Consistent Cut splits timeline on 2 global areas - BEFORE and AFTER. It guarantees that every transaction committed BEFORE
 * also will be committed BEFORE on every other node. It means that an Ignite node can safely recover itself to this
 * point without any coordination with other nodes.
 * <p>
 * The algorithm starts on Ignite coordinator node by user command. Other nodes notifies with discovery message
 * {@link ConsistentCutStartRequest} or by transaction messages with marker {@link ConsistentCutMarkerMessage}. {@link ConsistentCut}
 * can be finished with {@code true} for consistent cut, and {@code false} for inconsistent cut. If at least single instance
 * is inconsistent then whole process is inconsistent.
 * <p>
 * Ignit guarantees that {@link ConsistentCutMarker} is growing monotonously.
 * <p>
 * The algorithm consist of steps:
 * 1. On receiving new {@link ConsistentCutMarker} it immediately creates new {@link ConsistentCut} before it processed
 *    a message (that holds marker) itself.
 * 2. It writes {@link ConsistentCutStartRecord} to limit amount of transactions on the AFTER side of Consistent Cut.
 *    After writing this record it's safe to miss transactions on the AFTER side.
 * 3. It collects transactions with PREPARING+ state to check which side of Consistent Cut they belong to. This collection
 *    contains all transactions on the BEFORE side. It's guaranteed with:
 *        a) For 2PC case (and for 1PC near/primary nodes) there are 2 transaction messages (PrepareResponse, FinishRequest)
 *           to transfer transaction from {@link TransactionState#ACTIVE} to {@link TransactionState#COMMITTED}.
 *           In the point 1. it's guaranteed HB between versions in sent PrepareMessage and received FinishMessage.
 *           Then in such case transaction will never be on the BEFORE side.
 *        b) For 1PC case on backup node this node always choose the greatest version to send on other nodes. And then
 *           it will always be on the AFTER side.
 * 4. It awaits every transaction in this collection to be committed to decide which side of Consistent Cut they belong to.
 * 5. Every transaction is signed with the latest {@link ConsistentCutMarker} AFTER which it committed. This version is defined
 *    at single node within a transaction - {@link #isSetterTxCutVersion(IgniteInternalTx)}.
 * 6. After the check-list is empty it finishes Consistent Cut with writing {@link ConsistentCutFinishRecord} that contains
 *    collection of transactions on the BEFORE and AFTER sides.
 * 7. After Consistent Cut finished and all transactions from BEFORE side committed, it notifies coordinator that local node
 *    is ready for next Consistent Cut process.
 */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter implements PartitionsExchangeAware {
    /** It serves updates of {@link #cut} with CAS. */
    protected static final AtomicReferenceFieldUpdater<ConsistentCutManager, ConsistentCut> CONSISTENT_CUT =
        AtomicReferenceFieldUpdater.newUpdater(ConsistentCutManager.class, ConsistentCut.class, "cut");

    /** Running consistent cut, {@code null} if not running. */
    private volatile @Nullable ConsistentCut cut;

    /** Future that completes after cut has finished. */
    protected volatile ClusterConsistentCutFuture clusterCutFut;

    /** Distributed process for performing distributed Consistent Cut algorithm. */
    private DistributedProcess<ConsistentCutStartRequest, Boolean> consistentCutProc;

    /** The last handled {@link ConsistentCutMarker}. */
    private volatile ConsistentCutMarker lastSeenMarker;

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        super.start0();

        cctx.exchange().registerExchangeAwareComponent(this);

        consistentCutProc = new DistributedProcess<>(
            cctx.kernalContext(),
            CONSISTENT_CUT_CREATE,
            this::startLocalCut,
            this::finishLocalCut
        );
    }

    /** {@inheritDoc} */
    @Override public void stop0(boolean cancel) {
        cancelCut(new IgniteCheckedException("Ignite node is stopping."));
    }

    /**
     * Stops Consistent Cut in case of baseline topology changed.
     */
    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        if (fut.changedBaseline() || fut.isBaselineNodeFailed() || fut.firstEvent().type() == EventType.EVT_NODE_JOINED)
            cancelCut(new IgniteCheckedException("Ignite topology changed, can't finish Consistent Cut."));
    }

    /**
     * Mark transaction with {@link ConsistentCutMarker} for specified transaction if it hadn't been marked yet.
     * Single node is responsible for marking a transaction (see {@link #isSetterTxCutVersion(IgniteInternalTx)}). Other nodes
     * recieves this mark with Finish messages.
     * <p>
     * Note, there are some cases when marker equals to {@code null} even after this method:
     * 1. Transaction committed with transaction recovery mechanism.
     * 2. Cut wasn't run while transaction is committing.
     *
     * @param tx Transaction.
     */
    public void registerBeforeCommit(IgniteInternalTx tx) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut != null) {
            // Mark all new committing  transactions to be on the AFTER side.
            if (isSetterTxCutVersion(tx))
                tx.marker(cut.marker());

            cut.addCommittingTransaction(tx.finishFuture());
        }

        if (log.isInfoEnabled()) {
            log.info("`registerBeforeCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + " , txMarker=" + tx.marker() + ", cutMarker = " + (cut == null ? null : cut.marker()));
        }
    }

    /** Mark transaction messages with {@link ConsistentCutMarker} if needed. */
    public GridCacheMessage wrapTxMsgIfCutRunning(GridDistributedBaseMessage txMsg, @Nullable ConsistentCutMarker txMarker) {
        GridCacheMessage msg = txMsg;

        ConsistentCutMarker marker = runningCut();

        if (marker != null) {
            msg = txMarker == null ?
                new ConsistentCutMarkerMessage(txMsg, marker)
                : new ConsistentCutMarkerTxFinishMessage(txMsg, marker, txMarker);
        }

        return msg;
    }

    /**
     * Handles received {@link ConsistentCutMarker} from remote node. It compares it with the latest marker that local node
     * is aware of. Init local {@link ConsistentCut} procedure if received version is greater than the local.
     *
     * @param marker Received Cut marker from different node.
     */
    public void handleConsistentCutMarker(ConsistentCutMarker marker) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut == null && !marker.equals(lastSeenMarker) && cctx.discovery().topologyVersionEx().compareTo(marker.topVer()) == 0) {
            ConsistentCut newCut = newConsistentCut(marker);

            if (CONSISTENT_CUT.compareAndSet(this, cut, newCut)) {
                lastSeenMarker = marker;

                cctx.kernalContext().pools().getSystemExecutorService().submit(() -> {
                    try {
                        newCut.init(marker);

                        if (log.isInfoEnabled())
                            log.info("Prepared Consistent Cut: " + newCut);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to handle Consistent Cut version.", e);

                        newCut.onDone(e);
                    }
                });
            }
        }
    }

    /** Creates new Consistent Cut instance. */
    protected ConsistentCut newConsistentCut(ConsistentCutMarker marker) {
        return new ConsistentCut(cctx, marker);
    }

    /**
     * Stops local Consistent Cut on error.
     *
     * @param err Error.
     */
    private void cancelCut(Exception err) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut != null && !cut.isDone())
            cut.onDone(err);
    }

    /**
     * Triggers global Consistent Cut procedure.
     *
     * @return New Consistent Cut Version.
     */
    public synchronized IgniteInternalFuture<Boolean> triggerConsistentCutOnCluster() {
        if (clusterCutFut != null || CONSISTENT_CUT.get(this) != null)
            return new GridFinishedFuture<>(new IgniteCheckedException("Previous Consistent Cut is still running."));

        AffinityTopologyVersion topVer = cctx.discovery().topologyVersionEx();

        ConsistentCutMarker marker = new ConsistentCutMarker(System.currentTimeMillis(), topVer);

        ClusterConsistentCutFuture fut = clusterCutFut = new ClusterConsistentCutFuture(topVer);

        consistentCutProc.start(UUID.randomUUID(), new ConsistentCutStartRequest(marker));

        if (log.isInfoEnabled())
            log.info("Start Consistent Cut, marker = " + marker);

        return fut;
    }

    /**
     * @return Latest {@link ConsistentCutMarker} for running cut, or {@code null} if not cut is running.
     */
    private @Nullable ConsistentCutMarker runningCut() {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        return cut == null ? null : cut.marker();
    }

    /**
     * @return Latest seen {@link ConsistentCutMarker}, no matter whether this Consistent Cut has just started or already finished.
     */
    protected @Nullable ConsistentCutMarker lastSeenMarker() {
        return lastSeenMarker;
    }

    /**
     * Starts {@link ConsistentCut} in the discovery thread if not started yet within a transaction thread.
     *
     * @param req ConsistentCut start request.
     * @return Future that completes with {@code true} for consistent cut, {@code false} for inconsistent cut.
     */
    private IgniteInternalFuture<Boolean> startLocalCut(ConsistentCutStartRequest req) {
        if (log.isInfoEnabled())
            log.info("StartLocalCut for " + req.marker() + " " + cctx.localNodeId());

        handleConsistentCutMarker(req.marker());

        return CONSISTENT_CUT.get(this);
    }

    /**
     * @param reqId Consistent Cut request ID.
     * @param results Consistent Cut results from all nodes: {@code true} for consistent cut, {@code false} for inconsistent.
     * @param exceptions Errors raised from all nodes.
     */
    private void finishLocalCut(UUID reqId, Map<UUID, Boolean> results, Map<UUID, Exception> exceptions) {
        // Stops signing messages.
        CONSISTENT_CUT.set(this, null);

        if (clusterCutFut != null) {
            DiscoCache discoCache = cctx.discovery().discoCache(clusterCutFut.topVer);

            // Can't check that all baseline nodes are finished.
            if (discoCache == null) {
                clusterCutFut.onDone(new IgniteCheckedException(
                    String.format("No baseline description found for topology [%s] on which Consistent Cut started." +
                        " Cluster topology is changing too fast. Consider increasing `GNITE_DISCOVERY_HISTORY_SIZE` property.",
                        clusterCutFut.topVer))
                );

                return;
            }

            Set<UUID> baselineTop = discoCache.serverNodes().stream()
                .map(ClusterNode::id)
                .filter(discoCache::baselineNode)
                .collect(Collectors.toSet());

            Optional<Map.Entry<UUID, Exception>> baselineExcp = exceptions.entrySet().stream()
                .filter(e -> baselineTop.contains(e.getKey()))
                .findFirst();

            if (baselineExcp.isPresent()) {
                clusterCutFut.onDone(new IgniteCheckedException(
                    String.format("Baseline node [%s] failed: %s", baselineExcp.get().getKey(), baselineExcp.get().getValue().getMessage())
                ));

                return;
            }

            Map<UUID, Boolean> baselineRes = results.entrySet().stream()
                .filter(e -> baselineTop.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (baselineTop.size() != baselineRes.size()) {
                baselineTop.removeAll(baselineRes.keySet());

                clusterCutFut.onDone(
                    new IgniteCheckedException(String.format("Baseline nodes failed: %s", baselineTop)));

                return;
            }

            clusterCutFut.onDone(
                baselineRes.values().stream().reduce(true, (l, r) -> l && r)
            );

            clusterCutFut = null;
        }
    }

    /**
     * Finds whether local node is responsible for setting Consistent Cut version for specified transaction.
     * - For 2PC transactions the version is inherited in direct order (from originated to primary and backup nodes).
     * - For 1PC transactions the version is inherited in reverse order (from backup to primary).
     *
     * @return Whether local node for the specified transaction sets Consistent Cut Version for whole transaction.
     */
    private boolean isSetterTxCutVersion(IgniteInternalTx tx) {
        if (log.isInfoEnabled()) {
            log.info("`txCutVerSetNode` " + tx.nearXidVersion().asIgniteUuid() + " " + getClass().getSimpleName()
                + " 1pc=" + tx.onePhaseCommit() + " node=" + tx.nodeId() + " nodes=" + tx.transactionNodes() + " " + "client="
                + cctx.kernalContext().clientNode() + " near=" + tx.near() + " local=" + tx.local() + " dht=" + tx.dht());
        }

        if (tx.onePhaseCommit()) {
            if (tx.near() && cctx.kernalContext().clientNode())
                return false;

            Collection<UUID> backups = tx.transactionNodes().get(tx.nodeId());

            // We are on backup node. It's by default set the version.
            if (tx.dht() && backups == null)
                return true;

            // Near can set version iff it's colocated and there is no backups.
            if (tx.near())
                return F.isEmpty(backups) && ((GridNearTxLocal)tx).colocatedLocallyMapped();

            // This is a backup or primary node. Primary node sets the version iff cache doesn't have backups.
            return (tx.dht() && !tx.local()) || backups.isEmpty();
        }
        else
            return tx.near();
    }

    /**
     * Future that completes after Consistent Cut finished on all nodes. It is set only on a node that started new
     * iteration of Consistent Cut.
     */
    private static class ClusterConsistentCutFuture extends GridFutureAdapter<Boolean> {
        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private ClusterConsistentCutFuture(AffinityTopologyVersion ver) {
            topVer = ver;
        }
    }
}
