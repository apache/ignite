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
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
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
 * 5. Every transaction is signed with the latest {@link ConsistentCutMarker} AFTER which it committed. This marker is defined
 *    at single node within a transaction before it starts committing {@link #registerBeforeCommit(IgniteInternalTx)}.
 * 6. After the check-list is empty it finishes Consistent Cut with writing {@link ConsistentCutFinishRecord} that contains
 *    collection of transactions on the BEFORE and AFTER sides.
 * 7. After Consistent Cut finished and all transactions from BEFORE side committed, it notifies coordinator that local node
 *    is ready for next Consistent Cut process.
 */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter implements PartitionsExchangeAware {
    /** It serves updates of {@link #cut} with CAS. */
    protected static final AtomicReferenceFieldUpdater<ConsistentCutManager, ConsistentCut> CONSISTENT_CUT =
        AtomicReferenceFieldUpdater.newUpdater(ConsistentCutManager.class, ConsistentCut.class, "cut");

    /** Running {@link ConsistentCut}, {@code null} if not running. */
    private volatile @Nullable ConsistentCut cut;

    /** Future that completes after Consistent Cut finished on every node in a cluster. */
    protected volatile ClusterConsistentCutFuture clusterCutFut;

    /** Distributed process for performing distributed Consistent Cut algorithm. */
    private DistributedProcess<ConsistentCutStartRequest, Boolean> consistentCutProc;

    /** Marker of the last finished Consistent Cut. Required to avoid re-run Consistent Cut with the same marker. */
    protected volatile ConsistentCutMarker lastFinishedCutMarker;

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
     * Registers transaction before it starts committing.
     *
     * @param tx Transaction.
     */
    public void registerBeforeCommit(IgniteInternalTx tx) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut != null) {
            tx.marker(cut.marker());

            cut.addCommittingTransaction(tx.finishFuture());
        }

        if (log.isDebugEnabled()) {
            log.info("`registerFirstCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + " , txMarker=" + tx.marker() + ", cutMarker = " + (cut == null ? null : cut.marker()));
        }
    }

    /**
     * Registers specified committing transaction.
     *
     * @param tx Transaction.
     * @param marker Marker with that transaction was signed in {@link #registerBeforeCommit(IgniteInternalTx)},
     *               or {@code null} if not specified.
     */
    public void registerCommitting(IgniteInternalTx tx, @Nullable ConsistentCutMarker marker) {
        tx.marker(marker);

        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut != null)
            cut.addCommittingTransaction(tx.finishFuture());

        if (log.isDebugEnabled()) {
            log.info("`registerBeforeCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + " , txMarker=" + tx.marker() + ", cutMarker = " + (cut == null ? null : cut.marker()));
        }
    }

    /**
     * Wraps transaction message to marker messages if cut is running.
     *
     * @param txMsg Transaction message to wrap.
     * @param txMarker {@code null} if message isn't a finish message, non-null otherwise.
     */
    public GridCacheMessage wrapTxMsgIfCutRunning(GridDistributedBaseMessage txMsg, @Nullable ConsistentCutMarker txMarker) {
        GridCacheMessage msg = txMsg;

        ConsistentCutMarker marker = runningCutMarker();

        if (marker != null) {
            msg = txMarker == null ?
                new ConsistentCutMarkerMessage(txMsg, marker)
                : new ConsistentCutMarkerTxFinishMessage(txMsg, marker, txMarker);
        }

        return msg;
    }

    /**
     * Handles received marker from remote node. It compares it with the latest marker that local node is aware of.
     * Init local Consistent Cut procedure if received marker is a new one.
     *
     * @param marker Received Cut marker from different node.
     */
    public void handleConsistentCutMarker(ConsistentCutMarker marker) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut == null && !marker.equals(lastFinishedCutMarker) && cctx.discovery().topologyVersionEx().compareTo(marker.topVer()) == 0) {
            ConsistentCut newCut = newConsistentCut(marker);

            if (CONSISTENT_CUT.compareAndSet(this, cut, newCut)) {
                cctx.kernalContext().pools().getSystemExecutorService().submit(() -> {
                    try {
                        newCut.init();

                        if (log.isDebugEnabled())
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

        if (log.isDebugEnabled())
            log.info("Start Consistent Cut, marker = " + marker);

        return fut;
    }

    /**
     * @return Marker of current running Consistent Cut, if cut isn't running then {@code null}.
     */
    private @Nullable ConsistentCutMarker runningCutMarker() {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        return cut == null ? null : cut.marker();
    }

    /**
     * Starts {@link ConsistentCut} in the discovery thread if not started yet within a transaction thread.
     *
     * @param req ConsistentCut start request.
     * @return Future that completes with {@code true} for consistent cut, {@code false} for inconsistent cut.
     */
    private IgniteInternalFuture<Boolean> startLocalCut(ConsistentCutStartRequest req) {
        if (log.isDebugEnabled())
            log.info("`startLocalCut` for " + req.marker() + " " + cctx.localNodeId());

        // Checks whether this ConsistentCutVersion has already started.
        // This check can be reliably performed only on the coordinator node, as after version applied on coordinator
        // it started spreading across cluster by Communication SPI with transaction messages.
        if (U.isLocalNodeCoordinator(cctx.discovery())
            && lastFinishedCutMarker != null
            && req.marker().compareTo(lastFinishedCutMarker) <= 0) {
            return new GridFinishedFuture<>(new IgniteCheckedException(
                String.format("Consistent Cut for marker [%s] already started, last seen marker [%s].",
                    req.marker(), lastFinishedCutMarker)
            ));
        }

        handleConsistentCutMarker(req.marker());

        return CONSISTENT_CUT.get(this);
    }

    /**
     * Finishes local Consistent Cut, stop signing outgoing messages with marker.
     *
     * @param reqId Consistent Cut request ID.
     * @param results Consistent Cut results from all nodes: {@code true} for consistent cut, {@code false} for inconsistent.
     * @param exceptions Errors raised from all nodes.
     */
    private void finishLocalCut(UUID reqId, Map<UUID, Boolean> results, Map<UUID, Exception> exceptions) {
        lastFinishedCutMarker = CONSISTENT_CUT.get(this).marker();

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
