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
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.CONSISTENT_CUT_PROC;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CONSISTENT_CUT_CREATE;

/**
 * Processes all stuff related to Consistent Cut.
 *
 * Consistent Cut splits timeline on 2 global areas - BEFORE and AFTER. It guarantees that every transaction committed BEFORE
 * also will be committed BEFORE on every other node. It means that an Ignite node can safely recover itself to this
 * point without any coordination with other nodes.
 *
 * The algorithm starts on Ignite coordinator node by user command. Other nodes notifies with discovery message
 * {@link ConsistentCutStartRequest} or by transaction messages {@link ConsistentCutVersionAware}. {@link ConsistentCut}
 * can be finished with {@code true} for consistent cut, and {@code false} for inconsistent cut. If at least single instance
 * is inconsistent then whole process is inconsistent.
 *
 * Ignit guarantees that {@link ConsistentCutVersion} is growing monotonously.
 *
 * The algorithm consist of steps:
 * 1. On receiving new {@link ConsistentCutVersion} it immediately updates local version and creates new {@link ConsistentCut}
 *    before it processed a message (that holds new version) itself. It's required to do this two things atomically:
 *        a) To guarantee happens-before between versions are received and sent after by the same node.
 *        b) To guarantee that every transaction committed after the version update isn't cleaned from {@link #committingTxs}
 *           and then is checked by {@link ConsistentCut}.
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
 * 5. Every transaction is signed with the latest Consistent Cut Version AFTER which it committed. This version is defined
 *    at single node within a transaction - {@link #isSetterTxCutVersion(IgniteInternalTx)}}.
 * 6. It's possible to receive transaction FinishMessages concurrently with preparing the check-list. To avoid misses
 *    such transactions all committing transactions are stored in {@link #committingTxs}.
 * 7. After the check-list is empty it finishes Consistent Cut with writing {@link ConsistentCutFinishRecord} that contains
 *    collection of transactions on the BEFORE and AFTER sides.
 * 8. After Consistent Cut finished and all transactions from BEFORE side committed, it notifies coordinator that local node
 *    is ready for next Consistent Cut process.
 */
public class ConsistentCutProcessor extends GridProcessorAdapter implements PartitionsExchangeAware, MetastorageLifecycleListener {
    /**
     * It serves updates of {@link #cut} with CAS.
     */
    protected static final AtomicReferenceFieldUpdater<ConsistentCutProcessor, ConsistentCut> CONSISTENT_CUT =
        AtomicReferenceFieldUpdater.newUpdater(ConsistentCutProcessor.class, ConsistentCut.class, "cut");

    /** */
    private static final String CONSISTENT_CUT_VERSION_META_KEY = "consistent.cut.version";

    /** */
    private GridCacheSharedContext<?, ?> cctx;

    /**
     * Immutable snapshot of Consistent Cut state. It atomically updates of {@link ConsistentCutVersion} and {@link ConsistentCut}.
     *
     * a) To guarantee happens-before between versions are received and sent after by the same node.
     * b) To guarantee that every transaction committed after the version update isn't cleaned from {@link #committingTxs}
     *    and then is checked by {@link ConsistentCut}.
     */
    private volatile ConsistentCut cut;

    /**
     * Collection of committing and committed transactions. Track them because {@link IgniteTxManager#activeTransactions()}
     * doesn't contain information about transactions in COMMITTING / COMMITTED state.
     */
    private final Set<IgniteInternalTx> committingTxs = ConcurrentHashMap.newKeySet();

    /** */
    protected volatile ClusterConsistentCutFuture clusterCutFut;

    /**
     * Distributed process for performing distributed Consistent Cut algorithm.
     */
    private final DistributedProcess<ConsistentCutStartRequest, Boolean> consistentCutProc;

    // TODO: printMemoryStates --> committing Txs.

    /**
     * @param ctx Kernal context.
     */
    public ConsistentCutProcessor(GridKernalContext ctx) {
        super(ctx);

        consistentCutProc = new DistributedProcess<>(
            ctx,
            CONSISTENT_CUT_CREATE,
            this::startLocalCut,
            this::finishLocalCut
        );
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        cctx = ctx.cache().context();

        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
        cctx.exchange().registerExchangeAwareComponent(this);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopCut(new IgniteCheckedException("Ignite node is stopping."));
    }

    /**
     * Reads {@link ConsistentCutVersion} from metastorage before send it for other nodes.
     */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) throws IgniteCheckedException {
        cctx.database().checkpointReadLock();

        try {
            ConsistentCutVersion ver = (ConsistentCutVersion)metastorage.read(CONSISTENT_CUT_VERSION_META_KEY);

            if (ver != null) {
                ConsistentCut cut = new ConsistentCut(cctx, ver);
                cut.onDone();

                CONSISTENT_CUT.set(this, cut);
            }
        }
        finally {
            cctx.database().checkpointReadUnlock();
        }
    }

    /**
     * Stops Consistent Cut in case of baseline topology changed.
     */
    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        if (fut.changedBaseline() || fut.isBaselineNodeFailed()) {
            // Cancel current Consistent Cut, if exists.
            stopCut(new IgniteCheckedException("Ignite topology changed, can't finish Consistent Cut."));
        }
    }

    /** {@inheritDoc} */
    @Override public @Nullable DiscoveryDataExchangeType discoveryDataType() {
        return CONSISTENT_CUT_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        dataBag.addJoiningNodeData(CONSISTENT_CUT_PROC.ordinal(), cutVersion());
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        dataBag.addGridCommonData(CONSISTENT_CUT_PROC.ordinal(), cutVersion());
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        ConsistentCutVersion rcvCutVer = (ConsistentCutVersion)data.joiningNodeData();

        if (rcvCutVer.compareTo(cutVersion()) > 0)
            CONSISTENT_CUT.set(this, newConsistentCut(rcvCutVer));
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        ConsistentCutVersion rcvCutVer = (ConsistentCutVersion)data.commonData();

        if (rcvCutVer.compareTo(cutVersion()) > 0)
            CONSISTENT_CUT.set(this, newConsistentCut(rcvCutVer));
    }

    /**
     * Registers transaction before commit it, sets Consistent Cut Version if needed (for non-near nodes).
     * It invokes before committing transactions leave {@link IgniteTxManager#activeTransactions()}.
     *
     * @param tx Transaction.
     * @return {@code true} if transaction has registered with this call.
     */
    public boolean registerBeforeCommit(IgniteInternalTx tx) {
        setTxCutVersionIfNeeded(tx);

        if (log.isDebugEnabled()) {
            log.debug("`registerBeforeCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + " , txCutVer=" + tx.txCutVersion() + ", cutVer = " + cutVersion());
        }

        return committingTxs.add(tx);
    }

    /**
     * Cancels transaction registration (with {@link #registerBeforeCommit(IgniteInternalTx)}) in case it is being finished
     * concurrently.
     *
     * @param tx Transaction.
     */
    public void cancelRegisterBeforeCommit(IgniteInternalTx tx) {
        if (log.isDebugEnabled())
            log.debug("`cancelRegisterBeforeCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid());

        committingTxs.remove(tx);
    }

    /**
     * Unregisters committed transaction. It invokes after specified transaction committed and wrote related {@link TxRecord} to WAL.
     *
     * @param tx Transaction.
     */
    public void unregisterAfterCommit(IgniteInternalTx tx) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut == null || cut.isDone())
            committingTxs.remove(tx);
    }

    /**
     * Handles received Consistent Cut Version from remote node. It compares it with the latest version that local node
     * is aware of. Init local Consistent Cut procedure if received version is greater than the local.
     *
     * @param rcvCutVer Received Cut Version from different node.
     */
    public void handleConsistentCutVersion(ConsistentCutVersion rcvCutVer) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        ConsistentCutVersion cutVer = cut == null ? ConsistentCutVersion.NONE : cut.version();

        if (rcvCutVer.compareTo(cutVer) > 0) {
            ConsistentCut newCut = newConsistentCut(rcvCutVer);

            if (CONSISTENT_CUT.compareAndSet(this, cut, newCut)) {
                ctx.pools().getSystemExecutorService().submit(() -> {
                    try {
                        writeVerToMetastorage(rcvCutVer);

                        newCut.listen((f) -> committingTxs.removeIf(tx -> tx.finishFuture().isDone()));

                        newCut.init();

                        if (log.isDebugEnabled())
                            log.debug("Prepared Consistent Cut: " + newCut);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to handle Consistent Cut version.", e);

                        newCut.onDone(e);
                    }
                });
            }
        }
    }

    /** */
    private void writeVerToMetastorage(ConsistentCutVersion cutVer) throws IgniteCheckedException {
        cctx.database().checkpointReadLock();

        try {
            cctx.database().metaStorage().write(CONSISTENT_CUT_VERSION_META_KEY, cutVer);
        }
        finally {
            cctx.database().checkpointReadUnlock();
        }
    }

    /**
     * @return Latest known Consistent Cut Version, no matter whether this Consistent Cut has just started or already finished.
     */
    public ConsistentCutVersion cutVersion() {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        return cut == null ? ConsistentCutVersion.NONE : cut.version();
    }

    /** Creates new Consistent Cut instance. */
    protected ConsistentCut newConsistentCut(ConsistentCutVersion cutVer) {
        return new ConsistentCut(ctx.cache().context(), cutVer);
    }

    /**
     * @return Iterator over committing transactions.
     */
    Iterator<IgniteInternalTx> committingTxs() {
        return committingTxs.iterator();
    }

    /**
     * Stops local Consistent Cut on error.
     *
     * @param err Error.
     */
    private void stopCut(Exception err) {
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
        if (clusterCutFut != null)
            return new GridFinishedFuture<>(new IgniteCheckedException("Previous Consistent Cut is still running."));

        ConsistentCut cut = CONSISTENT_CUT.get(this);

        ConsistentCutVersion prevCutVer = cut == null ? ConsistentCutVersion.NONE : cut.version();

        ConsistentCutVersion cutVer = new ConsistentCutVersion(prevCutVer.version() + 1);

        ClusterConsistentCutFuture fut = clusterCutFut = new ClusterConsistentCutFuture(ctx.discovery().topologyVersionEx());

        consistentCutProc.start(UUID.randomUUID(), new ConsistentCutStartRequest(cutVer));

        if (log.isInfoEnabled())
            log.info("Start Consistent Cut, version = " + cutVer);

        return fut;
    }

    /**
     * Starts {@link ConsistentCut} if not started yet within a transaction thread.
     *
     * @param req ConsistentCut start request.
     * @return Future that completes with {@code true} for consistent cut, {@code false} for inconsistent cut.
     */
    private IgniteInternalFuture<Boolean> startLocalCut(ConsistentCutStartRequest req) {
        if (ctx.clientNode() || !CU.baselineNode(cctx.localNode(), ctx.state().clusterState()))
            return new GridFinishedFuture<>(true);

        log.info("StartLocalCut for " + req.version() + " " + ctx.localNodeId());

        // Checks whether this ConsistentCutVersion has already started.
        // This check can be reliably performed only on the coordinator node, as after version applied on coordinator
        // it started spreading across cluster by Communication SPI with transaction messages.
        if (U.isLocalNodeCoordinator(ctx.discovery()) && req.version().compareTo(cutVersion()) <= 0) {
            return new GridFinishedFuture<>(new IgniteCheckedException(
                String.format("Consistent Cut for version [%s] already started.", req.version())
            ));
        }

        handleConsistentCutVersion(req.version());

        ConsistentCut cut = CONSISTENT_CUT.get(this);

        assert cut != null;

        return req.version().equals(cut.version())
            ? cut
            : new GridFinishedFuture<>(new IgniteCheckedException(
                String.format("Consistent Cut not found for verion [%s], current version [%s].", req.version(), cut.version())
            ));
    }

    /**
     * @param reqId Consistent Cut request ID.
     * @param results Consistent Cut results from all nodes: {@code true} for consistent cut, {@code false} for inconsistent.
     * @param exceptions Errors raised from all nodes.
     */
    private void finishLocalCut(UUID reqId, Map<UUID, Boolean> results, Map<UUID, Exception> exceptions) {
        if (clusterCutFut != null) {
            DiscoCache discoCache = ctx.discovery().discoCache(clusterCutFut.topVer);

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
                log.info("BASELINE " + baselineTop);

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

                log.info("RESULTS " + results);

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
     * Sets Consistent Cut version for specified transaction if it hasn't set yet. Single node is responsible for
     * setting the version within a transaction (see {@link #isSetterTxCutVersion(IgniteInternalTx)}). Other nodes
     * recieves this version due to Finish messages.
     *
     * Note, that it's still possible txCutVer equals to {@code null} even after this method. In case of
     * transaction recovery txCutVer is chosen between all nodes participated in a transaction.
     */
    private void setTxCutVersionIfNeeded(IgniteInternalTx tx) {
        if (tx.txCutVersion() == null && isSetterTxCutVersion(tx))
            tx.txCutVersion(cutVersion());
    }

    /**
     * Finds whether local node is responsible for setting Consistent Cut version for specified transaction.
     * - For 2PC transactions the version is inherited in direct order (from originated to primary and backup nodes).
     * - For 1PC transactions the version is inherited in reverse order (from backup to primary).
     *
     * @return Whether local node for the specified transaction sets Consistent Cut Version for whole transaction.
     */
    private boolean isSetterTxCutVersion(IgniteInternalTx tx) {
        if (log.isDebugEnabled()) {
            log.debug("`txCutVerSetNode` " + tx.nearXidVersion().asIgniteUuid() + " " + getClass().getSimpleName()
                + " 1pc=" + tx.onePhaseCommit() + " node=" + tx.nodeId() + " nodes=" + tx.transactionNodes() + " " + "client="
                + ctx.clientNode() + " near=" + tx.near() + " local=" + tx.local() + " dht=" + tx.dht());
        }

        if (tx.onePhaseCommit()) {
            if (tx.near() && ctx.clientNode())
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
