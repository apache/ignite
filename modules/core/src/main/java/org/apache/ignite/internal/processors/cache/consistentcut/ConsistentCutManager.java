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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_CONSISTENT_CUT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Manages all stuff related to Consistent Cut.
 *
 * Consistent Cut splits timeline on 2 global areas - BEFORE and AFTER. It guarantees that every transaction committed BEFORE
 * also will be committed BEFORE on every other node. It means that an Ignite node can safely recover itself to this
 * point without any coordination with other nodes.
 *
 * The algorithm starts on Ignite coordinator node by timer. Period of starting Consistent Cut is defined in
 * {@link DataStorageConfiguration#setPitrPeriod(long)}. Other nodes notifies with direct message from coordinator
 * {@link ConsistentCutStartRequest} or by transaction messages {@link ConsistentCutVersionAware}. After node finishes
 * Consistent Cut locally it becomes ready for new Consistent Cut, and notifies coordinator with {@link ConsistentCutFinishResponse}.
 *
 * Coordinator node guarntees that {@link ConsistentCutVersion} is growing monotonously.
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
 * 8. After Consistent Cut finished and all transactions from BEFORE side committed, it notifies coordinator with
 *    {@link ConsistentCutFinishResponse} that local node is ready for next Consistent Cut process.
 */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter {
    /**
     * It is used for CAS updates of {@link #cutState}.
     */
    private static final AtomicReferenceFieldUpdater<ConsistentCutManager, ConsistentCutState> CONSITENT_CUT_STATE =
        AtomicReferenceFieldUpdater.newUpdater(ConsistentCutManager.class, ConsistentCutState.class, "cutState");

    /**
     * Immutable snapshot of Consistent Cut state. It atomically updates of {@link ConsistentCutVersion} and {@link ConsistentCut}.
     *
     * a) To guarantee happens-before between versions are received and sent after by the same node.
     * b) To guarantee that every transaction committed after the version update isn't cleaned from {@link #committingTxs}
     *    and then is checked by {@link ConsistentCut}.
     */
    private volatile ConsistentCutState cutState;

    /**
     * Colleciton of committing transactions. Track them due to {@link IgniteTxManager#activeTransactions()} doesn't
     * contain information about transactions in COMMITTING / COMMITTED state.
     */
    private final Set<IgniteInternalTx> committingTxs = ConcurrentHashMap.newKeySet();

    /**
     * Node ID of Consistent Cut coordinator.
     */
    private UUID crdNodeId;

    /**
     * Collection of server nodes that aren't ready to run new Consistent Cut procedure. {@code null} on non-coordinator nodes.
     */
    private @Nullable Set<UUID> notFinishedSrvNodes;

    /**
     * Schedules next global Consistent Cut procedure. {@code null} on non-coordinator nodes, or if Consistent Cut scheduling
     * is disabled.
     */
    private volatile @Nullable GridTimeoutObject cutTimer;

    /**
     * If disabled then Ignite coordinator stops scheduling new Consistent Cuts.
     */
    private volatile boolean disableScheduling;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        CONSITENT_CUT_STATE.set(this, new ConsistentCutState(new ConsistentCutVersion(0)));

        cctx.kernalContext().io().addMessageListener(TOPIC_CONSISTENT_CUT, (nodeId, msg, plc) -> {
            if (msg instanceof ConsistentCutStartRequest)
                handleConsistentCutVersion(nodeId, ((ConsistentCutStartRequest)msg).version());
            else if (msg instanceof ConsistentCutFinishResponse)
                handleConsistentCutFinishResponse(nodeId, (ConsistentCutFinishResponse)msg);
        });

        cctx.kernalContext().discovery().setCustomEventListener(ChangeGlobalStateFinishMessage.class, (top, snd, msg) -> {
            if (!U.isLocalNodeCoordinator(cctx.discovery()))
                return;

            if (msg.state().active()) {
                if (disableScheduling) {
                    log.warning("Consistent Cut had been disabled previously and it wasn't activated. To start" +
                        " Consistent Cut again it's required to explicitly enable it.");

                    return;
                }

                if (cutTimer == null)
                    scheduleConsistentCut();
            }
            else
                disableScheduling = true;
        });

        cctx.kernalContext().event().addLocalEventListener((e) -> {
            if (notFinishedSrvNodes != null) {
                if (notFinishedSrvNodes.remove(e.node().id())) {
                    log.error("Node participated in Consistent Cut failed, stops scheduling new Consistent Cuts.");

                    disableScheduling = true;
                }
            }

        }, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0(boolean active) {
        if (U.isLocalNodeCoordinator(cctx.discovery())) {
            if (notFinishedSrvNodes == null)
                notFinishedSrvNodes = ConcurrentHashMap.newKeySet();
        }

        if (crdNodeId == null)
            crdNodeId = U.oldest(cctx.discovery().aliveServerNodes(), null).id();
    }

    /**
     * Schedules global Consistent Cut procedure on Ignite coordinator.
     */
    private void scheduleConsistentCut() {
        long period = CU.getPitrPointsPeriod(cctx.gridConfig());

        cutTimer = new GridTimeoutObjectAdapter(period) {
            /** {@inheritDoc} */
            @Override public void onTimeout() {
                if (disableScheduling)
                    return;

                if (notFinishedSrvNodes != null && notFinishedSrvNodes.isEmpty())
                    triggerConsistentCutOnCluster("onTimeout");
                else {
                    log.warning("Skip Consistent Cut procedure. Some nodes hasn't finished yet previous one. " +
                        "Latest version: " + latestKnownCutVersion() +
                        "\n\tConsistent Cut may require more time that it configured." +
                        "\n\tConsider to increase param `DataStorageConfiguration#setPointInTimeRecoveryPeriod`. " +
                        "\n\tNodes that hasn't finished their job: " + notFinishedSrvNodes);
                }

                scheduleConsistentCut();
            }
        };

        cctx.time().addTimeoutObject(cutTimer);
    }

    /**
     * Registers transaction before commit it, sets Consistent Cut Version if needed (for non-near nodes).
     *
     * It invokes before committing transactions leave {@link IgniteTxManager#activeTransactions()}.
     */
    public void registerBeforeCommit(IgniteInternalTx tx) {
        setTxCutVersionIfNeeded(tx);

        committingTxs.add(tx);

        if (log.isDebugEnabled()) {
            log.debug("`registerBeforeCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + " , ver=" + ((ConsistentCutVersionAware)tx).txCutVersion() + ", cutVer = " + latestKnownCutVersion());
        }
    }

    /**
     * Unregisters committed transaction. It invokes after specified transaction committed and wrote related {@link TxRecord} to WAL.
     *
     * @param tx Transaction.
     */
    public void unregisterAfterCommit(IgniteInternalTx tx) {
        if (CONSITENT_CUT_STATE.get(this).cut() == null)
            committingTxs.remove(tx);
    }

    /**
     * Handles received Consistent Cut Version from remote node. It compares it with the latest version that local node
     * is aware of. Init local Consistent Cut procedure if received version is greater than the local.
     *
     * @param crdNodeId ID of Consistent Cut coordinator node.
     * @param rcvCutVer Received Cut Version from different node.
     */
    public void handleConsistentCutVersion(@Nullable UUID crdNodeId, ConsistentCutVersion rcvCutVer) {
        ConsistentCutState cutState = CONSITENT_CUT_STATE.get(this);
        ConsistentCutVersion cutVer = cutState.version();

        try {
            verifyMessage(cutVer, crdNodeId, rcvCutVer);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Invalid Consistent Cut message.", e);

            onFinish(e);

            return;
        }

        if (rcvCutVer.version() > cutVer.version()) {
            ConsistentCut cut = new ConsistentCut(cctx);

            ConsistentCutState newCutState = new ConsistentCutState(rcvCutVer, cut);

            if (CONSITENT_CUT_STATE.compareAndSet(this, cutState, newCutState)) {
                cctx.kernalContext().pools().getSystemExecutorService().submit(() -> {
                    try {
                        cut.init(rcvCutVer);

                        if (log.isDebugEnabled())
                            log.debug("Prepared Consistent Cut: " + newCutState);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to handle Consistent Cut version.", e);

                        onFinish(e);
                    }
                });
            }
        }
    }

    /**
     * @return Latest known Consistent Cut Version, no matter whether this Consistent Cut has just started or already finished.
     */
    public ConsistentCutVersion latestKnownCutVersion() {
        return CONSITENT_CUT_STATE.get(this).version();
    }

    /**
     * Verifies whether received message looks like as expected. If it's not then skip handling Consistent Cut.
     */
    private void verifyMessage(ConsistentCutVersion cutVer, UUID crdNodeId, ConsistentCutVersion rcvCutVer) throws IgniteCheckedException {
        if (crdNodeId != null && !this.crdNodeId.equals(crdNodeId))
            throw new IgniteCheckedException("Consistent Cut coordinator has changed: prev=" + this.crdNodeId + ", new=" + crdNodeId);

        if (Math.abs(rcvCutVer.version() - cutVer.version()) > 1)
            throw new IgniteCheckedException("Unexpected Consistent Cut version: prev=" + cutVer + ", new=" + rcvCutVer);
    }

    /**
     * @return Reference to mutable collection of committing transactions.
     */
    Collection<IgniteInternalTx> committingTxs() {
        return committingTxs;
    }

    /**
     * Finishes local Consistent Cut: cleans {@link ConsistentCut} reference and sends finish message to Consistent Cut coordinator node.
     */
    void onFinish(Throwable err) {
        try {
            ConsistentCutState cutState = CONSITENT_CUT_STATE.get(this);

            if (cutState.cut() != null) {
                ConsistentCutState finishedCutState = new ConsistentCutState(cutState.version());

                if (CONSITENT_CUT_STATE.compareAndSet(this, cutState, finishedCutState)) {
                    // Clean committed transactions.
                    committingTxs.removeIf(tx -> tx.state() == TransactionState.COMMITTED);

                    if (cctx.kernalContext().clientNode())
                        return;

                    Message msg = new ConsistentCutFinishResponse(cutState.version(), err != null);

                    if (log.isDebugEnabled())
                        log.debug("Send " + msg + " from " + cctx.localNodeId());

                    cctx.kernalContext().io().sendToGridTopic(crdNodeId, TOPIC_CONSISTENT_CUT, msg, SYSTEM_POOL);
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send Consistent Cut Finish message to coordinator node.", e);
        }
    }

    /**
     * Triggers global Consistent Cut procedure.
     *
     * @param reason Reason to trigger new Consistent Cut.
     * @return New Consistent Cut Version.
     */
    ConsistentCutVersion triggerConsistentCutOnCluster(String reason) {
        AffinityTopologyVersion topVer = cctx.kernalContext().discovery().topologyVersionEx();

        assert F.isEmpty(notFinishedSrvNodes);

        Set<UUID> srvNodes = ConcurrentHashMap.newKeySet();

        cctx.kernalContext().discovery().serverNodes(topVer).forEach((n) -> srvNodes.add(n.id()));

        notFinishedSrvNodes.addAll(srvNodes);

        ConsistentCutVersion prevCutVer = CONSITENT_CUT_STATE.get(this).version();

        ConsistentCutVersion cutVer = new ConsistentCutVersion(prevCutVer.version() + 1);

        if (log.isDebugEnabled())
            log.debug("Start Consistent Cut, version = " + cutVer + ", reason='" + reason + "'");

        Message msg = new ConsistentCutStartRequest(cutVer);

        // Send message to all nodes, incl. client nodes.
        for (ClusterNode n: cctx.kernalContext().discovery().allNodes()) {
            try {
                cctx.kernalContext().io().sendToGridTopic(n, TOPIC_CONSISTENT_CUT, msg, SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send Consistent Cut message to remote node.", e);

                notFinishedSrvNodes.remove(n.id());
            }
        }

        return cutVer;
    }

    /**
     * Coordinator handles finish responses from remote nodes.
     */
    void handleConsistentCutFinishResponse(UUID nodeId, ConsistentCutFinishResponse msg) {
        if (log.isDebugEnabled())
            log.debug("Received ConsistentCutReadyResponse from node " + nodeId + ": " + msg + " . Wait " + notFinishedSrvNodes);

        if (!U.isLocalNodeCoordinator(cctx.discovery())) {
            log.error("Received unexpected message from node " + nodeId + ": " + msg);

            return;
        }

        if (!notFinishedSrvNodes.remove(nodeId)) {
            log.error("Received duplicated message from node " + nodeId + ":" + msg);

            return;
        }

        if (msg.error()) {
            log.error("Consistent Cut " + msg.version() + " failed on remote node " + nodeId + ". Stop scheduling new Consistent Cut.");

            disableScheduling = true;
        }
    }

    /**
     * Disables starting new Consistent Cut procedures on Ignite coordinator.
     */
    public void disable() {
        assert U.isLocalNodeCoordinator(cctx.discovery());

        disableScheduling = true;
    }

    /**
     * Enables starting new Consistent Cut procedures on Ignite coordinator.
     */
    public void enable() {
        assert U.isLocalNodeCoordinator(cctx.discovery());

        disableScheduling = false;

        scheduleConsistentCut();
    }

    /**
     * @return {@code true} if it's safe to run new Consistent Cut procedure.
     */
    boolean latestGlobalCutReady() {
        return F.isEmpty(notFinishedSrvNodes);
    }

    /**
     * Sets Consistent Cut version for specified transaction if it hasn't set yet. Single node is responsible for
     * setting the version within transaction (see {@link #isSetterTxCutVersion(IgniteInternalTx)}). Other nodes
     * recieves this version due to Finish messages.
     */
    private void setTxCutVersionIfNeeded(IgniteInternalTx tx) {
        ConsistentCutVersionAware txCutVerAware = (ConsistentCutVersionAware)tx;

        if (isSetterTxCutVersion(tx)) {
            assert txCutVerAware.txCutVersion() == null : tx;

            txCutVerAware.txCutVersion(latestKnownCutVersion());
        }

        assert txCutVerAware.txCutVersion() != null : tx;
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

    /** For test purposes. */
    protected ConsistentCut consistentCut() {
        return new ConsistentCut(cctx);
    }
}
