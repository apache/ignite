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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.ConsistentCutReadyResponse;
import org.apache.ignite.internal.processors.cache.ConsistentCutStartRequest;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxRemoteAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_CONSISTENT_CUT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;

/**
 * Manages all stuff related to Consistent Cut.
 */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter {
    /**
     * Mutable local state of the latest observable Consistent Cut.
     */
    private volatile ConsistentCutState latestCutState;

    /**
     * Version of the latest observable Consistent Cut.
     */
    private final AtomicLong latestCutVer = new AtomicLong();

    /**
     * Collection of transactions in COMMITTING / COMMTTED state.
     */
    private final Set<IgniteInternalTx> committingTxs = ConcurrentHashMap.newKeySet();

    /**
     * Collection of transactions in COMMITTING / COMMTTED state.
     */
    private final Map<GridCacheVersion, Long> excludedTxs = new ConcurrentHashMap<>();

    /**
     * Collection of server nodes that aren't ready to run new Consistent Cut procedure.
     */
    private volatile Set<UUID> notReadySrvNodes;

    /**
     * Schedules next global Consistent Cut procedure.
     */
    private volatile GridTimeoutObject cutTimer;

    /** Whether coordinator disabled to schedule Consistent Cuts. */
    private volatile boolean disabled;

    /** Node ID of Consistent Cut coordinator. */
    private volatile UUID crdNodeId;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        latestCutState = new InitialConsistentCutState();

        cctx.kernalContext().io().addMessageListener(TOPIC_CONSISTENT_CUT, (nodeId, msg, plc) -> {
            if (msg instanceof ConsistentCutStartRequest)
                handleConsistentCutVersion(nodeId, ((ConsistentCutStartRequest)msg).version());
            else if (msg instanceof ConsistentCutReadyResponse) {
                log.info("Receive ConsistentCutReadyResponse before: " + msg + ", node " + nodeId);

                assert U.isLocalNodeCoordinator(cctx.discovery());

                handleConsistentCutFinishResponse(nodeId, (ConsistentCutReadyResponse)msg);
            }
        });

        cctx.kernalContext().discovery().setCustomEventListener(ChangeGlobalStateFinishMessage.class, (top, snd, msg) -> {
            if (U.isLocalNodeCoordinator(cctx.discovery()) && msg.state().active()) {
                if (cutTimer != null || disabled)
                    return;

                long cutPeriod = CU.getPitrPointsPeriod(cctx.gridConfig());

                scheduleConsistentCut(cutPeriod, cutPeriod);
            }
        });

        cctx.kernalContext().event().addLocalEventListener((e) -> {
            if (notReadySrvNodes != null)
                notReadySrvNodes.remove(e.node().id());

        }, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /**
     * Schedules global Consistent Cut procedure on Ignite coordinator.
     *
     * @param delay Delay to start new Consistent Cut.
     * @param period Configured period to start Consistent Cuts.
     */
    private synchronized void scheduleConsistentCut(long delay, long period) {
        cutTimer = new GridTimeoutObjectAdapter(delay) {
            /** {@inheritDoc} */
            @Override public void onTimeout() {
                if (disabled)
                    return;

                if (!F.isEmpty(notReadySrvNodes)) {
                    log.warning("Skip Consistent Cut procedure. Some nodes hasn't finished yet previous one. " +
                        "Last 2 versions: " + latestCutState().prevVersion() + ", " + latestCutState().version() +
                        "\n\tConsistent Cut may require more time that it configured." +
                        "\n\tConsider to increase param `DataStorageConfiguration#setPointInTimeRecoveryPeriod`. " +
                        "\n\tNodes that hasn't finished their job: " + notReadySrvNodes);

                    scheduleConsistentCut(delay, period);

                    return;
                }

                long cutVer = triggerConsistentCutOnCluster();

                long d = (cutVer + period) - System.currentTimeMillis();

                if (d < 0) {
                    log.warning("Consistent Cut may require more time that it configured: " + (-d + delay) + " ms." +
                        "\n\tConsider to increase param `DataStorageConfiguration#setPointInTimeRecoveryPeriod`.");

                    d = period;
                }

                scheduleConsistentCut(d, period);
            }
        };

        cctx.time().addTimeoutObject(cutTimer);
    }

    /**
     * Register committing transactions in internal collection to track them in Consistent Cut algorithm.
     */
    public void registerBeforeCommit(IgniteInternalTx tx) {
        long cutVer = -1L;

        if (tx.onePhaseCommit() && tx.dht() && !tx.local()) {
            GridDistributedTxRemoteAdapter txAdapter = (GridDistributedTxRemoteAdapter)tx;

            cutVer = txCutVersion((IgniteTxAdapter)tx);

            txAdapter.txCutVer(cutVer);
        }

        if (!tx.onePhaseCommit() && tx.near()) {
            GridNearTxLocal txAdapter = (GridNearTxLocal)tx;

            cutVer = txCutVersion((IgniteTxAdapter)tx);

            txAdapter.txCutVer(cutVer);
        }

        if (log.isInfoEnabled())
            log.info("`registerBeforeCommit` " + tx.nearXidVersion().asIgniteUuid() + " , ver " + cutVer);

        committingTxs.add(tx);
    }

    /**
     * Unregister committed transactions.
     */
    public void unregisterAfterCommit(IgniteInternalTx tx) {
        if (!committingTxs.remove(tx))
            return;

        ConsistentCutState cutState = latestCutState;

        if (cutState.ready())
            return;

        GridCacheVersion txVer = tx.nearXidVersion();

        if (log.isInfoEnabled())
            log.info("`unregisterAfterCommit` " + txVer.asIgniteUuid() + ", state " + cutState);

        // In some cases transaction was included into the check-list after it's notified with txCutVer.
        // Then it's required to check such transactions twice: on commit, on receive notification.
        if (!cutState.finished() && (tx.onePhaseCommit() && tx.local()) || (!tx.onePhaseCommit() && tx.dht())) {
            GridCacheVersion chkTxVer = tx.nearXidVersion();

            if (excludedTxs.remove(chkTxVer) != null)
                cutState.exclude(chkTxVer);

            tryFinish(cutState, txVer);
        }

        cutState.onCommit(txVer);

        checkReady(cutState);

        // Re-check transactions in the check-list. They may be committed concurrently with Consistent Cut.
        if (!cutState.finished()) {
            for (IgniteInternalTx t: cutState.checkList()) {
                if (t.state() == COMMITTED) {
                    GridCacheVersion chkTxVer = t.nearXidVersion();

                    if (excludedTxs.get(chkTxVer) == cutState.version() && excludedTxs.remove(chkTxVer) != null)
                        cutState.exclude(chkTxVer);

                    tryFinish(cutState, chkTxVer);
                }
            }
        }

        // Re-check transactions in the after list. They may be committed concurrently with Consistent Cut.
        if (cutState.finished() && !cutState.ready()) {
            for (IgniteInternalTx t: cutState.afterList()) {
                if (t.state() == COMMITTED)
                    cutState.onCommit(t.nearXidVersion());
            }

            checkReady(cutState);
        }
    }

    /**
     * @return Latest Consistent Cut Version.
     */
    public long latestCutVersion() {
        return latestCutVer.get();
    }

    /**
     * @return Latest Consistent Cut state.
     */
    public ConsistentCutState latestCutState() {
        return latestCutState;
    }

    /**
     * @return {@code true} if it's safe to run new Consistent Cut procedure.
     */
    public boolean latestGlobalCutReady() {
        return F.isEmpty(notReadySrvNodes);
    }

    /**
     * Checks local CutVersion and start Consistent Cut if version has changed.
     *
     * @param cutVer Received CutVersion from different node.
     */
    public void handleConsistentCutVersion(UUID crdNodeId, long cutVer) {
        if (this.crdNodeId == null)
            this.crdNodeId = crdNodeId;
        else
            assert this.crdNodeId.equals(crdNodeId);

        long locCutVer = latestCutVer.get();

        // Already handled this version.
        if (locCutVer >= cutVer)
            return;

        if (cutVer > locCutVer) {
            if (latestCutVer.compareAndSet(locCutVer, cutVer)) {
                // Log Cut before publishing cut state (due to concurrancy with `handleRcvdTxFinishRequest`).
                walLog(cutVer, new ConsistentCutStartRecord(cutVer));

                ConsistentCutState state = latestCutState = consistentCut(locCutVer, cutVer);

                if (log.isInfoEnabled())
                    log.info("Prepare Consistent Cut State: " + state);

                if (state.finished())
                    walLog(cutVer, state.buildFinishRecord());

                checkReady(state);
            }
        }
    }

    /**
     * Triggers global Consistent Cut procedure.
     */
    public long triggerConsistentCutOnCluster() {
        long cutVer = System.currentTimeMillis();

        if (log.isInfoEnabled())
            log.info("Start Consistent Cut, version = " + cutVer);

        AffinityTopologyVersion topVer = cctx.kernalContext().discovery().topologyVersionEx();

        assert F.isEmpty(notReadySrvNodes);

        Set<UUID> srvNodes = ConcurrentHashMap.newKeySet();

        cctx.kernalContext().discovery().serverNodes(topVer).forEach((n) -> srvNodes.add(n.id()));

        notReadySrvNodes = srvNodes;

        Message msg = new ConsistentCutStartRequest(cutVer);

        // Send message to all nodes, incl. client nodes.
        for (ClusterNode n: cctx.kernalContext().discovery().allNodes()) {
            try {
                cctx.kernalContext().io().sendToGridTopic(n, TOPIC_CONSISTENT_CUT, msg, SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send Consistent Cut message to remote node.", e);

                notReadySrvNodes.remove(n.id());
            }
        }

        return cutVer;
    }

    /**
     * Coordinator handles finish responses from remote nodes.
     */
    public void handleConsistentCutFinishResponse(UUID nodeId, ConsistentCutReadyResponse msg) {
        if (log.isInfoEnabled())
            log.info("Receive ConsistentCutReadyResponse from node " + nodeId + ": " + msg + " . Wait " + notReadySrvNodes);

        if (!notReadySrvNodes.remove(nodeId)) {
            log.error("Unexpected message from node " + nodeId + ":" + msg);

            return;
        }

        if (msg.error())
            log.error("Consistent Cut " + msg.version() + " failed on remote node " + nodeId);
    }

    /**
     * Handles notifications from remote nodes. Local node verifies the check-list of transactions that are waiting for
     * the notification, and make a decision whether to include the transaction to the latest Consistent Cut State.
     *
     * @param txVer Transaction ID on near node.
     * @param rmtTxCutVer Consistent Cut Version after which the specified transaction was committed on remote node.
     */
    public void handleRemoteTxCutVersion(GridCacheVersion txVer, long rmtTxCutVer) {
        ConsistentCutState cutState = latestCutState;

        if (log.isInfoEnabled())
            log.info("`handleRemoteTxCutVersion` " + txVer.asIgniteUuid() + ". Rmt=" + rmtTxCutVer + ", state " + cutState);

        // Finish request is received concurrently with Consistent Cut procedure. Handle it on `unregisterAfterCommit`.
        if (cutState.version() < rmtTxCutVer) {
            excludedTxs.put(txVer, rmtTxCutVer);

            return;
        }

        Long locTxCutVer = cutState.needCheck(txVer);

        if (locTxCutVer != null) {
            if (rmtTxCutVer == cutState.version() && rmtTxCutVer > locTxCutVer)
                cutState.exclude(txVer);

            tryFinish(cutState, txVer);

            checkReady(cutState);
        }
    }

    /**
     * Finds the latest Consistent Cut Version AFTER which the specified transaction committed.
     *
     * @param tx Transaction.
     * @return the latest Consistent Cut Version AFTER which the specified transaction committed.
     */
    private long txCutVersion(IgniteTxAdapter tx) {
        ConsistentCutState cutState = latestCutState;

        if (log.isInfoEnabled())
            log.info("`txCutVersion` " + tx.nearXidVersion().asIgniteUuid() + " " + cutState);

        GridCacheVersion txVer = tx.nearXidVersion();

        if (cutState.afterCut(txVer))
            return cutState.version();

        if (cutState.beforeCut(txVer))
            return cutState.prevVersion();

        Long v = cutState.txCutVersion(txVer);

        return v != null ? v : cutState.version();
    }

    /**
     * Disables starting new Consistent Cut procedures on Ignite coordinator.
     */
    public void disable() {
        assert U.isLocalNodeCoordinator(cctx.discovery());

        disabled = true;
    }

    /**
     * Enables starting new Consistent Cut procedures on Ignite coordinator.
     */
    public void enable() {
        assert U.isLocalNodeCoordinator(cctx.discovery());

        disabled = false;

        long cutPeriod = CU.getPitrPointsPeriod(cctx.gridConfig());

        scheduleConsistentCut(cutPeriod, cutPeriod);
    }

    /**
     * Performs the Consistent Cut procedure: updates local Consistent Cut Version, prepares local Consistent Cut State.
     *
     * @param prevCutVer Previous Consistent Cut version.
     * @param cutVer Consistent Cut version.
     */
    protected ConsistentCutState consistentCut(long prevCutVer, long cutVer) {
        ConsistentCutState cutState = new ConsistentCutState(cutVer, prevCutVer);

        // Committing transactions aren't part of active transactions.
        Collection<IgniteInternalTx> txs = F.concat(true, cctx.tm().activeTransactions(), committingTxs);

        for (IgniteInternalTx tx : txs) {
            TransactionState txState = tx.state();
            GridCacheVersion txVer = tx.nearXidVersion();

            // Skip fast finish transactions (no write entries).
            if (tx.near() && ((GridNearTxLocal)tx).fastFinish())
                continue;

            if (!tx.onePhaseCommit()) {
                // Include to new Consistent Cut all transactions that are committing.
                //
                // Back ---Ced----------------
                //             \
                //              \    CUT
                // Prim --Ped---Cing--|--Ced--
                if (txState == COMMITTING || txState == COMMITTED) {
                    cutState.includeBeforeCut(txVer);

                    continue;
                }

                if (tx.near()) {
                    // Prepare request may not achieve primary or backup nodes to the moment of local Consistent Cut.
                    // This case is inconsistent, then exclude such transactions from new Consistent Cut.
                    //
                    // Back --|----Ped----------
                    //       CUT  /
                    //           /       CUT
                    // Prim ----Ping------|-----
                    if (txState == PREPARING) {
                        cutState.txCutVersion(txVer, cutState.version());

                        cutState.includeAfterCut(tx);
                    }
                    // Transaction prepared on all participated nodes. Every node can track events order.
                    //
                    // Back -------Ped---|------
                    //            / \   CUT
                    //           /   \     CUT
                    // Prim ----Ping--Ped---|---
                    else if (txState == PREPARED) {
                        cutState.txCutVersion(txVer, prevCutVer);

                        cutState.includeBeforeCut(txVer);
                    }
                }
                // Primary or Backup nodes need to check PREPARED transactions and wait for FinishRequest.
                //
                // Back ------Ped---|-----------
                //            / \  CUT   /
                //           /   \      /
                // Prim ---Ping--Ped-----Cing---
                else if (txState == PREPARING || txState == PREPARED)
                    cutState.addForCheck(tx, prevCutVer);
            }
            // One phase commit. For 1PC it is used a reverse order for the notifications (backup -> prim -> near).
            else {
                // Near node is waiting for notification from primary.
                if (tx.near() && (txState == PREPARING || txState == PREPARED))
                    cutState.addForCheck(tx, prevCutVer);

                // Primary node is waiting for notification from backup.
                else if (tx.dht() && tx.local() && (txState == PREPARING || txState == PREPARED))
                    cutState.addForCheck(tx, prevCutVer);

                // Include all transactions on backup.
                //
                // Back ------Ped---|---Ced-------
                //            /    CUT      \
                //           /               \
                // Prim ---Ping----------------
                else if (tx.dht() && !tx.local() && (/*txState == PREPARING || */txState == PREPARED)) {
                    cutState.includeBeforeCut(txVer);

                    cutState.txCutVersion(txVer, prevCutVer);
                }

                // Include all transactions that are committing.
                else if (txState == COMMITTING || txState == COMMITTED)
                    cutState.includeBeforeCut(txVer);
            }
        }

        // For cases when node has multiple participations: near and primary or backup.
        cutState.tryFinish();

        return cutState;
    }

    /**
     * Tries to finish local Consistent Cut procedure.
     *
     * @param cutState Local Consistent Cut state.
     * @param txVer Transaction ID to check.
     */
    private void tryFinish(ConsistentCutState cutState, GridCacheVersion txVer) {
        if (cutState.tryFinish(txVer))
            walLog(cutState.version(), cutState.buildFinishRecord());
    }

    /**
     * Checks whether local node is ready for new Consistent Cut and send notification if it is ready.
     *
     * @param cutState Local Consistent Cut state.
     */
    private void checkReady(ConsistentCutState cutState) {
        if (cutState.checkReady())
            sendFinish(cutState.version(), false);
    }

    /**
     * Logs ConsistentCutRecord to WAL.
     */
    private void walLog(long cutVer, WALRecord record) {
        try {
            if (cctx.wal() != null)
                cctx.wal().log(record);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to write to WAL local Consistent Cut record.", e);

            sendFinish(cutVer, true);

            throw new IgniteException(e);
        }
    }

    /**
     * Sends finish message to Consistent Cut coordinator node.
     */
    private void sendFinish(long cutVer, boolean err) {
        try {
            if (cctx.kernalContext().clientNode())
                return;

            Message msg = new ConsistentCutReadyResponse(cutVer, err);

            if (log.isInfoEnabled())
                log.info("Send ConsistentCutReadyResponse from " + cctx.localNodeId() + ": " + msg);

            cctx.kernalContext().io().sendToGridTopic(crdNodeId, TOPIC_CONSISTENT_CUT, msg, SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send Consistent Cut Finish message to coordinator node.", e);
        }
    }

    /**
     * Consistent Cut state that initialized on Ignite node start.
     */
    private static class InitialConsistentCutState extends ConsistentCutState {
        /** */
        private InitialConsistentCutState() {
            super(0, 0);

            finish();
            checkReady();
        }
    }
}
