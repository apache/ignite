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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.ConsistentCutFinishResponse;
import org.apache.ignite.internal.processors.cache.ConsistentCutStartRequest;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
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
import org.jetbrains.annotations.Nullable;

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
     * Collection of transactions in COMMITTING / COMMTTED state.
     */
    private final Set<IgniteInternalTx> committingTxs = ConcurrentHashMap.newKeySet();

    /**
     * Set of transactions to exclude from the {@link #latestCutState} and include them into next Consistent Cut.
     */
    private final Set<GridCacheVersion> includeNext = ConcurrentHashMap.newKeySet();

    /**
     * Guards {@link #latestCutState}. When one thread updates the state, other threads handle messages that can change it.
     */
    private final ReentrantReadWriteLock cutGuard = new ReentrantReadWriteLock();

    /**
     * Collection of nodes that haven't completed local Consistent Cut procedure. Ignite coordinator await them
     * to schedule new global Consistent Cut.
     */
    private volatile Set<UUID> notCompletedNodes;

    /**
     * Schedules next global Consistent Cut procedure.
     */
    private volatile GridTimeoutObject cutTimer;

    /** Whether coordinator disabled to schedule Consistent Cuts. */
    private volatile boolean disabled;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        latestCutState = new InitialConsistentCutState();

        cctx.kernalContext().io().addMessageListener(TOPIC_CONSISTENT_CUT, (nodeId, msg, plc) ->
            cctx.kernalContext().pools().consistentCutExecutorService().execute(() -> {
                if (msg instanceof ConsistentCutStartRequest)
                    handleConsistentCutVersion(nodeId, ((ConsistentCutStartRequest)msg).version());

                else if (msg instanceof ConsistentCutFinishResponse) {
                    if (U.isLocalNodeCoordinator(cctx.discovery()))
                        handleConsistentCutFinishResponse(nodeId, (ConsistentCutFinishResponse)msg);
                }
            })
        );

        cctx.kernalContext().discovery().setCustomEventListener(ChangeGlobalStateFinishMessage.class, (top, snd, msg) -> {
            if (U.isLocalNodeCoordinator(cctx.discovery()) && msg.state().active()) {
                if (cutTimer != null || disabled)
                    return;

                long cutPeriod = CU.getPitrPointsPeriod(cctx.gridConfig());

                scheduleConsistentCut(cutPeriod, cutPeriod);
            }
        });

        cctx.kernalContext().event().addLocalEventListener((e) -> {
            if (notCompletedNodes != null)
                notCompletedNodes.remove(e.node().id());

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

                if (notCompletedNodes != null && !notCompletedNodes.isEmpty()) {
                    log.warning("Skip Consistent Cut procedure. Some nodes hasn't finished yet previous one. " +
                        "Consistent Cut may require more time that it configured. Consider to increase param " +
                        "`DataStorageConfiguration#setPointInTimeRecoveryPeriod`. " +
                        "Nodes that hasn't finished their job: " + notCompletedNodes);

                    scheduleConsistentCut(delay, period);

                    return;
                }

                long cutVer = triggerConsistentCutOnCluster();

                long d = (cutVer + period) - System.currentTimeMillis();

                if (d < 0) {
                    long run = -d + delay;

                    log.warning("Consistent Cut may require more time that it configured: " + run + " ms." +
                        " Consider to increase param `DataStorageConfiguration#setPointInTimeRecoveryPeriod`.");

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
        committingTxs.add(tx);
    }

    /**
     * Unregister committed transactions.
     */
    public void unregisterAfterCommit(IgniteInternalTx tx) {
        committingTxs.remove(tx);

        // Lock for case: tx is PREPARED, CUT is concurrently processing while it's receiving FinishRequest.
        cutGuard.readLock().lock();

        try {
            ConsistentCutState s = latestCutState;

            if (s.finished() && includeNext.isEmpty())
                return;

            GridCacheVersion txVer = tx.nearXidVersion();

            if (log.isDebugEnabled())
                log.debug("`unregisterAfterCommit` " + txVer.asIgniteUuid() + " " + s);

            includeNext.remove(txVer);

            // In some cases transaction was included int the check-list after it's notified with txCutVer.
            // Then it's required to check such transactions twice: on commit, on receive notification.
            tryFinish(s, txVer);
        }
        finally {
            cutGuard.readLock().unlock();
        }
    }

    /**
     * @return Latest Consistent Cut Version.
     */
    public long latestCutVersion() {
        return latestCutState.version();
    }

    /**
     * Checks local CutVersion and start Consistent Cut if version has changed.
     *
     * @param cutVer Received CutVersion from different node.
     */
    public void handleConsistentCutVersion(UUID crdNodeId, long cutVer) {
        // Already handled this version.
        if (latestCutVersion() >= cutVer)
            return;

        // Try handle new version.
        if (cutGuard.writeLock().tryLock()) {
            try {
                consistentCut(crdNodeId, cutVer);

                tryFinish(latestCutState, null);
            }
            finally {
                cutGuard.writeLock().unlock();
            }
        }
        // Some other thread already has handled it. Just wait it for finishing.
        else {
            cutGuard.readLock().lock();

            cutGuard.readLock().unlock();

            assert crdNodeId.equals(latestCutState.crdNodeId()) : crdNodeId + " " + latestCutState;
        }
    }

    /**
     * Triggers global Consistent Cut procedure.
     */
    public long triggerConsistentCutOnCluster() {
        long cutVer = System.currentTimeMillis();

        log.info("Start Consistent Cut, version = " + cutVer);

        AffinityTopologyVersion topVer = cctx.kernalContext().discovery().topologyVersionEx();

        Collection<ClusterNode> nodes = cctx.kernalContext().discovery().serverNodes(topVer);

        assert F.isEmpty(notCompletedNodes);

        notCompletedNodes = nodes.stream().map(ClusterNode::id).collect(Collectors.toSet());

        Message msg = new ConsistentCutStartRequest(cutVer);

        for (ClusterNode n: nodes) {
            try {
                cctx.kernalContext().io().sendToGridTopic(n, TOPIC_CONSISTENT_CUT, msg, SYSTEM_POOL);

            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send Consistent Cut message to remote node.", e);

                notCompletedNodes.remove(n.id());
            }
        }

        return cutVer;
    }

    /**
     * Coordinator handles finish responses from remote nodes.
     */
    public void handleConsistentCutFinishResponse(UUID nodeId, ConsistentCutFinishResponse msg) {
        long ver = latestCutVersion();

        long rcvVer = msg.version();

        if (rcvVer != ver) {
            log.error("Unexpected Consistent Cut version " + rcvVer + " from remote node " + nodeId + ". Expect " + ver);

            return;
        }

        if (!notCompletedNodes.remove(nodeId)) {
            log.error("Unexpected message from node " + nodeId + ". It finished Consistent Cut with version " + rcvVer
                + ". Latest local version " + ver);

            return;
        }

        if (msg.error())
            log.error("Consistent Cut " + rcvVer + " failed on remote node " + nodeId);
    }

    /**
     * Handles notifications from remote node with the latest Consistent Cut Version that doesn't include specified
     * transaction. Local node verifies the check-list of transactions that are waiting for the notification, and make
     * a decision whether to include the transaction to the latest Consistent Cut State.
     *
     * @param txVer Transaction ID on near node.
     * @param rmtTxCutVer Consistent Cut Version after which the specified transaction was committed on remote node.
     */
    public void handleRemoteTxCutVersion(GridCacheVersion txVer, long rmtTxCutVer) {
        ConsistentCutState cutState = latestCutState;

        if (log.isDebugEnabled())
            log.debug("`handleRemoteTxCutVersion` " + txVer.asIgniteUuid() + " " + cutState);

        Long locTxCutVer = cutState.needCheck(txVer);

        if (locTxCutVer != null) {
            if (rmtTxCutVer == cutState.version() && rmtTxCutVer > locTxCutVer) {
                cutState.exclude(txVer);

                includeNext.add(txVer);
            }

            tryFinish(cutState, txVer);
        }
    }

    /**
     * Finds the latest Consistent Cut Version that doesn't include specified transaction.
     *
     * @param tx Transaction.
     * @return the latest Consistent Cut Version that doesn't include specified transaction.
     */
    public long txCutVersion(IgniteTxAdapter tx) {
        // Need lock here to avoid concurrent threads - that prepare FinishRequest and making ConsistentCut.
        cutGuard.readLock().lock();

        try {
            ConsistentCutState cutState = latestCutState;

            GridCacheVersion txVer = tx.nearXidVersion();

            if (includeNext.contains(txVer))
                return cutState.version();

            if (cutState.includes(txVer))
                return cutState.prevVersion();

            Long v = cutState.txCutVersion(txVer);

            return v != null ? v : cutState.version();
        }
        finally {
            cutGuard.readLock().unlock();
        }
    }

    /**
     * Stop starting new Consistent Cut procedures on Ignite coordinator.
     */
    public void disable() {
        disabled = true;
    }

    /**
     * Performs the Consistent Cut procedure: updates local Consistent Cut Version, prepares local Consistent Cut State.
     *
     * @param crdNodeId Consistent Cut coordinator node ID.
     * @param cutVer Consistent Cut Version.
     */
    private void consistentCut(UUID crdNodeId, long cutVer) {
        long prevCutVer = latestCutVersion();

        // Check for duplicated Consistent Cut.
        if (prevCutVer >= cutVer)
            return;

        ConsistentCutState cutState = new ConsistentCutState(crdNodeId, cutVer, prevCutVer);

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
                    cutState.include(txVer);

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
                        cutState.txCutVersion(txVer, cutVer);

                        includeNext.add(txVer);
                    }
                    // Transaction prepared on all participated nodes. Every node can track events order.
                    //
                    // Back -------Ped---|------
                    //            / \   CUT
                    //           /   \     CUT
                    // Prim ----Ping--Ped---|---
                    else if (txState == PREPARED) {
                        cutState.txCutVersion(txVer, prevCutVer);

                        cutState.include(txVer);
                    }
                }
                // Primary or Backup nodes need to check PREPARED transactions and wait for FinishRequest.
                //
                // Back ------Ped---|-----------
                //            / \  CUT   /
                //           /   \      /
                // Prim ---Ping--Ped-----Cing---
                else if (txState == PREPARED)
                    cutState.addForCheck(txVer, prevCutVer);
            }
            // One phase commit. For 1PC it is used a reverse order for the notifications (backup -> prim -> near).
            else {
                // Near node is waiting for notification from primary.
                if (tx.near() && (txState == PREPARING || txState == PREPARED))
                    cutState.addForCheck(txVer, prevCutVer);

                // Primary node is waiting for notification from backup.
                else if (tx.dht() && tx.local() && (txState == PREPARING || txState == PREPARED))
                    cutState.addForCheck(txVer, prevCutVer);

                // Include all transactions on backup.
                //
                // Back ------Ped---|---Ced-------
                //            /    CUT      \
                //           /               \
                // Prim ---Ping----------------
                else if (tx.dht() && !tx.local() && (txState == PREPARING || txState == PREPARED)) {
                    cutState.include(txVer);

                    cutState.txCutVersion(txVer, prevCutVer);
                }

                // Include all transactions that are committing.
                else if (txState == COMMITTING || txState == COMMITTED)
                    cutState.include(txVer);
            }
        }

        // For cases when node has multiple participations: near and primary or backup.
        cutState.tryFinish();

        // Log Cut before publishing cut state (due to concurrancy with `handleRcvdTxFinishRequest`).
        walLog(cutState, cutState.buildStartRecord());

        if (log.isDebugEnabled())
            log.debug("Prepare Consistent Cut State: " + cutState);

        latestCutState = cutState;
    }

    /**
     * Tries to finish local Consistent Cut procedure.
     *
     * @param cutState Local Consistent Cut state.
     * @param txVer Optional ID of transaction to check.
     */
    private void tryFinish(ConsistentCutState cutState, @Nullable GridCacheVersion txVer) {
        if (txVer != null && cutState.tryFinish(txVer))
            walLog(cutState, cutState.buildFinishRecord());

        if (cutState.finished() && includeNext.isEmpty())
            sendFinish(cutState, false);
    }

    /**
     * Logs ConsistentCutRecord to WAL.
     */
    private void walLog(ConsistentCutState cutState, WALRecord record) {
        try {
            if (cctx.wal() != null)
                cctx.wal().log(record);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to write to WAL local Consistent Cut record.", e);

            sendFinish(cutState, true);

            throw new IgniteException(e);
        }
    }

    /**
     * Sends finish message to Consistent Cut coordinator node.
     */
    private void sendFinish(ConsistentCutState cutState, boolean err) {
        try {
            if (cctx.kernalContext().clientNode())
                return;

            Message msg = new ConsistentCutFinishResponse(cutState.version(), err);

            cctx.kernalContext().io().sendToGridTopic(cutState.crdNodeId(), TOPIC_CONSISTENT_CUT, msg, SYSTEM_POOL);
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
            super(null, 0, 0);

            finish();
        }
    }
}
