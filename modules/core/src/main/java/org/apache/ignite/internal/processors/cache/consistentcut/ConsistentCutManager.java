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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.ConsistentCutReadyResponse;
import org.apache.ignite.internal.processors.cache.ConsistentCutStartRequest;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
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
 *
 * Consistent Cut splits timeline on 2 global areas - BEFORE and AFTER. It guarantees that every transaction committed BEFORE
 * also will be committed BEFORE on every other node. It means that an Ignite node can safely recover itself to this
 * point without any coordination with other nodes.
 *
 * The algorithm consist of steps:
 * 1. On receiving new version it immediately updates {@link #latestKnownCutVer} to the newest one.
 * 2. It writes {@link ConsistentCutStartRecord} before any transaction committed. It guarantees that every transaction
 * committed before this record is part of the BEFORE state.
 * 3. It prepares collection of transactions: to commit BEFORE, to commit AFTER and the check-list to verify. Those collections
 * are stored in {@link ConsistentCutState}, and it publishes the state after preparing.
 * 4. Published state is checked by every transaction after commit. If transaction is in the check-list or after-list,
 * it notifies the state.
 * 5. Prepared state might be incomplete due to receiving tx finish requests concurrently with the state preparing.
 * Then it might be unsafe to finish Consistent Cut before analyzing such transactions. Committing transactions are stored
 * in {@link #committingTxs}. It merges the prepared state with committing transactions. It guarantees that there is no
 * any missed transactions that can affect the state. After the merge it enables finishing Consistent Cut.
 * 6. After the check-list is empty it finishes Consistent Cut with writing {@link ConsistentCutFinishRecord} that contains
 * collection of transactions to include BEFORE.
 * 7. After the after-list is empty it notifies coordinator with {@link ConsistentCutReadyResponse} that local node is
 * ready for next Consistent Cut process.
 *
 * The algorithm starts on Ignite coordinator node by timer. Period of starting Consistent Cut is defined in
 * {@link DataStorageConfiguration#setPitrPeriod(long)}. Other nodes notifies with direct message from coordinator
 * {@link ConsistentCutStartRequest} or by transaction messages {@link ConsistentCutVersionAware}. After node finishes
 * Consistent Cut locally and becomes ready for new Consistent Cut it notifies coordinator with {@link ConsistentCutReadyResponse}.
 */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter {
    /**
     * Mutable local state of the latest published Consistent Cut.
     */
    private volatile ConsistentCutState latestPublishedCutState;

    /**
     * Version of the latest started Consistent Cut. It means that {@link ConsistentCutStartRecord} with this version
     * was written to WAL.
     */
    private final AtomicLong latestStartedCutVer = new AtomicLong();

    /**
     * Version of the latest known Consistent Cut. Can be greater than {@link #latestPublishedCutState} version while the
     * latter hasn't prepared and published, and greater {@link #latestStartedCutVer} while the latter hasn't written to WAL.
     */
    private final AtomicLong latestKnownCutVer = new AtomicLong();

    /**
     * Collections of local transactions IDs to be committed by specific Consistent Cut version.
     */
    private final Map<GridCacheVersion, T2<GridCacheVersion, Long>> committingTxs = new ConcurrentHashMap<>();

    /**
     * Collection of server nodes that aren't ready to run new Consistent Cut procedure.
     */
    private volatile Set<UUID> notReadySrvNodes;

    /**
     * Schedules next global Consistent Cut procedure.
     */
    private volatile GridTimeoutObject cutTimer;

    /**
     * Whether coordinator was disabled to schedule Consistent Cuts.
     */
    private volatile boolean disabled;

    /**
     * Node ID of Consistent Cut coordinator.
     */
    private volatile UUID crdNodeId;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        latestPublishedCutState = new InitialConsistentCutState();

        cctx.kernalContext().io().addMessageListener(TOPIC_CONSISTENT_CUT, (nodeId, msg, plc) -> {
            if (msg instanceof ConsistentCutStartRequest)
                handleConsistentCutVersion(nodeId, ((ConsistentCutStartRequest)msg).version());
            else if (msg instanceof ConsistentCutReadyResponse) {
                if (log.isDebugEnabled())
                    log.debug("Receive ConsistentCutReadyResponse before: " + msg + ", node " + nodeId);

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
    private void scheduleConsistentCut(long delay, long period) {
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
     * Register Consistent Cut version for transaction before commit it, if not register before in
     * {@link #handleRemoteTxCutVersion(GridCacheVersion, GridCacheVersion, long)}.
     */
    public void registerBeforeCommit(IgniteInternalTx tx) {
        // Await ConsistentCutStartRecord has written to WAL.
        while (latestStartedCutVer.get() < latestCutVersion()) {
            T2<GridCacheVersion, Long> v = committingTxs.get(tx.xidVersion());

            // Wait only commits related to new Consistent Cut.
            if (v != null && v.get2() == latestStartedCutVer.get())
                break;
        }

        ConsistentCutState cutState = latestPublishedCutState;

        if (!tx.onePhaseCommit() && tx.near())
            setCutVersion(cutState, tx.nearXidVersion(), tx.xidVersion(), (ConsistentCutVersionAware)tx);
        else if (tx.onePhaseCommit()) {
            ConsistentCutVersionAware s = (ConsistentCutVersionAware)tx;

            if (s.txCutVersion() < 0)
                setCutVersion(cutState, tx.nearXidVersion(), tx.xidVersion(), s);
        }

        if (log.isDebugEnabled()) {
            T2<GridCacheVersion, Long> info = committingTxs.get(tx.xidVersion());

            log.debug("`registerBeforeCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + " , ver=" + (info == null ? null : info.get2()) + ", cutState = " + cutState + ", latestVer = " + latestCutVersion());
        }
    }

    /**
     * Sets Consistent Cut version for transaction.
     */
    private void setCutVersion(
        ConsistentCutState cutState,
        GridCacheVersion nearTxVer,
        GridCacheVersion txVer,
        ConsistentCutVersionAware tx
    ) {
        long cutVer = txCutVersion(cutState, nearTxVer, txVer);

        tx.txCutVersion(cutVer);

        committingTxs.put(txVer, new T2<>(nearTxVer, cutVer));

        if (log.isDebugEnabled()) {
            log.debug("`setCutVersion` from " + nearTxVer.asIgniteUuid() + " to " + txVer.asIgniteUuid()
                + " ver=" + cutVer);
        }
    }

    /**
     * Unregister committed transaction and try finish Consistent Cut.
     */
    public void unregisterAfterCommit(IgniteInternalTx tx) {
        GridCacheVersion txVer = tx.xidVersion();

        ConsistentCutState cutState = latestPublishedCutState;

        if (log.isDebugEnabled()) {
            T2<GridCacheVersion, Long> info = committingTxs.get(txVer);

            log.debug("`unregisterAfterCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + ", state " + cutState + ", txCutVer " + (info == null ? null : info.get2()));
        }

        if (!cutState.ready()) {
            cutState.onCommit(txVer);

            if (checkTransaction(cutState, txVer))
                tryFinish(cutState, null);

            checkReady(cutState);
        }
    }

    /**
     * @return Latest Consistent Cut Version.
     */
    public long latestCutVersion() {
        return latestKnownCutVer.get();
    }

    /**
     * @return Latest Consistent Cut state.
     */
    public ConsistentCutState latestCutState() {
        return latestPublishedCutState;
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

        long knwnCutVer = latestKnownCutVer.get();

        if (cutVer > knwnCutVer) {
            if (latestKnownCutVer.compareAndSet(knwnCutVer, cutVer)) {
                walLog(cutVer, new ConsistentCutStartRecord(cutVer));

                // Allow threads commit transaction and write it to WAL.
                latestStartedCutVer.set(cutVer);

                ConsistentCutState cutState = latestPublishedCutState = consistentCut(knwnCutVer, cutVer);

                afterPublishState(cutState);
            }
        }
    }

    /**
     * After publishing state it's required to check transactions registered concurrently with preparing state and
     * try finish Consistent Cut, if possible.
     */
    private void afterPublishState(ConsistentCutState cutState) {
        Map<GridCacheVersion, T2<GridCacheVersion, Long>> txs = new HashMap<>(committingTxs);

        for (GridCacheVersion txVer: txs.keySet())
            checkTransaction(cutState, txVer);

        cutState.allowFinish();

        tryFinish(cutState, null);

        checkReady(cutState);

        if (log.isDebugEnabled())
            log.debug("Prepare Consistent Cut State afterPublish: " + cutState);
    }

    /**
     * Checks whether transaction can finish current Consistent Cut.
     *
     * @return Whether Consistent Cut can be finished after checking transaction.
     */
    private boolean checkTransaction(ConsistentCutState cutState, GridCacheVersion txVer) {
        T2<GridCacheVersion, Long> v = committingTxs.get(txVer);

        if (v == null)
            return false;

        boolean finished;

        if (v.get2() < cutState.version())
            finished = cutState.includeBeforeCut(txVer, v.get1());
        else
            finished = cutState.checkExclude(txVer);

        if (log.isDebugEnabled())
            log.debug("`checkCommittingTransaction` " + txVer.asIgniteUuid() + " ver =" + v.get2() + " state " + cutState);

        committingTxs.remove(txVer);

        return finished;
    }

    /**
     * Triggers global Consistent Cut procedure.
     */
    public long triggerConsistentCutOnCluster() {
        long cutVer = System.currentTimeMillis();

        if (log.isDebugEnabled())
            log.debug("Start Consistent Cut, version = " + cutVer);

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
        if (log.isDebugEnabled())
            log.debug("Receive ConsistentCutReadyResponse from node " + nodeId + ": " + msg + " . Wait " + notReadySrvNodes);

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
     * @param nearTxVer Transaction ID on near node.
     * @param rmtTxCutVer Consistent Cut Version after which the specified transaction was committed on remote node.
     */
    public void handleRemoteTxCutVersion(GridCacheVersion txVer, GridCacheVersion nearTxVer, long rmtTxCutVer) {
        ConsistentCutState cutState = latestPublishedCutState;

        committingTxs.putIfAbsent(txVer, new T2<>(nearTxVer, rmtTxCutVer));

        if (log.isDebugEnabled()) {
            log.debug("`handleRemoteTxCutVersion` from " + nearTxVer.asIgniteUuid() + " to " + txVer.asIgniteUuid() +
                ", txCutVer=" + rmtTxCutVer + ", state " + cutState + ", lastVer = " + latestCutVersion());
        }
    }

    /**
     * Finds the latest Consistent Cut Version AFTER which the specified transaction committed.
     *
     * @param txVer Transaction local ID.
     * @return the latest Consistent Cut Version AFTER which the specified transaction committed.
     */
    private long txCutVersion(ConsistentCutState cutState, GridCacheVersion nearTxVer, GridCacheVersion txVer) {
        if (cutState.afterCut(txVer))
            return cutState.version();

        if (cutState.beforeCut(nearTxVer))
            return cutState.prevVersion();

        Long cutVer = cutState.txCutVersion(txVer);

        return cutVer != null ? cutVer : latestCutVersion();
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
        Collection<IgniteInternalTx> txs = cctx.tm().activeTransactions();

        for (IgniteInternalTx tx : txs) {
            TransactionState txState = tx.state();

            // Skip fast finish transactions (no write entries).
            if (tx.near() && ((GridNearTxLocal)tx).fastFinish())
                continue;

            // Add all committing transactions to check list.
            if (txState == COMMITTING || txState == COMMITTED)
                cutState.addForCheck(tx);

            if (!tx.onePhaseCommit()) {
                if (tx.near()) {
                    // Prepare request may not achieve primary or backup nodes to the moment of local Consistent Cut.
                    // This case is inconsistent, then exclude such transactions from new Consistent Cut.
                    //
                    // Back --|----Ped----------
                    //       CUT  /
                    //           /       CUT
                    // Prim ----Ping------|-----
                    if (txState == PREPARING || txState == PREPARED) {
                        cutState.txCutVersion(tx.xidVersion(), cutState.version());

                        cutState.includeAfterCut(tx);
                    }
                }
                // Primary or Backup nodes need to check PREPARED transactions and wait for FinishRequest.
                //
                // Back ------Ped---|-----------
                //            / \  CUT   /
                //           /   \      /
                // Prim ---Ping--Ped-----Cing---
                else if (txState == PREPARING || txState == PREPARED)
                    cutState.addForCheck(tx);
            }
            // One phase commit. For 1PC it is used a reverse order for the notifications (backup -> prim -> near).
            else {
                // Near node is waiting for notification from primary.
                if (tx.near() && (txState == PREPARING || txState == PREPARED))
                    cutState.addForCheck(tx);

                // Primary node is waiting for notification from backup.
                else if (tx.dht() && tx.local() && (txState == PREPARING || txState == PREPARED))
                    cutState.addForCheck(tx);

                // Include all transactions on backup.
                //
                // Back ------Ped---|---Ced-------
                //            /    CUT      \
                //           /               \
                // Prim ---Ping----------------
                else if (tx.dht() && !tx.local() && (txState == PREPARING || txState == PREPARED)) {
                    cutState.includeAfterCut(tx);

                    cutState.txCutVersion(tx.xidVersion(), cutState.version());
                }
            }
        }

        // For cases when node has multiple participations: near and primary or backup.
        cutState.beforePublish(cctx.tm());

        if (log.isDebugEnabled())
            log.debug("Prepare Consistent Cut State: " + cutState);

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
    protected void walLog(long cutVer, WALRecord record) {
        try {
            if (cctx.wal() != null) {
                if (log.isDebugEnabled())
                    log.debug("Write ConsistentCut[" + cutVer + "] record to WAL: " + record);

                cctx.wal().log(record);
            }
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

            if (log.isDebugEnabled())
                log.debug("Send ConsistentCutReadyResponse from " + cctx.localNodeId() + ": " + msg);

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

            allowFinish();
            tryFinish(null);
            checkReady();
        }
    }
}
