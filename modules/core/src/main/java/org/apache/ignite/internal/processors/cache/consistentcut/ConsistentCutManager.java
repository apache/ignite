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
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
 *
 * Consistent Cut splits timeline on 2 global areas - BEFORE and AFTER. It guarantees that every transaction committed BEFORE
 * also will be committed BEFORE on every other node. It means that an Ignite node can safely recover itself to this
 * point without any coordination with other nodes.
 *
 * The algorithm starts on Ignite coordinator node by timer. Period of starting Consistent Cut is defined in
 * {@link DataStorageConfiguration#setPitrPeriod(long)}. Other nodes notifies with direct message from coordinator
 * {@link ConsistentCutStartRequest} or by transaction messages {@link ConsistentCutVersionAware}. After node finishes
 * Consistent Cut locally and becomes ready for new Consistent Cut it notifies coordinator with {@link ConsistentCutReadyResponse}.
 *
 * Coordinator node guarntees that Consistent Cut Version is growing monotonously.
 *
 * The algorithm consist of steps:
 * 1. On receiving new version it immediately updates {@link #latestKnownCutVer} to the newest one.
 * 2. It writes {@link ConsistentCutStartRecord} before any transaction committed. It guarantees that every transaction
 *    committed before this record is part of the BEFORE state.
 * 3. It prepares collection of transactions: to commit BEFORE, to commit AFTER and the check-list to verify. Those collections
 *    are stored in {@link ConsistentCutState}, and it publishes the state after preparing.
 * 4. Published state is checked by every transaction after commit. If transaction is in the check-list or the before-list,
 *    it notifies the state.
 * 5. Prepared state might be incomplete due to receiving tx finish requests concurrently with the state preparing.
 *    Then it might be unsafe to finish Consistent Cut before analyzing such transactions. Committing transactions are stored
 *    in {@link #committingTxs}. It merges the prepared state with committing transactions. It guarantees that there is no
 *    any missed transactions that can affect the state. After the merge it enables finishing Consistent Cut.
 * 6. After the check-list is empty it finishes Consistent Cut with writing {@link ConsistentCutFinishRecord} that contains
 *    collection of transactions to include BEFORE.
 * 7. After every transaction in the before-list committed it notifies coordinator with {@link ConsistentCutReadyResponse}
 *    that local node is ready for next Consistent Cut process.
 */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter {
    /** {@link #latestKnownCutVer} field updater. */
    private static final AtomicLongFieldUpdater<ConsistentCutManager> LATEST_KNOWN_CUT_VERSION =
        AtomicLongFieldUpdater.newUpdater(ConsistentCutManager.class, "latestKnownCutVer");

    /**
     * It serves the Step 1.
     *
     * The latest Consistent Cut Version local node is aware of. Knowing means that local node is aware of this version,
     * but it may not start Consistent Cut locally yet. Then known version can be greater than {@link #latestPublishedCutState}
     * version while the latter hasn't prepared and published.
     */
    private volatile long latestKnownCutVer;

    /**
     * It serves the Step 2.
     *
     * It guarantees that every committing transaction awaits switching {@link #latestPublishedCutState}. It's required
     * to guarantee that transaction from AFTER side won't be committed before {@link ConsistentCutStartRecord}.
     */
    private volatile Phaser cutStartPhase = new Phaser();

    /**
     * It serves the Step 3.
     *
     * Helps to switch actual {@link ConsistentCutState}. It guarantees atomic updating {@link #latestPublishedCutState} and
     * writing {@link ConsistentCutStartRecord} to WAL. It serves the Step 3.
     */
    private final ReentrantReadWriteLock cutPublishedStateGuard = new ReentrantReadWriteLock();

    /**
     * State of the latest published Consistent Cut.
     */
    private ConsistentCutState latestPublishedCutState;

    /**
     * It serves the Step 5.
     *
     * Colleciton of transaction Consistent Cut Version info. It contains the latest Consistent Cut version AFTER that
     * this transaction is committed.
     */
    private final Set<IgniteInternalTx> committingTxs = ConcurrentHashMap.newKeySet();

    /**
     * Collection of server nodes that aren't ready to run new Consistent Cut procedure.
     */
    private final Set<UUID> notReadySrvNodes = ConcurrentHashMap.newKeySet();

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
                        "Last 2 versions: " + latestPublishedCutState().prevVersion() + ", " + latestPublishedCutState().version() +
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
     * Register Consistent Cut Version for transaction before commit it.
     */
    public void registerBeforeCommit(IgniteInternalTx tx) {
        ConsistentCutState cutState = latestPublishedCutState();

        // If current state is ready then new Consistent Cut might be started after registering this transaction.
        // It's safe to add transaction to `committingTxs` here because it is still in `#activeTransactions` at this moment,
        // and this transaction wouldn't be missed.
        if (cutState.ready())
            committingTxs.add(tx);

        ConsistentCutVersionAware txCutVerAware = (ConsistentCutVersionAware)tx;

        if (txCutVerAware.txCutVerSetNode())
            setTransactionCutVersion(txCutVerAware, cutState);

        assert txCutVerAware.txCutVersion() >= 0 : tx;

        if (log.isDebugEnabled()) {
            long v = ((ConsistentCutVersionAware)tx).txCutVersion();

            log.debug("`registerBeforeCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + " , ver=" + v + ", cutState = " + cutState + ", latestVer = " + latestKnownCutVersion());
        }
    }

    /**
     * Sets Consistent Cut version for the specified transaction.
     */
    private void setTransactionCutVersion(ConsistentCutVersionAware tx, ConsistentCutState cutState) {
        assert tx.txCutVersion() == -1 : tx;

        long cutVer = transactionCutVersion(cutState, tx);

        tx.txCutVersion(cutVer);

        if (log.isDebugEnabled()) {
            log.debug("`setCutVersion` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xidVersion().asIgniteUuid()
                + " ver=" + cutVer + " " + tx.getClass().getSimpleName());
        }
    }

    /**
     * Finds the latest Consistent Cut Version AFTER which the specified transaction will commit.
     */
    private long transactionCutVersion(ConsistentCutState cutState, ConsistentCutVersionAware tx) {
        if (cutState.afterCut(tx.xidVersion()))
            return cutState.version();
        else if (cutState.beforeCut(tx.nearXidVersion()))
            return cutState.prevVersion();
        else
            return latestKnownCutVersion();
    }

    /**
     * Unregister committed transaction and try finish local Consistent Cut.
     */
    public void unregisterAfterCommit(IgniteInternalTx tx) {
        ConsistentCutState cutState = latestPublishedCutState();

        if (!cutState.ready()) {
            if (checkTransaction(cutState, tx))
                walLog(cutState.version(), cutState.buildFinishRecord());

            cutState.onCommit(tx.xidVersion());

            checkReady(cutState);
        }

        // Safely remove transaction here as it's whether:
        // 1. Committed before Consistent Cut if state is still ready
        // 2. Analyzed withing Consistent Cut while this method has been locking on `cutStartGuard` in `latestPublishedCutState`.
        committingTxs.remove(tx);

        if (log.isDebugEnabled()) {
            long v = ((ConsistentCutVersionAware)tx).txCutVersion();

            log.debug("`unregisterAfterCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + ", state " + cutState + ", txCutVer " + v);
        }
    }

    /**
     * @return Latest known Consistent Cut Version.
     */
    public long latestKnownCutVersion() {
        return LATEST_KNOWN_CUT_VERSION.get(this);
    }

    /**
     * This state is required for committing transaction. It's important to have actual state before commit transaction.
     * It's required to exactly understand which Cut Version this transaction belongs to.
     *
     * @return Latest published Consistent Cut state.
     */
    public ConsistentCutState latestPublishedCutState() {
        ConsistentCutState cutState;

        cutPublishedStateGuard.readLock().lock();

        try {
            cutState = latestPublishedCutState;
        }
        finally {
            cutPublishedStateGuard.readLock().unlock();
        }

        // If version was updated but states hasn't been changed try to get the state again.
        if (cutState.version() < latestKnownCutVersion()) {
            cutStartPhase.awaitAdvance(0);

            return latestPublishedCutState();
        }

        return cutState;
    }

    /**
     * @return {@code true} if it's safe to run new Consistent Cut procedure.
     */
    public boolean latestGlobalCutReady() {
        return F.isEmpty(notReadySrvNodes);
    }

    /**
     * Handles received Consistent Cut Version from remote node. It compares it with the latest version that local node
     * is aware of. Starts local Consistent Cut procedure if received version is greater than the local.
     *
     * @param crdNodeId ID of Consistent Cut coordinator node.
     * @param rcvCutVer Received Cut Version from different node.
     */
    public void handleConsistentCutVersion(UUID crdNodeId, long rcvCutVer) {
        if (this.crdNodeId == null)
            this.crdNodeId = crdNodeId;
        else
            assert this.crdNodeId.equals(crdNodeId);

        long cutVer = LATEST_KNOWN_CUT_VERSION.get(this);

        if (rcvCutVer > cutVer && LATEST_KNOWN_CUT_VERSION.compareAndSet(this, cutVer, rcvCutVer)) {
            ConsistentCutState cutState;

            cutStartPhase.register();

            try {
                cutState = startLocalConsistentCut(cutVer, rcvCutVer);
            }
            finally {
                // Enable threads with committing transactions fetches actual state.
                cutStartPhase.arriveAndDeregister();
            }

            // Try to finish Consistent Cut immediately.
            if (cutState.tryFinish())
                walLog(cutState.version(), cutState.buildFinishRecord());

            checkReady(cutState);

            if (log.isDebugEnabled())
                log.debug("Prepare Consistent Cut State afterPublish: " + cutState);
        }
    }

    /**
     * Atomically writes {@link ConsistentCutStartRecord}, prepares and publishes actual {@link ConsistentCutState}.
     */
    protected ConsistentCutState startLocalConsistentCut(long prevCutVer, long newCutVer) {
        cutPublishedStateGuard.writeLock().lock();

        try {
            walLog(newCutVer, new ConsistentCutStartRecord(newCutVer));

            return latestPublishedCutState = consistentCut(prevCutVer, newCutVer);
        }
        finally {
            cutPublishedStateGuard.writeLock().unlock();
        }
    }

    /**
     * Checks whether transaction can finish current Consistent Cut.
     *
     * @return Whether Consistent Cut can be finished after checking transaction.
     */
    private boolean checkTransaction(ConsistentCutState cutState, IgniteInternalTx tx) {
        ConsistentCutVersionAware aware = (ConsistentCutVersionAware)tx;

        boolean finished;

        if (aware.txCutVersion() < cutState.version())
            finished = cutState.checkInclude(tx);
        else
            finished = cutState.checkExclude(tx.xidVersion());

        if (log.isDebugEnabled())
            log.debug("`checkTransaction` " + tx.xid() + " state " + cutState + " finished=" + finished + " " + aware.txCutVersion());

        return finished;
    }

    /**
     * Triggers global Consistent Cut procedure.
     *
     * @return New Consistent Cut Version.
     */
    public long triggerConsistentCutOnCluster() {
        long cutVer = System.currentTimeMillis();

        if (log.isDebugEnabled())
            log.debug("Start Consistent Cut, version = " + cutVer);

        AffinityTopologyVersion topVer = cctx.kernalContext().discovery().topologyVersionEx();

        assert F.isEmpty(notReadySrvNodes);

        Set<UUID> srvNodes = ConcurrentHashMap.newKeySet();

        cctx.kernalContext().discovery().serverNodes(topVer).forEach((n) -> srvNodes.add(n.id()));

        notReadySrvNodes.addAll(srvNodes);

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

            // Skip fast finish transactions (no write entries).
            if (tx.near() && ((GridNearTxLocal)tx).fastFinish())
                continue;

            if (txState == COMMITTING || txState == COMMITTED)
                cutState.addForCheck(tx);

            if (txState == PREPARING || txState == PREPARED) {
                if (((ConsistentCutVersionAware)tx).txCutVerSetNode())
                    cutState.includeAfterCut(tx);
                else
                    cutState.addForCheck(tx);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Prepare Consistent Cut State: " + cutState);

        return cutState;
    }

    /**
     * Checks whether local node is ready for new Consistent Cut and send notification if it is ready.
     *
     * @param cutState Local Consistent Cut state.
     */
    private void checkReady(ConsistentCutState cutState) {
        if (cutState.tryReady())
            onFinish(cutState.version(), false);
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

            onFinish(cutVer, true);

            throw new IgniteException(e);
        }
    }

    /**
     * Finishes local Consistent Cut: re-inits variables, sends finish message to Consistent Cut coordinator node.
     */
    private void onFinish(long cutVer, boolean err) {
        try {
            cutStartPhase = new Phaser();

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
        }

        /** {@inheritDoc} */
        @Override public boolean ready() {
            return true;
        }
    }
}
