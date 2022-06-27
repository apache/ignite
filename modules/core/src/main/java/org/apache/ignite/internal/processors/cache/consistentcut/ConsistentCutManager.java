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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
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
 * 1. On receiving new {@link ConsistentCutVersion} it immediately updates local version and create new {@link ConsistentCut}.
 * 2. It grabs active and committing transactions to check which side of Consistent Cut they belong to. It guarantees
 *    that every transaction appeared after this moment belongs to the AFTER side.
 * 3. It writes {@link ConsistentCutStartRecord} before any transaction from the AFTER side committed. It guarantees that
 *    every transaction committed before this record is part of the BEFORE side.
 * 4. It prepares check-list of transactions to verify which side of the Consistent Cut every of them belongs to. It
 *    guarantees that every transaction from this check-list will be handled in {@link #unregisterAfterCommit(IgniteInternalTx)}.
 * 5. Every transaction is signed with the latest Consistent Cut Version AFTER which it committed. This version is defined
 *    at single node within a transaction - {@link #isSetterTxCutVersion(IgniteInternalTx)}}.
 * 6. It's possible to receive transaction finish messages concurrently with preparing the check-list. To avoid misses
 *    such transactions all committing transactions are stored in {@link #beforeCut}.
 * 7. After the check-list is empty it finishes Consistent Cut with writing {@link ConsistentCutFinishRecord} that contains
 *    collection of transactions to include into the BEFORE side.
 * 8. After Consistent Cut finished and every transaction in {@link #beforeCut} committed it notifies coordinator with
 *    {@link ConsistentCutFinishResponse} that local node is ready for next Consistent Cut process.
 */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter {
    /**
     * It atomically updates Consistent Cut Version and creates a new {@link ConsistentCut} object.
     */
    private static final AtomicReferenceFieldUpdater<ConsistentCutManager, ConsistentCutHolder> CUT_HOLDER =
        AtomicReferenceFieldUpdater.newUpdater(ConsistentCutManager.class, ConsistentCutHolder.class, "cutHolder");

    /**
     * Holder for Consistent Cut. Required to track separately {@link ConsistentCutVersion} and nullable {@link ConsistentCut}.
     */
    private volatile ConsistentCutHolder cutHolder;

    /**
     * Colleciton of committing transactions that will be included into the BEFORE side of next Consistent Cut.
     * Track them due to {@link IgniteTxManager#activeTransactions()} doesn't contain information about transactions in
     * COMMITTING / COMMITTED state. But it's required to analyze them to avoid misses.
     */
    private final Set<IgniteInternalTx> beforeCut = ConcurrentHashMap.newKeySet();

    /**
     * Collection of server nodes that aren't ready to run new Consistent Cut procedure. Exist only on coordinator node.
     */
    private final Set<UUID> notFinishedSrvNodes = ConcurrentHashMap.newKeySet();

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

        CUT_HOLDER.set(this, new ConsistentCutHolder(new ConsistentCutVersion(0, 0), null));

        cctx.kernalContext().io().addMessageListener(TOPIC_CONSISTENT_CUT, (nodeId, msg, plc) -> {
            if (msg instanceof ConsistentCutStartRequest)
                handleConsistentCutVersion(nodeId, ((ConsistentCutStartRequest)msg).version());
            else if (msg instanceof ConsistentCutFinishResponse) {
                if (log.isDebugEnabled())
                    log.debug("Receive ConsistentCutReadyResponse before: " + msg + ", node " + nodeId);

                assert U.isLocalNodeCoordinator(cctx.discovery());

                handleConsistentCutFinishResponse(nodeId, (ConsistentCutFinishResponse)msg);
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
            if (notFinishedSrvNodes != null)
                notFinishedSrvNodes.remove(e.node().id());

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

                if (!F.isEmpty(notFinishedSrvNodes)) {
                    log.warning("Skip Consistent Cut procedure. Some nodes hasn't finished yet previous one. " +
                        "Latest version: " + CUT_HOLDER.get(ConsistentCutManager.this).ver +
                        "\n\tConsistent Cut may require more time that it configured." +
                        "\n\tConsider to increase param `DataStorageConfiguration#setPointInTimeRecoveryPeriod`. " +
                        "\n\tNodes that hasn't finished their job: " + notFinishedSrvNodes);

                    scheduleConsistentCut(delay, period);

                    return;
                }

                ConsistentCutVersion cutVer = triggerConsistentCutOnCluster();

                long d = (cutVer.timestamp() + period) - System.currentTimeMillis();

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
     * Registers transaction before commit it, defines Consistent Cut Version if needed.
     *
     * It invokes before committing transactions leave {@link IgniteTxManager#activeTransactions()}.
     */
    public void registerBeforeCommit(IgniteInternalTx tx) {
        long txCutVer = setTxCutVersionIfNeeded(tx);

        ConsistentCut cut = CUT_HOLDER.get(this).cut;

        try {
            // If Consistent Cut isn't running now then it might be started in any moment after registering this transaction.
            // To avoid misses add every transaction while Consistent Cut is grabbing active transactions.
            if (cut == null || (cut.grabTransactionsInProgress() && cut.txBeforeCut(txCutVer)))
                beforeCut.add(tx);

            if (cut != null)
                cut.processTxBeforeCommit(txCutVer);

            if (log.isDebugEnabled()) {
                log.debug("`registerBeforeCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                    + " , ver=" + ((ConsistentCutVersionAware)tx).txCutVersion() + ", cut = " + cut + ", latestVer = "
                    + latestKnownCutVersion());
            }
        }
        catch (IgniteCheckedException e) {
            onFinish(cut, true);
        }
    }

    /**
     * Unregisters committed transaction and tries finish local Consistent Cut.
     *
     * It invokes after specified transaction committed and write related {@link TxRecord} to WAL.
     */
    public void unregisterAfterCommit(IgniteInternalTx tx) {
        long v = ((ConsistentCutVersionAware)tx).txCutVersion();

        ConsistentCut cut = CUT_HOLDER.get(this).cut;

        // Safely remove transaction here as it's whether:
        // 1. Committed before Consistent Cut write `ConsistentCutStartRecord` to WAL.
        // 2. Grabbed to be checked while preparing the check-list.
        boolean incl = beforeCut.remove(tx);

        try {
            if (cut != null && cut.processTxAfterCommit(tx, !incl) && beforeCut.isEmpty())
                onFinish(cut, false);

            if (log.isDebugEnabled()) {
                log.debug("`unregisterAfterCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                    + ", cut " + cut + ", txCutVer " + v + "; latestCutVer " + latestKnownCutVersion());
            }
        }
        catch (IgniteCheckedException e) {
            onFinish(cut, true);
        }
    }

    /**
     * @return Latest known Consistent Cut Version, no matter whether this Consistent Cut has started or finished.
     */
    public ConsistentCutVersion latestKnownCutVersion() {
        return CUT_HOLDER.get(this).ver;
    }

    /**
     * Handles received Consistent Cut Version from remote node. It compares it with the latest version that local node
     * is aware of. Starts local Consistent Cut procedure if received version is greater than the local.
     *
     * @param crdNodeId ID of Consistent Cut coordinator node.
     * @param rcvCutVer Received Cut Version from different node.
     */
    public void handleConsistentCutVersion(UUID crdNodeId, ConsistentCutVersion rcvCutVer) {
        if (this.crdNodeId == null)
            this.crdNodeId = crdNodeId;
        else
            assert this.crdNodeId.equals(crdNodeId);

        ConsistentCutHolder cutHolder = CUT_HOLDER.get(this);

        if (rcvCutVer.compareTo(cutHolder.ver.version()) > 0) {
            ConsistentCutHolder holder = new ConsistentCutHolder(rcvCutVer, consistentCut(cctx, log, rcvCutVer));

            if (CUT_HOLDER.compareAndSet(this, cutHolder, holder)) {
                ConsistentCut cut = holder.cut;

                try {
                    if (cut.prepare(beforeCut) && beforeCut.isEmpty())
                        onFinish(holder.cut, false);

                    if (log.isDebugEnabled())
                        log.debug("Prepared Consistent Cut State: " + cut);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to write to WAL local Consistent Cut record.", e);

                    onFinish(cut, true);

                    throw new IgniteException(e);
                }
            }
        }
    }

    /**
     * Finishes local Consistent Cut: cleans {@link ConsistentCut} reference and sends finish message to Consistent Cut coordinator node.
     */
    private void onFinish(ConsistentCut cut, boolean err) {
        try {
            ConsistentCutHolder curHolder = CUT_HOLDER.get(this);

            if (curHolder.cut != null) {
                ConsistentCutHolder holder = new ConsistentCutHolder(curHolder.ver, null);

                if (CUT_HOLDER.compareAndSet(this, curHolder, holder)) {
                    if (cctx.kernalContext().clientNode())
                        return;

                    Message msg = new ConsistentCutFinishResponse(holder.ver, err);

                    if (log.isDebugEnabled())
                        log.debug("Send ConsistentCutFinishResponse from " + cctx.localNodeId() + ": " + msg);

                    cctx.kernalContext().io().sendToGridTopic(crdNodeId, TOPIC_CONSISTENT_CUT, msg, SYSTEM_POOL);
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send Consistent Cut Finish message to coordinator node.", e);

            throw new IgniteException(e);
        }
    }

    /**
     * Triggers global Consistent Cut procedure.
     *
     * @return New Consistent Cut Version.
     */
    ConsistentCutVersion triggerConsistentCutOnCluster() {
        AffinityTopologyVersion topVer = cctx.kernalContext().discovery().topologyVersionEx();

        assert F.isEmpty(notFinishedSrvNodes);

        Set<UUID> srvNodes = ConcurrentHashMap.newKeySet();

        cctx.kernalContext().discovery().serverNodes(topVer).forEach((n) -> srvNodes.add(n.id()));

        notFinishedSrvNodes.addAll(srvNodes);

        ConsistentCutVersion cutVer = new ConsistentCutVersion(CUT_HOLDER.get(this).ver.version() + 1, System.currentTimeMillis());

        if (log.isDebugEnabled())
            log.debug("Start Consistent Cut, version = " + cutVer);

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
            log.debug("Receive ConsistentCutReadyResponse from node " + nodeId + ": " + msg + " . Wait " + notFinishedSrvNodes);

        if (!notFinishedSrvNodes.remove(nodeId)) {
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
     * @return {@code true} if it's safe to run new Consistent Cut procedure.
     */
    boolean latestGlobalCutReady() {
        return F.isEmpty(notFinishedSrvNodes);
    }

    /**
     * Sets Consistent Cut version for specified transaction if it hasn't set yet. Single node is responsible for
     * setting the version within transaction (see {@link #isSetterTxCutVersion(IgniteInternalTx)}). Other nodes
     * recieves this version due to Finish messages.
     *
     * @return Consistent Cut version AFTER which specified transctions committed.
     */
    private long setTxCutVersionIfNeeded(IgniteInternalTx tx) {
        ConsistentCutVersionAware txCutVerAware = (ConsistentCutVersionAware)tx;

        if (isSetterTxCutVersion(tx)) {
            assert txCutVerAware.txCutVersion() == -1 : tx;

            txCutVerAware.txCutVersion(latestKnownCutVersion().version());
        }

        long txCutVer = txCutVerAware.txCutVersion();

        assert txCutVer >= 0 : tx;

        return txCutVer;
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

    /**
     * Holds optional current Consistent Cut procedure and latest known Consistent Cut version.
     */
    private static final class ConsistentCutHolder {
        /** */
        private final ConsistentCutVersion ver;

        /** */
        private final @Nullable ConsistentCut cut;

        /** */
        public ConsistentCutHolder(ConsistentCutVersion ver, @Nullable ConsistentCut cut) {
            this.ver = ver;
            this.cut = cut;
        }
    }

    /** For test purposes. */
    protected ConsistentCut consistentCut(GridCacheSharedContext<?, ?> cctx, IgniteLogger log, ConsistentCutVersion cutVer) {
        return new ConsistentCut(cctx, log, cutVer);
    }

    /** For test purposes. */
    ConsistentCut consistentCut() {
        return CUT_HOLDER.get(this).cut;
    }
}
