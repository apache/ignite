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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

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
public class ConsistentCutManager extends GridCacheSharedManagerAdapter implements PartitionsExchangeAware {
    /**
     * It serves updates of {@link #cutState} with CAS.
     */
    protected static final AtomicReferenceFieldUpdater<ConsistentCutManager, ConsistentCutState> CONSITENT_CUT_STATE =
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
     * Collection of committing and committed transactions. Track them for two reasons:
     * 1.{@link IgniteTxManager#activeTransactions()} doesn't contain information about transactions in COMMITTING / COMMITTED state.
     * 2. For transaction recovery process, it's required to extract Consistent Cut Version for a transaction even after
     *    it committed.
     *
     * Key is {@link IgniteInternalTx#xidVersion()} - transaction version on local node.
     */
    private final Map<GridCacheVersion, IgniteInternalTx> committingTxs = new ConcurrentHashMap<>();

    /**
     * On Consistent Cut initiator node, tracks server nodes hasn't finished Consistent Cut yet.
     */
    protected volatile @Nullable Set<UUID> notFinishedSrvNodes;

    /**
     * Schedules next global Consistent Cut. {@code null} on non-coordinator nodes, or if Consistent Cut scheduling
     * was disabled.
     */
    protected volatile @Nullable GridTimeoutProcessor.CancelableTask scheduleConsistentCutTask;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        CONSITENT_CUT_STATE.set(this, new ConsistentCutState(new ConsistentCutVersion(0, null), null));

        cctx.kernalContext().io().addMessageListener(TOPIC_CONSISTENT_CUT, (nodeId, msg, plc) -> {
            if (msg instanceof ConsistentCutStartRequest)
                handleConsistentCutVersion(((ConsistentCutStartRequest)msg).version());
            else if (msg instanceof ConsistentCutFinishResponse)
                handleConsistentCutFinishResponse(nodeId, (ConsistentCutFinishResponse)msg);
        });

        cctx.exchange().registerExchangeAwareComponent(this);
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        super.stop0(cancel);

        disableScheduling(false);
    }

    /**
     * Stops scheduling new Consistent Cut in case of server topology changed.
     */
    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        if (fut.serverNodeDiscoveryEvent())
            disableScheduling(true);
    }

    /**
     * Extract Consistent Cut version by specified transaction version.
     *
     * @param xidVer Transaction version on local node.
     * @return Consistent Cut version AFTER which specified transaction committed. {@code null} guarantees that transaction
     *         committed BEFORE the latest known Consistent Cut version.
     */
    public @Nullable ConsistentCutVersion txCutVersion(GridCacheVersion xidVer) {
        IgniteInternalTx tx = committingTxs.get(xidVer);

        return tx != null ? ((ConsistentCutVersionAware)tx).txCutVersion() : null;
    }

    /**
     * Schedules global Consistent Cut procedure on Ignite coordinator.
     */
    public void scheduleConsistentCut() {
        if (scheduleConsistentCutTask != null)
            return;

        long period = CU.getPitrPointsPeriod(cctx.gridConfig());

        scheduleConsistentCutTask = cctx.time().schedule(() -> {
            Set<UUID> awaitNodes = notFinishedSrvNodes;

            if (awaitNodes == null)
                triggerConsistentCutOnCluster( "onTimeout");
            else {
                log.warning("Skip Consistent Cut procedure. " +
                    "\n  ^-- Some nodes hasn't finished yet previous one. Latest version: " + latestKnownCutVersion() +
                    "\n  ^-- Consistent Cut may require more time that is configured." +
                    "\n  ^-- Consider to increase param `DataStorageConfiguration#setPointInTimeRecoveryPeriod`. " +
                    "\n  ^-- Nodes that hasn't finished their job: " + awaitNodes);
            }
        }, period, period);
    }

    /**
     * Disables scheduling new Consistent Cut procedures on Ignite coordinator.
     */
    protected void disableScheduling(boolean topChanged) {
        GridTimeoutProcessor.CancelableTask task = scheduleConsistentCutTask;

        if (task != null) {
            if (topChanged) {
                U.error(log, "PITR (Point-in-time-recovery) is not available since the moment. " +
                    "\n  ^-- PITR doesn't support server topology changes. " +
                    "\n  ^-- The latest version to recover on is " + latestKnownCutVersion().version() + "." +
                    "\n  ^-- To enable PITR again, please start new cluster snapshot.");
            }

            task.close();

            scheduleConsistentCutTask = null;
            notFinishedSrvNodes = null;
        }
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
                + " , ver=" + ((ConsistentCutVersionAware)tx).txCutVersion() + ", cutVer = " + latestKnownCutVersion());
        }

        return committingTxs.put(tx.xidVersion(), tx) == null;
    }

    /**
     * Cancels transaction registration (with {@link #registerBeforeCommit(IgniteInternalTx)}) in case it is being finished
     * concurrently.
     *
     * @param tx Transaction.
     */
    public void cancelRegisterBeforeCommit(IgniteInternalTx tx) {
        committingTxs.remove(tx.xidVersion());
    }

    /**
     * Unregisters committed transaction. It invokes after specified transaction committed and wrote related {@link TxRecord} to WAL.
     *
     * @param tx Transaction.
     */
    public void unregisterAfterCommit(IgniteInternalTx tx) {
        if (CONSITENT_CUT_STATE.get(this).cut() == null)
            committingTxs.remove(tx.xidVersion());
    }

    /**
     * Handles received Consistent Cut Version from remote node. It compares it with the latest version that local node
     * is aware of. Init local Consistent Cut procedure if received version is greater than the local.
     *
     * @param rcvCutVer Received Cut Version from different node.
     */
    public void handleConsistentCutVersion(ConsistentCutVersion rcvCutVer) {
        ConsistentCutState cutState = CONSITENT_CUT_STATE.get(this);
        ConsistentCutVersion cutVer = cutState.version();

        if (rcvCutVer.version() > cutVer.version()) {
            ConsistentCut cut = newConsistentCut();

            ConsistentCutState newCutState = new ConsistentCutState(rcvCutVer, cut);

            if (CONSITENT_CUT_STATE.compareAndSet(this, cutState, newCutState)) {
                cctx.kernalContext().pools().getSystemExecutorService().submit(() -> {
                    try {
                        // No need to track old transactions after version update.
                        cleanCommittedTransactions(rcvCutVer);

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

    /** Creates new Consistent Cut instance. */
    protected ConsistentCut newConsistentCut() {
        return new ConsistentCut(cctx);
    }

    /**
     * @return Reference to mutable collection of committing transactions.
     */
    Collection<IgniteInternalTx> committingTxs() {
        return committingTxs.values();
    }

    /**
     * Finishes local Consistent Cut: cleans {@link ConsistentCut} reference and sends finish message to Consistent Cut coordinator node.
     *
     * @param err Exception if occured.
     */
    void onFinish(@Nullable Throwable err) {
        try {
            ConsistentCutState cutState = CONSITENT_CUT_STATE.get(this);

            if (cutState.cut() != null) {
                ConsistentCutVersion cutVer = cutState.version();

                ConsistentCutState finishedCutState = new ConsistentCutState(cutVer);

                if (CONSITENT_CUT_STATE.compareAndSet(this, cutState, finishedCutState)) {
                    // Clean transactions that committed BEFORE concurrently with Consistent Cut procedure.
                    cleanCommittedTransactions(cutVer);

                    if (cctx.kernalContext().clientNode())
                        return;

                    Message msg = new ConsistentCutFinishResponse(cutVer);

                    if (log.isDebugEnabled())
                        log.debug("Send " + msg + " from " + cctx.localNodeId());

                    cctx.kernalContext().io().sendToGridTopic(cutVer.cutCrdNodeId(), TOPIC_CONSISTENT_CUT, msg, SYSTEM_POOL);
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send Consistent Cut Finish message to coordinator node.", e);
        }
    }

    /**
     * Cleans transactions committed BEFORE specified version.
     */
    private void cleanCommittedTransactions(ConsistentCutVersion cutVer) {
        committingTxs.values().removeIf(tx ->
            tx.finishFuture().isDone()
                && tx.state() == TransactionState.COMMITTED
                && cutVer.compareToNullable(((ConsistentCutVersionAware)tx).txCutVersion()) > 0
        );
    }

    /**
     * Triggers global Consistent Cut procedure.
     *
     * @param reason Reason to trigger new Consistent Cut.
     * @return New Consistent Cut Version.
     */
    ConsistentCutVersion triggerConsistentCutOnCluster(String reason) {
        assert notFinishedSrvNodes == null;

        Set<UUID> srvNodes = ConcurrentHashMap.newKeySet();

        cctx.kernalContext().discovery().serverNodes(AffinityTopologyVersion.NONE).forEach((n) -> srvNodes.add(n.id()));

        notFinishedSrvNodes = srvNodes;

        ConsistentCutVersion prevCutVer = CONSITENT_CUT_STATE.get(this).version();

        ConsistentCutVersion cutVer = new ConsistentCutVersion(prevCutVer.version() + 1, cctx.localNodeId());

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
            }
        }

        return cutVer;
    }

    /**
     * Coordinator handles finish responses from remote nodes.
     */
    void handleConsistentCutFinishResponse(UUID nodeId, ConsistentCutFinishResponse msg) {
        Set<UUID> awaitNodes = notFinishedSrvNodes;

        // Receive response after ConsistentCut was disablied.
        if (awaitNodes == null)
            return;

        if (log.isDebugEnabled())
            log.info("Received ConsistentCutReadyResponse from node " + nodeId + ": " + msg + " . Wait " + awaitNodes);

        awaitNodes.remove(nodeId);

        if (awaitNodes.isEmpty())
            notFinishedSrvNodes = null;
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
        ConsistentCutVersionAware txCutVerAware = (ConsistentCutVersionAware)tx;

        if (isSetterTxCutVersion(tx))
            txCutVerAware.txCutVersion(latestKnownCutVersion());
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
}
