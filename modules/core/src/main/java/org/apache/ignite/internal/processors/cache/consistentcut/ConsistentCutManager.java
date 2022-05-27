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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.ConsistentCutCheckDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.ConsistentCutStartDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.ConsistentCutStartRequest;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.internal.GridTopic.TOPIC_CONSISTENT_CUT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.PREPARING;

/**
 * Manages all stuff related to Consistent Cut.
 */
public class ConsistentCutManager {
    /**
     * Mutable state of the latest Consistent Cut.
     */
    private volatile ConsistentCutState latestCutState;

    /**
     * Collection of transactions in COMMITTING state.
     */
    private final Set<IgniteInternalTx> committingTxs = ConcurrentHashMap.newKeySet();

    /**
     * Set of transactions to exclude from {@link #latestCutState} and include them into next Consistent Cut.
     */
    private final Set<GridCacheVersion> includeNext = ConcurrentHashMap.newKeySet();

    /**
     * Guards {@link #latestCutState}. When one thread updates the state, other threads with messages may change it.
     */
    private final ReentrantReadWriteLock cutGuard = new ReentrantReadWriteLock();

    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    private final IgniteLogger log;

    /** */
    public ConsistentCutManager(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;

        log = cctx.logger(getClass());

        latestCutState = new InitialConsistentCutState();

        cctx.discovery().setCustomEventListener(ConsistentCutCheckDiscoveryMessage.class, (topVer, snd, m) ->
            handleConsistentCutCheckEvent(m)
        );

        cctx.discovery().setCustomEventListener(ConsistentCutStartDiscoveryMessage.class, (topVer, snd, m) ->
            handleConsistentCutStartEvent()
        );

        cctx.kernalContext().io().addMessageListener(TOPIC_CONSISTENT_CUT, (nodeId, msg, plc) ->
            cctx.kernalContext().pools().consistentCutExecutorService().execute(() ->
                cctx.consistentCutMgr().handleConsistentCutStart(((ConsistentCutStartRequest)msg).version())
            )
        );
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

        // Nead lock here due to cases when tx is in PREPARED and there is concurrency between CUT and rcv FinishRequest.
        // We have to wait here finishing of ConsistentCut to avoid cases when tx holds in `cutAwait`.
        cutGuard.readLock().lock();

        try {
            ConsistentCutState s = latestCutState;

            if (s.finished() && includeNext.isEmpty())
                return;

            GridCacheVersion txVer = tx.nearXidVersion();

            if (log.isDebugEnabled())
                log.debug("`unregisterAfterCommit` " + txVer.asIgniteUuid() + " " + s);

            includeNext.remove(tx.nearXidVersion());

            // In some cases there is a concurrency between TX states and the ConsistentCut procedure.
            // Transaction was included to `cutAwait` (CC procedure) but already had received message that allows to commit it.
            // Then we just verify transactions in moment of commit.
            // - 1PC + (near or primary node).
            // - 2PC + (primary or backup).
            if ((tx.onePhaseCommit() && tx.local()) || (!tx.onePhaseCommit() && tx.dht())) {
                if (s.tryFinish(txVer)) {
                    if (log.isDebugEnabled())
                        log.debug("Log " + s + ") on `unregisterAfterCommit `" + txVer.asIgniteUuid());

                    logConsistentCutFinish(s);
                }
            }
        }
        finally {
            cutGuard.readLock().unlock();
        }
    }

    /**
     * @return Last CutVersion.
     */
    public long latestCutVer() {
        return latestCutState.ver();
    }

    /**
     * Checks local CutVersion and run ConsistentCut if version has changed.
     *
     * @param cutVer Received CutVersion from different node.
     */
    public void handleConsistentCutStart(long cutVer) {
        // Already handled this version.
        if (latestCutVer() >= cutVer)
            return;

        // Try handle new version.
        if (cutGuard.writeLock().tryLock()) {
            try {
                consistentCut(cutVer);
            }
            finally {
                cutGuard.writeLock().unlock();
            }
        }
        // Some other thread already has handled it. Just wait it for finishing.
        else {
            cutGuard.readLock().lock();

            cutGuard.readLock().unlock();
        }
    }

    /**
     * Handles GridTxFinishRequest for 2PC and GridTxPrepareResponse for 1PC. Local node verifies list of transactions
     * that are waiting for the check and make a decision to include transaction to CutVersion.
     *
     * @param nearTxId Transaction ID on near node.
     * @param txLastCutVer Received last CutVersion in momemnt the tx committed on remote node.
     */
    public void handleRemoteTxCutVersion(GridCacheVersion nearTxId, long txLastCutVer, boolean onePhaseCommit) {
        ConsistentCutState s = latestCutState;

        if (s.finished())
            return;

        if (log.isDebugEnabled())
            log.debug("`handleRemoteTxCutVersion` " + nearTxId.asIgniteUuid() + " " + s);

        Long prepVer = s.needCheck(nearTxId);

        if (prepVer != null) {
            if (txLastCutVer >= s.ver() && txLastCutVer > prepVer) {
                s.exclude(nearTxId);

                includeNext.add(nearTxId);
            }

            if (s.tryFinish(nearTxId)) {
                if (log.isDebugEnabled()) {
                    log.debug("Log ConsistentCutFinish(ver=" + s.ver() + ") on `handleRemoteTxCutVersion`" +
                        " for tx " + nearTxId.asIgniteUuid());
                }

                logConsistentCutFinish(s);
            }
        }
    }

    /**
     * @return CutVersion for specified transaction.
     */
    public long txCommitCutVer(IgniteTxAdapter tx) {
        // Need lock here to avoid concurrent threads - that prepare FinishRequest and making ConsistentCut.
        cutGuard.readLock().lock();

        try {
            ConsistentCutState s = latestCutState;

            if (tx.near() && !tx.onePhaseCommit()) {
                if (s.includes(tx.nearXidVersion()))
                    return s.ver() - 1;

                Long v = s.txCutVer(tx.nearXidVersion());

                if (v != null)
                    return v;
            }
            // Shows that this 1PC will be included to PREVIOUS cut (decrement lastCutVer to show that we need prev one).
            else if (tx.dht() && !tx.local() && tx.onePhaseCommit() && s.includes(tx.nearXidVersion()))
                return s.ver() - 1;

            // Commit after Cut.
            return s.ver();
        }
        finally {
            cutGuard.readLock().unlock();
        }
    }

    /**
     * Handles a Consistent Cut Check event sent over discovery.
     */
    private void handleConsistentCutCheckEvent(ConsistentCutCheckDiscoveryMessage m) {
        if (m.progress()) {
            log.warning("Skip Consistent Cut procedure. Some nodes hasn't finished yet previous one.");

            return;
        }

        if (skipConsistentCut())
            m.inProgress();
    }

    /**
     * Handles Consistent Cut Start event sent over discovery.
     */
    private void handleConsistentCutStartEvent() {
        // Coordinator inits the Consistent Cut procedure on every node in a cluster (by Communication SPI).
        // Then other nodes just skip the message.
        if (!U.isLocalNodeCoordinator(cctx.discovery()))
            return;

        cctx.kernalContext().pools().consistentCutExecutorService().execute(() -> {
            long commitVer;

            if (skipConsistentCut())
                return;

            commitVer = System.currentTimeMillis();

            log.info("Start Consistent Cut for timestamp " + commitVer);

            try {
                Collection<ClusterNode> nodes = cctx.kernalContext().discovery().aliveServerNodes();

                Message msg = new ConsistentCutStartRequest(commitVer);

                cctx.kernalContext().io().sendToGridTopic(nodes, TOPIC_CONSISTENT_CUT, msg, SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to trigger Consistent Cut on remote node.", e);
            }
        });
    }

    /** */
    private boolean skipConsistentCut() {
        ConsistentCutState s = latestCutState;

        if (!s.finished() || !includeNext.isEmpty()) {
            Set<IgniteUuid> w1 = s.checkList().stream().map(GridCacheVersion::asIgniteUuid).collect(Collectors.toSet());
            Set<IgniteUuid> w2 = includeNext.stream().map(GridCacheVersion::asIgniteUuid).collect(Collectors.toSet());

            log.warning("Skip Consistent Cut procedure. This node hasn't finished yet previous one. " +
                "Await=" + w1 + "; Next=" + w2);

            return true;
        }

        return false;
    }

    /**
     * @param cutVer CutVersion to commit with this ConsistentCut.
     */
    private void consistentCut(long cutVer) {
        // Check for duplicated Consistent Cut.
        if (latestCutVer() >= cutVer)
            return;

        ConsistentCutState cutState = new ConsistentCutState(cutVer);

        // Committing transactions aren't part of active transactions.
        Collection<IgniteInternalTx> txs = F.concat(true, cctx.tm().activeTransactions(), committingTxs);

        for (IgniteInternalTx tx : txs) {
            TransactionState txState = tx.state();
            GridCacheVersion txVer = tx.nearXidVersion();

            // Skip fast finish transactions (no write entries).
            if (tx.near() && ((GridNearTxLocal)tx).fastFinish())
                continue;

            long prevCutVer = latestCutVer();

            if (!tx.onePhaseCommit()) {
                if (tx.near()) {
                    // Prepare request may not achieve primary or backup nodes to the moment of local Cut.
                    // Consistent events order is broken. Exclude this tx from this CutVersion.
                    // Back --|----P------------
                    //       CUT  /
                    //           /       CUT
                    // Prim ----P---------|-----
                    if (txState == PREPARING) {
                        cutState.txCutVer(txVer, cutVer);

                        includeNext.add(txVer);
                    }
                    // Transaction prepared on all participated nodes. Every node can track events order.
                    else if (txState == PREPARED) {
                        cutState.txCutVer(txVer, prevCutVer);

                        cutState.include(txVer);
                    }
                    // Transaction is a part of LocalState of CutVer.
                    else if (txState == COMMITTING || txState == COMMITTED)
                        cutState.include(txVer);
                }
                // Primary or Backup nodes.
                else {
                    // To check consistent events order this node requires finish message from near node with CutVer.
                    if (txState == PREPARED)
                        cutState.addForCheck(txVer, prevCutVer);

                    // Transaction is a part of LocalState of CutVer.
                    else if (txState == COMMITTING || txState == COMMITTED)
                        cutState.include(txVer);
                }
            }
            // One phase commit.
            else {
                // For 1PC it uses reverse events order for definition CutVersion (from backup to near).
                if (tx.near() && (txState == PREPARING || txState == PREPARED))
                    cutState.addForCheck(txVer, prevCutVer);

                // Near version uses the same version as primary in this case, otherwise Cut started earlier.
                else if (tx.near() && (txState == COMMITTING || txState == COMMITTED))
                    cutState.include(txVer);

                // Primary node waites for notification from backup node commit CutVersion.
                else if (tx.dht() && tx.local() && (txState == PREPARING || txState == PREPARED))
                    cutState.addForCheck(txVer, prevCutVer);

                // Primary version uses the same version as backup in this case, otherwise Cut started earlier.
                else if (tx.dht() && tx.local() && (txState == COMMITTING || txState == COMMITTED))
                    cutState.include(txVer);

                // Include to LocalState (concurrency: tx state --> WAL cut --> WAL tx state).
                else if (tx.dht() && !tx.local() && txState == COMMITTED)
                    cutState.include(tx.nearXidVersion());

                // Exclude from this CutVersion transactions on backup node of 1PC, and move it to next CutVersion.
                else if (tx.dht() && !tx.local())
                    includeNext.add(tx.nearXidVersion());
            }
        }

        // For cases when node has multiple participations: near and primary or backup.
        cutState.tryFinish();

        // Log Cut before publishing cut state (due to concurrancy with `handleRcvdTxFinishRequest`).
        walLog(cutState.buildStartRecord());

        if (log.isDebugEnabled())
            log.debug("PREPARE CUT " + cutState);

        latestCutState = cutState;
    }

    /**
     * Logs ConsistentCut Finish event.
     */
    private void logConsistentCutFinish(ConsistentCutState s) {
        walLog(s.buildFinishRecord());
    }

    /**
     * Logs ConsistentCutRecord to WAL.
     */
    private void walLog(WALRecord record) {
        BaselineTopology baselineTop;

        if (cctx.wal() == null
            || (baselineTop = cctx.kernalContext().state().clusterState().baselineTopology()) == null
            || !baselineTop.consistentIds().contains(cctx.localNode().consistentId()))
            return;

        try {
            cctx.wal().log(record);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to log ConsistentCutRecord: " + record, e);

            throw new IgniteException("Failed to log ConsistentCutRecord: " + record, e);
        }
    }

    /**
     * Consistent Cut state that initialized on Ignite node start.
     */
    private static class InitialConsistentCutState extends ConsistentCutState {
        /** */
        private InitialConsistentCutState() {
            super(0);

            finish();
        }
    }
}
