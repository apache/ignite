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
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutRecord;
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
     * Mutable state of last Consistent Cut.
     */
    private volatile ConsistentCutState lastCutState;

    /**
     * Collection of transactions in COMMITTING state.
     */
    private final Set<IgniteInternalTx> committingTxs = ConcurrentHashMap.newKeySet();

    /**
     * Guards {@link #lastCutState}. When one thread updates the state, other threads with messages may change it.
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

        cctx.discovery().setCustomEventListener(ConsistentCutCheckDiscoveryMessage.class, (topVer, snd, m) ->
            handleConsistentCutCheckEvent(m)
        );

        cctx.discovery().setCustomEventListener(ConsistentCutStartDiscoveryMessage.class, (topVer, snd, m) ->
            handleConsistentCutStartEvent()
        );

        cctx.kernalContext().io().addMessageListener(TOPIC_CONSISTENT_CUT, (nodeId, msg, plc) ->
            cctx.kernalContext().pools().consistentCutExecutorService().execute(() ->
                cctx.consistentCutMgr().handleConsistentCutStart(((ConsistentCutStartRequest)msg).cutVer())
            )
        );
    }

    /**
     * IgniteTxHandler#activeTransactions doesn't track transactions in COMMITTING state. But those transactions
     * are required to be tracked for ConsistentCut algorithm. Then store them in a separate collection.
     */
    public void beforeCommit(IgniteInternalTx tx) {
        committingTxs.add(tx);
    }

    /**
     * Cleans collections of transactions in COMMITTING state after handling them.
     */
    public void cleanCommitting(IgniteInternalTx tx) {
        committingTxs.remove(tx);

        // Nead lock here due to cases when tx is in PREPARED and there is concurrency between CUT and rcv FinishRequest.
        // We have to wait here finishing of ConsistentCut to avoid cases when tx holds in `cutAwait`.
        cutGuard.readLock().lock();

        try {
            ConsistentCutState s = lastCutState();

            if (s == null || s.completed())
                return;

            log.info("`cleanCommitting` " + s.ver() + " " + tx.nearXidVersion().asIgniteUuid() + " " + s.cutAwait() + " " +
                tx.onePhaseCommit() + " " + tx.dht());

            if (!s.completed())
                s.complete(tx.nearXidVersion());

            // In some cases there is a concurrency between TX states and the ConsistentCut procedure.
            // Transaction was included to `cutAwait` (CC procedure) but already had received message that allows to commit it.
            // Then we just verify transactions in moment of commit.
            // - 1PC + (near or primary node).
            // - 2PC + (primary or backup).
            if ((tx.onePhaseCommit() && tx.local()) || (!tx.onePhaseCommit() && tx.dht())) {
                if (s.cutAwait().remove(tx.nearXidVersion()) != null && s.cutAwait().isEmpty() && s.complete()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Log ConsistentCutFinish(ver=" + s.ver() + ") on `addCommitted`" +
                            " for tx " + tx.nearXidVersion().asIgniteUuid());
                    }

                    logConsistentCutFinish(s.ver(), s.checkInclude());
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
    public long lastCutVer() {
        ConsistentCutState s = lastCutState;

        return s == null ? 0 : s.ver();
    }

    /**
     * @return Last CutVersion.
     */
    private ConsistentCutState lastCutState() {
//        cutGuard.readLock().lock();

        try {
            return lastCutState;
        }
        finally {
//            cutGuard.readLock().unlock();
        }
    }

    /**
     * Checks local CutVersion and run ConsistentCut if version has changed.
     *
     * @param cutVer Received CutVersion from different node.
     */
    public void handleConsistentCutStart(long cutVer) {
        // Already handled this version.
        if (lastCutVer() >= cutVer)
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
        ConsistentCutState s = lastCutState();

        if (s == null || s.completed())
            return;

        log.info("`handleRemoteTxCutVersion` " + s.ver() + " " + " " + s.cutAwait() + " " +
            nearTxId.asIgniteUuid() + " " + txLastCutVer + " " + onePhaseCommit);

        if (s.cutAwait().containsKey(nearTxId)) {
            Long prepVer = s.cutAwait().remove(nearTxId);

            if (prepVer != null) {
                if (txLastCutVer >= s.ver() && txLastCutVer > prepVer)
                    s.exclude(nearTxId);
            }
            else if (txLastCutVer >= s.ver())
                s.exclude(nearTxId);

            if (s.cutAwait().isEmpty() && s.complete()) {
                if (log.isDebugEnabled()) {
                    log.debug("Log ConsistentCutFinish(ver=" + s.ver() + ") on `handleRemoteTxCutVersion`" +
                        " for tx " + nearTxId.asIgniteUuid());
                }

                logConsistentCutFinish(s.ver(), s.checkInclude());
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
            ConsistentCutState s = lastCutState();

            if (s == null)
                return 0L;

            if (tx.near() && !tx.onePhaseCommit()) {
                if (!s.include().isEmpty() && s.include().contains(tx.nearXidVersion()))
                    return s.ver() - 1;

                if (!s.nearPrepare().isEmpty()) {
                    Long v = s.nearPrepare().remove(tx.nearXidVersion());

                    if (v != null)
                        return v;
                }
            }
            // Shows that this 1PC will be included to PREVIOUS cut (decrement lastCutVer to show that we need prev one).
            else if (tx.dht() && !tx.local() && tx.onePhaseCommit() && s.include().contains(tx.nearXidVersion()))
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
        ConsistentCutState s = lastCutState();

        if (s != null && !s.completed()) {
            Set<IgniteUuid> w1 = s.cutAwait().keySet().stream().map(GridCacheVersion::asIgniteUuid).collect(Collectors.toSet());
            Set<IgniteUuid> w2 = s.includeNext().stream().map(GridCacheVersion::asIgniteUuid).collect(Collectors.toSet());

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
        ConsistentCutState lastState = lastCutState;

        if (lastState != null && lastState.ver() >= cutVer)
            return;

        ConsistentCutState s = new ConsistentCutState(cutVer);

        // Committing transactions aren't part of active transactions.
        Collection<IgniteInternalTx> txs = F.concat(true, cctx.tm().activeTransactions(), committingTxs);

        for (IgniteInternalTx tx : txs) {
            TransactionState txState = tx.state();
            GridCacheVersion txVer = tx.nearXidVersion();

            // Skip fast finish transactions (no write entries).
            if (tx.near() && ((GridNearTxLocal)tx).fastFinish())
                continue;

            long prevCutVer = lastCutVer();

            if (!tx.onePhaseCommit()) {
                if (tx.near()) {
                    // Prepare request may not achieve primary or backup nodes to the moment of local Cut.
                    // Consistent events order is broken. Exclude this tx from this CutVersion.
                    // Back --|----P------------
                    //       CUT  /
                    //           /       CUT
                    // Prim ----P---------|-----
                    if (txState == PREPARING) {
                        s.nearPrepare(txVer, cutVer);

                        s.includeNext(txVer);
                    }
                    // Transaction prepared on all participated nodes. Every node can track events order.
                    else if (txState == PREPARED) {
                        s.nearPrepare(txVer, prevCutVer);

                        s.include(txVer);
                    }
                    // Transaction is a part of LocalState of CutVer.
                    else if (txState == COMMITTING || txState == COMMITTED)
                        s.include(txVer);
                }
                // Primary or Backup nodes.
                else {
                    // To check consistent events order this node requires finish message from near node with CutVer.
                    if (txState == PREPARED)
                        s.cutAwait(txVer, prevCutVer);

                    // Transaction is a part of LocalState of CutVer.
                    else if (txState == COMMITTING || txState == COMMITTED)
                        s.include(txVer);
                }
            }
            // One phase commit.
            else {
                // For 1PC it uses reverse events order for definition CutVersion (from backup to near).
                if (tx.near() && (txState == PREPARING || txState == PREPARED))
                    s.cutAwait(txVer, prevCutVer);

                // Near version uses the same version as primary in this case, otherwise Cut started earlier.
                else if (tx.near() && (txState == COMMITTING || txState == COMMITTED))
                    s.include(txVer);

                // Primary node waites for notification from backup node commit CutVersion.
                else if (tx.dht() && tx.local() && (txState == PREPARING || txState == PREPARED))
                    s.cutAwait(txVer, prevCutVer);

                // Primary version uses the same version as backup in this case, otherwise Cut started earlier.
                else if (tx.dht() && tx.local() && (txState == COMMITTING || txState == COMMITTED))
                    s.include(txVer);

                // Include to LocalState (concurrency: tx state --> WAL cut --> WAL tx state).
                else if (tx.dht() && !tx.local() && txState == COMMITTED)
                    s.include(tx.nearXidVersion());

                // Exclude from this CutVersion transactions on backup node of 1PC, and move it to next CutVersion.
                else if (tx.dht() && !tx.local())
                    s.includeNext(tx.nearXidVersion());
            }
        }

        // For cases when node has multiple participations: near and primary or backup.
        Iterator<GridCacheVersion> it = s.include().iterator();

        while (it.hasNext()) {
            GridCacheVersion tx = it.next();

            if (s.nearPrepare().containsKey(tx) && s.cutAwait().containsKey(tx)) {
                if (s.nearPrepare().get(tx) > s.cutAwait().get(tx)) {
                    s.cutAwait().remove(tx);

                    it.remove();
                }
            }
        }

        // Log Cut before publishing cut state (due to concurrancy with `handleRcvdTxFinishRequest`).
        logConsistentCutStart(s);

        log.info("PREPARE CUT " + s);

        lastCutState = s;
    }

    /**
     * Logs ConsistentCut Start event.
     *
     * @param s Consistent Cut State.
     */
    private void logConsistentCutStart(ConsistentCutState s) {
        ConsistentCutRecord record = new ConsistentCutRecord(s.ver(), s.include(), s.cutAwait().keySet(), false);

        walLog(record);

        if (s.cutAwait().isEmpty())
            s.complete();
    }

    /**
     * Logs ConsistentCut Finish event.
     *
     * @param ver Consistent Cut Version.
     * @param include Transactions to include to this CutVersion after check them.
     */
    private void logConsistentCutFinish(long ver, Set<GridCacheVersion> include) {
        ConsistentCutRecord record = new ConsistentCutRecord(ver, include, Collections.emptySet(), true);

        walLog(record);
    }

    /**
     * Logs ConsistentCutRecord to WAL.
     */
    private void walLog(ConsistentCutRecord record) {
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
}
