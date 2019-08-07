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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxAbstractEnlistFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;

import static java.util.Collections.singleton;
import static org.apache.ignite.internal.GridTopic.TOPIC_DEADLOCK_DETECTION;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.belongToSameTx;

/**
 * Component participating in deadlock detection in a cluster. Detection process is collaborative and it is performed
 * by relaying special probe messages from waiting transaction to it's blocker.
 * <p>
 * Ideas for used detection algorithm are borrowed from Chandy-Misra-Haas deadlock detection algorithm for resource
 * model.
 * <p>
 * Current implementation assumes that transactions obeys 2PL.
 */
public class DeadlockDetectionManager extends GridCacheSharedManagerAdapter {
    /** */
    private long detectionStartDelay;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        detectionStartDelay = cctx.kernalContext().config().getTransactionConfiguration().getDeadlockTimeout();

        cctx.gridIO().addMessageListener(TOPIC_DEADLOCK_DETECTION, (nodeId, msg, plc) -> {
            if (msg instanceof DeadlockProbe) {
                if (log.isDebugEnabled())
                    log.debug("Received a probe message [msg=" + msg + ']');

                DeadlockProbe msg0 = (DeadlockProbe)msg;

                handleDeadlockProbe(msg0);
            }
            else
                log.warning("Unexpected message received [node=" + nodeId + ", msg=" + msg + ']');
        });
    }

    /**
     * Starts a dedlock detection after a delay.
     *
     * @param waiterVer Version of the waiting transaction.
     * @param blockerVer Version of the waited for transaction.
     * @return Cancellable computation.
     */
    public DelayedDeadlockComputation initDelayedComputation(MvccVersion waiterVer, MvccVersion blockerVer) {
        if (detectionStartDelay <= 0)
            return null;

        return new DelayedDeadlockComputation(waiterVer, blockerVer, detectionStartDelay);
    }

    /**
     * Starts a deadlock detection for a given pair of transaction versions (wait-for edge).
     *
     * @param waiterVer Version of the waiting transaction.
     * @param blockerVer Version of the waited for transaction.
     */
    private void startComputation(MvccVersion waiterVer, MvccVersion blockerVer) {
        if (log.isDebugEnabled())
            log.debug("Starting deadlock detection [waiterVer=" + waiterVer + ", blockerVer=" + blockerVer + ']');

        Optional<GridDhtTxLocalAdapter> waitingTx = findTx(waiterVer);

        Optional<GridDhtTxLocalAdapter> blockerTx = findTx(blockerVer);

        if (waitingTx.isPresent() && blockerTx.isPresent()) {
            GridDhtTxLocalAdapter wTx = waitingTx.get();

            GridDhtTxLocalAdapter bTx = blockerTx.get();

            sendProbe(
                bTx.eventNodeId(),
                wTx.xidVersion(),
                // real start time will be filled later when corresponding near node is visited
                singleton(new ProbedTx(wTx.nodeId(), wTx.xidVersion(), wTx.nearXidVersion(), -1, wTx.lockCounter())),
                new ProbedTx(bTx.nodeId(), bTx.xidVersion(), bTx.nearXidVersion(), -1, bTx.lockCounter()),
                true);
        }
    }

    /** */
    private Optional<GridDhtTxLocalAdapter> findTx(MvccVersion mvccVer) {
        return cctx.tm().activeTransactions().stream()
            .filter(tx -> tx.local() && tx.mvccSnapshot() != null)
            .filter(tx -> belongToSameTx(mvccVer, tx.mvccSnapshot()))
            .map(GridDhtTxLocalAdapter.class::cast)
            .findAny();
    }

    /**
     * Handles received deadlock probe. Possible outcomes:
     * <ol>
     *     <li>Deadlock is found.</li>
     *     <li>Probe is relayed to other blocking transactions.</li>
     *     <li>Probe is discarded because receiving transaction is not blocked.</li>
     * </ol>
     *
     * @param probe Received probe message.
     */
    private void handleDeadlockProbe(DeadlockProbe probe) {
        if (probe.nearCheck())
            handleDeadlockProbeForNear(probe);
        else
            handleDeadlockProbeForDht(probe);
    }

    /** */
    private void handleDeadlockProbeForNear(DeadlockProbe probe) {
        // a probe is simply discarded if next wait-for edge is not found
        ProbedTx blocker = probe.blocker();

        GridNearTxLocal nearTx = cctx.tm().tx(blocker.nearXidVersion());

        if (nearTx == null)
            return;

        // probe each blocker
        for (UUID pendingNodeId : getPendingResponseNodes(nearTx)) {
            sendProbe(
                pendingNodeId,
                probe.initiatorVersion(),
                probe.waitChain(),
                // real start time is filled here
                blocker.withStartTime(nearTx.startTime()),
                false);
        }
    }

    /** */
    private void handleDeadlockProbeForDht(DeadlockProbe probe) {
        // a probe is simply discarded if next wait-for edge is not found
        cctx.tm().activeTransactions().stream()
            .filter(IgniteInternalTx::local)
            .filter(tx -> tx.nearXidVersion().equals(probe.blocker().nearXidVersion()))
            .findAny()
            .map(GridDhtTxLocalAdapter.class::cast)
            .ifPresent(tx -> {
                // search for locally checked tx (identified as blocker previously) in the wait chain
                Optional<ProbedTx> repeatedTx = probe.waitChain().stream()
                    .filter(wTx -> wTx.xidVersion().equals(tx.xidVersion()))
                    .findAny();

                if (repeatedTx.isPresent()) {
                    // a deadlock found
                    resolveDeadlock(probe, repeatedTx.get(), tx);
                }
                else
                    relayProbeIfLocalTxIsWaiting(probe, tx);
            });
    }

    /** */
    private void resolveDeadlock(DeadlockProbe probe, ProbedTx repeatedTx, GridDhtTxLocalAdapter locTx) {
        if (log.isDebugEnabled())
            log.debug("Deadlock detected [probe=" + probe + ']');

        ProbedTx victim = chooseVictim(
            // real start time is filled here for repeated tx
            repeatedTx.withStartTime(probe.blocker().startTime()),
            probe.waitChain());

        if (victim.xidVersion().equals(locTx.xidVersion())) {
            if (log.isDebugEnabled())
                log.debug("Chosen victim is on local node, tx will be aborted [victim=" + victim + ']');

            // if a victim tx has made a progress since it was identified as waiting
            // it means that detected deadlock was broken by other means (e.g. timeout of another tx)
            if (victim.lockCounter() == locTx.lockCounter())
                abortTx(locTx);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Chosen victim is on remote node, message will be sent [victim=" + victim + ']');

            // destination node must determine itself as a victim
            sendProbe(victim.nodeId(), probe.initiatorVersion(), singleton(victim), victim, false);
        }
    }

    /** */
    private void relayProbeIfLocalTxIsWaiting(DeadlockProbe probe, GridDhtTxLocalAdapter locTx) {
        assert locTx.mvccSnapshot() != null;

        cctx.coordinators().checkWaiting(locTx.mvccSnapshot())
            .flatMap(this::findTx)
            .ifPresent(nextBlocker -> {
                ArrayList<ProbedTx> waitChain = new ArrayList<>(probe.waitChain().size() + 1);
                waitChain.addAll(probe.waitChain());
                // real start time is filled here
                waitChain.add(new ProbedTx(locTx.nodeId(), locTx.xidVersion(), locTx.nearXidVersion(),
                    probe.blocker().startTime(), locTx.lockCounter()));

                // real start time will be filled later when corresponding near node is visited
                ProbedTx nextProbedTx = new ProbedTx(nextBlocker.nodeId(), nextBlocker.xidVersion(),
                    nextBlocker.nearXidVersion(), -1, nextBlocker.lockCounter());

                sendProbe(
                    nextBlocker.eventNodeId(),
                    probe.initiatorVersion(),
                    waitChain,
                    nextProbedTx,
                    true);
            });
    }

    /**
     * Chooses victim basing on tx start time. Algorithm chooses victim in such way that every site detected a deadlock
     * will choose the same victim. As a result only one tx participating in a deadlock will be aborted.
     * <p>
     * Local tx is needed here because start time for it might not be filled yet for corresponding entry in wait chain.
     *
     * @param locTx Deadlocked tx on local node.
     * @param waitChain Wait chain.
     * @return Tx chosen as a victim.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    private ProbedTx chooseVictim(ProbedTx locTx, Collection<ProbedTx> waitChain) {
        Iterator<ProbedTx> it = waitChain.iterator();

        // skip until local tx (inclusive), because txs before are not deadlocked
        while (it.hasNext() && !it.next().xidVersion().equals(locTx.xidVersion()));

        ProbedTx victim = locTx;
        long maxStartTime = locTx.startTime();

        while (it.hasNext()) {
            ProbedTx tx = it.next();

            // seek for youngest tx in order to guarantee forward progress
            if (tx.startTime() > maxStartTime) {
                maxStartTime = tx.startTime();
                victim = tx;
            }
            // tie-breaking
            else if (tx.startTime() == maxStartTime && tx.nearXidVersion().compareTo(victim.nearXidVersion()) > 0)
                victim = tx;
        }

        return victim;
    }

    /** */
    private void abortTx(GridDhtTxLocalAdapter tx) {
        cctx.coordinators().failWaiter(tx.mvccSnapshot(), new IgniteTxRollbackCheckedException(
            "Deadlock detected. Transaction will be rolled back [tx=" + tx + ']'));
    }

    /** */
    private Set<UUID> getPendingResponseNodes(GridNearTxLocal tx) {
        IgniteInternalFuture lockFut = tx.lockFuture();

        if (lockFut instanceof GridNearTxAbstractEnlistFuture)
            return ((GridNearTxAbstractEnlistFuture<?>)lockFut).pendingResponseNodes();

        return Collections.emptySet();
    }

    /** */
    private void sendProbe(UUID destNodeId, GridCacheVersion initiatorVer, Collection<ProbedTx> waitChain,
        ProbedTx blocker, boolean near) {

        DeadlockProbe probe = new DeadlockProbe(initiatorVer, waitChain, blocker, near);

        if (log.isDebugEnabled())
            log.debug("Sending probe [probe=" + probe + ", destNode=" + destNodeId + ']');

        try {
            cctx.gridIO().sendToGridTopic(destNodeId, TOPIC_DEADLOCK_DETECTION, probe, SYSTEM_POOL);
        }
        catch (ClusterTopologyCheckedException ignored) {
        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to send a deadlock probe [nodeId=" + destNodeId + ']', e);
        }
    }

    /**
     * Delayed deadlock probe computation which can be cancelled.
     */
    public class DelayedDeadlockComputation extends GridTimeoutObjectAdapter {
        /** */
        private final MvccVersion waiterVer;

        /** */
        private final MvccVersion blockerVer;

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            startComputation(waiterVer, blockerVer);
        }

        /** */
        private DelayedDeadlockComputation(MvccVersion waiterVer, MvccVersion blockerVer, long timeout) {
            super(timeout);
            this.waiterVer = waiterVer;
            this.blockerVer = blockerVer;

            cctx.kernalContext().timeout().addTimeoutObject(this);
        }

        /** */
        public void cancel() {
            cctx.kernalContext().timeout().removeTimeoutObject(this);
        }
    }
}
