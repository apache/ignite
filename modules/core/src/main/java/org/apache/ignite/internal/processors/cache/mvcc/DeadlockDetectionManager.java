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
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxAbstractEnlistFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TX_DEADLOCK_DETECTION_INITIAL_DELAY;
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
    private final long detectionStartDelay = Long.getLong(IGNITE_TX_DEADLOCK_DETECTION_INITIAL_DELAY, 10_000);

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        cctx.gridIO().addMessageListener(TOPIC_DEADLOCK_DETECTION, (nodeId, msg, plc) -> {
            DeadlockProbe msg0 = (DeadlockProbe)msg;

            if (log.isDebugEnabled())
                log.debug("Received a probe message msg=[" + msg0 + ']');

            handleDeadlockProbe(msg0);
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
        if (detectionStartDelay < 0)
            return null;

        if (detectionStartDelay == 0) {
            startComputation(waiterVer, blockerVer);

            return null;
        }

        return new DelayedDeadlockComputation(waiterVer, blockerVer, detectionStartDelay);
    }

    /**
     * Starts a deadlock detection for a given pair of transaction versions (wait-for edge).
     *
     * @param waiterVer Version of the waiting transaction.
     * @param blockerVer Version of the waited for transaction.
     */
    public void startComputation(MvccVersion waiterVer, MvccVersion blockerVer) {
        Optional<IgniteInternalTx> waitingTx = findTx(waiterVer);

        Optional<IgniteInternalTx> blockerTx = findTx(blockerVer);

        if (waitingTx.isPresent() && blockerTx.isPresent()) {
            IgniteInternalTx wTx = waitingTx.get();

            IgniteInternalTx bTx = blockerTx.get();

            sendProbe(
                waitingTx.get().nearXidVersion(),
                // real start time will be filled later when corresponding near node is visited
                Collections.singleton(new ProbedTx(wTx.eventNodeId(), wTx.nearXidVersion(), -1)),
                new ProbedTx(bTx.eventNodeId(), bTx.nearXidVersion(), -1),
                bTx.eventNodeId(),
                true);
        }
    }

    /** */
    private Optional<IgniteInternalTx> findTx(MvccVersion mvccVer) {
        return cctx.tm().activeTransactions().stream()
            .filter(tx -> tx.local() && tx.mvccSnapshot() != null)
            .filter(tx -> belongToSameTx(mvccVer, tx.mvccSnapshot()))
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
        GridNearTxLocal nearTx = cctx.tm().tx(probe.blocker().xidVersion());

        if (nearTx == null)
            return;

        if (probe.waitChain().stream().anyMatch(pTx -> pTx.xidVersion().equals(nearTx.nearXidVersion()))) {
            // a deadlock found
            ProbedTx victim = chooseVictim(nearTx, probe.waitChain());

            if (victim.nodeId().equals(cctx.localNodeId())) {
                // victim can be another tx on the same node
                IgniteInternalTx vTx = cctx.tm().tx(victim.xidVersion());

                if (vTx != null)
                    vTx.rollbackAsync();
            }
            else {
                // t0d0 special message for remote rollback
                sendProbe(probe.initiatorVersion(), Collections.singleton(victim), victim, victim.nodeId(), true);
            }
        }
        else {
            // probe each blocker
            for (UUID pendingNodeId : getPendingResponseNodes(nearTx)) {
                sendProbe(
                    probe.initiatorVersion(),
                    probe.waitChain(),
                    // real start time is filled here
                    new ProbedTx(nearTx.eventNodeId(), nearTx.nearXidVersion(), nearTx.startTime()),
                    pendingNodeId,
                    false);
            }
        }
    }

    /** */
    private void handleDeadlockProbeForDht(DeadlockProbe probe) {
        // a probe is simply discarded if next wait-for edge is not found

        cctx.tm().activeTransactions().stream()
            .filter(IgniteInternalTx::local)
            .filter(tx -> tx.nearXidVersion().equals(probe.blocker().xidVersion()))
            .findAny()
            .ifPresent(tx -> {
                assert tx.mvccSnapshot() != null;

                cctx.coordinators().checkWaiting(tx.mvccSnapshot())
                    .ifPresent(nextBlocker -> {
                        ArrayList<ProbedTx> waitChain = new ArrayList<>(probe.waitChain().size() + 1);
                        waitChain.addAll(probe.waitChain());
                        waitChain.add(probe.blocker());

                        sendProbe(
                            probe.initiatorVersion(),
                            waitChain,
                            // real start time will be filled later when corresponding near node is visited
                            new ProbedTx(nextBlocker.nodeId(), nextBlocker.xidVersion(), -1),
                            nextBlocker.nodeId(),
                            true);
                    });
            });
    }

    /**
     * Chooses victim basing on tx start time. Algorithm chooses victim in such way that every site detected a deadlock
     * will choose the same victim. As a result only one tx participating in a deadlock will be aborted.
     * <p>
     * Near tx is needed here because start time for it might not be filled yet in wait chain.
     *
     * @param nearTx Near tx.
     * @param waitChain Wait chain.
     * @return Tx chosen as a victim.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    private ProbedTx chooseVictim(
        GridNearTxLocal nearTx,
        Collection<ProbedTx> waitChain) {
        // Near tx should be present in a wait chain but start time for it might not be filled yet.
        // So, we use near tx (with real start time) as first victim candidate.
        ProbedTx victim = new ProbedTx(nearTx.eventNodeId(), nearTx.nearXidVersion(), nearTx.startTime());

        long maxStartTime = nearTx.startTime();

        Iterator<ProbedTx> it = waitChain.iterator();

        // skip until near tx (inclusive), because txs before are not deadlocked
        while (it.hasNext() && !it.next().xidVersion().equals(victim.xidVersion()));

        while (it.hasNext()) {
            ProbedTx tx = it.next();

            // seek for youngest tx in order to guarantee forward progress
            if (tx.startTime() > maxStartTime) {
                maxStartTime = tx.startTime();
                victim = tx;
            }
            // tie-breaking
            else if (tx.startTime() == maxStartTime && tx.xidVersion().compareTo(victim.xidVersion()) > 0)
                victim = tx;
        }

        return victim;
    }

    /** */
    private Set<UUID> getPendingResponseNodes(GridNearTxLocal tx) {
        IgniteInternalFuture lockFut = tx.lockFuture();

        if (lockFut instanceof GridNearTxAbstractEnlistFuture)
            return ((GridNearTxAbstractEnlistFuture<?>)lockFut).pendingResponseNodes();

        return Collections.emptySet();
    }

    /** */
    private void sendProbe(GridCacheVersion initiatorVer, Collection<ProbedTx> waitChain,
        ProbedTx blocker, UUID destNodeId, boolean near) {
        DeadlockProbe probe = new DeadlockProbe(initiatorVer, waitChain, blocker, near);

        try {
            cctx.gridIO().sendToGridTopic(destNodeId, TOPIC_DEADLOCK_DETECTION, probe, SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to send a deadlock probe [nodeId=" + destNodeId + ']', e);
        }
    }

    /**
     * Delayed deadlock probe computation which can be cancelled.
     */
    public class DelayedDeadlockComputation {
        /** */
        private final GridTimeoutObject computationTimeoutObj;

        /** */
        private DelayedDeadlockComputation(MvccVersion waiterVer, MvccVersion blockerVer, long timeout) {
            computationTimeoutObj = new GridTimeoutObjectAdapter(timeout) {
                @Override public void onTimeout() {
                    startComputation(waiterVer, blockerVer);
                }
            };

            cctx.kernalContext().timeout().addTimeoutObject(computationTimeoutObj);
        }

        /** */
        public void cancel() {
            cctx.kernalContext().timeout().removeTimeoutObject(computationTimeoutObj);
        }
    }
}
