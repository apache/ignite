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

import java.util.Collections;
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
 * Component participating in deadlock detection in a cluseter. Detection process is collaborative and it is performed
 * by relaying special probe messages from waiting transaction to it's blocker.
 * <p>
 * Ideas for used detection algorithm are borrowed from Chandy-Misra-Haas deadlock detection algorithm for resource
 * model.
 * <p>
 * Current implementation assumes that transactions obeys 2PL.
 */
public class DeadlockDetectionManager extends GridCacheSharedManagerAdapter {
    /** */
    private final long detectionStartDelay = Long.getLong(IGNITE_TX_DEADLOCK_DETECTION_INITIAL_DELAY, 500);

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
        Optional<IgniteInternalTx> waitingTx = cctx.tm().activeTransactions().stream()
            .filter(tx -> tx.local() && tx.mvccSnapshot() != null)
            .filter(tx -> belongToSameTx(waiterVer, tx.mvccSnapshot()))
            .findAny();

        Optional<IgniteInternalTx> blockerTx = cctx.tm().activeTransactions().stream()
            .filter(tx -> tx.local() && tx.mvccSnapshot() != null)
            .filter(tx -> belongToSameTx(blockerVer, tx.mvccSnapshot()))
            .findAny();

        if (waitingTx.isPresent() && blockerTx.isPresent()) {
            sendProbe(
                waitingTx.get().nearXidVersion(),
                waitingTx.get().nearXidVersion(),
                blockerTx.get().nearXidVersion(),
                blockerTx.get().eventNodeId(),
                true);
        }
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
        GridNearTxLocal nearTx = cctx.tm().tx(probe.blockerVersion());

        if (nearTx == null)
            return;

        if (nearTx.nearXidVersion().equals(probe.initiatorVersion())) {
            // a deadlock found
            nearTx.rollbackAsync();
        }
        else {
            // probe each blocker
            for (UUID pendingNodeId : getPendingResponseNodes(nearTx)) {
                sendProbe(
                    probe.initiatorVersion(),
                    nearTx.nearXidVersion(),
                    nearTx.nearXidVersion(),
                    pendingNodeId,
                    false);
            }
        }
    }

    /** */
    private void handleDeadlockProbeForDht(DeadlockProbe probe) {
        assert probe.waitingVersion().equals(probe.blockerVersion());

        // a probe is simply discarded if next wait-for edge is not found

        cctx.tm().activeTransactions().stream()
            .filter(IgniteInternalTx::local)
            .filter(tx -> tx.nearXidVersion().equals(probe.blockerVersion()))
            .findAny()
            .flatMap(tx -> cctx.coordinators().checkWaiting(tx.mvccSnapshot()))
            .ifPresent(locator -> sendProbe(
                probe.initiatorVersion(),
                probe.blockerVersion(),
                locator.xidVersion(),
                locator.nodeId(),
                true
            ));
    }

    /** */
    private Set<UUID> getPendingResponseNodes(GridNearTxLocal tx) {
        IgniteInternalFuture lockFut = tx.lockFuture();

        if (lockFut instanceof GridNearTxAbstractEnlistFuture)
            return ((GridNearTxAbstractEnlistFuture<?>)lockFut).pendingResponseNodes();

        return Collections.emptySet();
    }

    /** */
    private void sendProbe(GridCacheVersion initiatorVer, GridCacheVersion waiterVer, GridCacheVersion blockerVer,
        UUID blockerNearNodeId, boolean near) {
        DeadlockProbe probe = new DeadlockProbe(initiatorVer, waiterVer, blockerVer, near);
        try {
            cctx.gridIO().sendToGridTopic(blockerNearNodeId, TOPIC_DEADLOCK_DETECTION, probe, SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to send a deadlock probe [nodeId=" + blockerNearNodeId + ']', e);
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
