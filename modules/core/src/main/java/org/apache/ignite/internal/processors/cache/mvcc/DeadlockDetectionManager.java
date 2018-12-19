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

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxAbstractEnlistFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;

import static org.apache.ignite.internal.GridTopic.TOPIC_DEADLOCK_DETECTION;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.belongToSameTx;

// t0d0 javadoc
public class DeadlockDetectionManager extends GridCacheSharedManagerAdapter {
    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        cctx.gridIO().addMessageListener(TOPIC_DEADLOCK_DETECTION, (nodeId, msg, plc) -> {
            handleDeadlockProbe(((DeadlockProbe)msg));
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
        return new DelayedDeadlockComputation(waiterVer, blockerVer, 500);
    }

    /**
     * Starts a deadlock detection for a given pair of transaction versions (wait-for edge).
     *
     * @param waiterVer Version of the waiting transaction.
     * @param blockerVer Version of the waited for transaction.
     */
    public void startComputation(MvccVersion waiterVer, MvccVersion blockerVer) {
        Optional<IgniteInternalTx> waitingTx = cctx.tm().activeTransactions().stream()
            .filter(tx -> tx.mvccSnapshot() != null)
            .filter(tx -> belongToSameTx(waiterVer, tx.mvccSnapshot()))
            .findAny();

        Optional<IgniteInternalTx> blockerTx = cctx.tm().activeTransactions().stream()
            .filter(tx -> tx.mvccSnapshot() != null)
            .filter(tx -> belongToSameTx(blockerVer, tx.mvccSnapshot()))
            .findAny();

        if (waitingTx.isPresent() && blockerTx.isPresent()) {
            sendProbe(
                waitingTx.get().nearXidVersion(),
                waitingTx.get().nearXidVersion(),
                blockerTx.get().nearXidVersion(),
                blockerTx.get().eventNodeId()
            );
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
        // a probe is simply discarded if next wait-for edge is not found
        GridNearTxLocal nearTx = cctx.tm().tx(probe.blockerVersion());

        if (nearTx != null) {
            if (nearTx.nearXidVersion().equals(probe.initiatorVersion())) {
                // a deadlock found
                // t0d0 indicate that deadlock caused transaction abort
                nearTx.rollbackAsync();
            }
            else {
                // probe each blocker
                // t0d0 check if holds some lock already
                collectBlockers(nearTx).forEach(fut -> {
                    fut.listen(fut0 -> {
                        try {
                            NearTxLocator blockerTx = fut0.get();

                            if (blockerTx != null) {
                                sendProbe(
                                    probe.initiatorVersion(),
                                    nearTx.nearXidVersion(),
                                    blockerTx.xidVersion(),
                                    blockerTx.nodeId());
                            }
                        }
                        catch (IgniteCheckedException e) {
                            e.printStackTrace();
                        }
                    });
                });
            }
        }
    }

    /** */
    private Collection<IgniteInternalFuture<NearTxLocator>> collectBlockers(GridNearTxLocal tx) {
        return getPendingResponseNodes(tx).stream()
            .map(nodeId -> cctx.coordinators().checkWaiting(nodeId, tx.mvccSnapshot()))
            .collect(Collectors.toList());
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
        UUID blockerNearNodeId) {
        DeadlockProbe probe = new DeadlockProbe(initiatorVer, waiterVer, blockerVer);
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
