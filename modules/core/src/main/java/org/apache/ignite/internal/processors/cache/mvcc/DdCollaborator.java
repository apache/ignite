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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxAbstractEnlistFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;

import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE_COORDINATOR;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

// t0d0 meaningful name
// t0d0 javadoc
public class DdCollaborator {
    /** */
    private final GridKernalContext ctx;

    /** */
    public DdCollaborator(GridKernalContext ctx) {
        this.ctx = ctx;
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
        // t0d0 filter out non-mvcc transactions where needed
        Optional<IgniteInternalTx> waitingTx = ctx.cache().context().tm().activeTransactions().stream()
            .filter(tx -> tx.mvccSnapshot() != null)
            .filter(tx -> belongToSameTx(waiterVer, tx.mvccSnapshot()))
            .findAny();

        Optional<IgniteInternalTx> blockerTx = tm().activeTransactions().stream()
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
    public void handleDeadlockProbe(DeadlockProbe probe) {
        // a probe is simply discarded if next wait-for edge is not found
        GridNearTxLocal nearTx = tm().tx(probe.blockerVersion());

        if (nearTx != null) {
            if (nearTx.nearXidVersion().equals(probe.initiatorVersion())) {
                // a deadlock found
                // t0d0 indicate that deadlock caused transaction abort
                nearTx.rollbackAsync();
            }
            else {
                // probe each blocker
                // t0d0 check if holding some lock already
                // t0d0 first find all peers then send messages
                // t0d0 consider grouping (only if it will lead to correct results!)
                collectBlockers(nearTx).forEach(fut -> {
                    fut.listen(fut0 -> {
                        try {
                            NearTxLocator blockerTx = fut.get();

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
            .map(nodeId -> ctx.cache().context().coordinators().checkWaiting(nodeId, tx.mvccSnapshot()))
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
    private void sendProbe(
        GridCacheVersion initiatorVer, GridCacheVersion waiterVer, GridCacheVersion blockerVer, UUID blockerNearNodeId) {
        // t0d0 review if message order is important here
        // t0d0 PROPER TOPIC
        DeadlockProbe probe = new DeadlockProbe(initiatorVer, waiterVer, blockerVer);
        try {
            ctx.io().sendToGridTopic(blockerNearNodeId, TOPIC_CACHE_COORDINATOR, probe, SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            // t0d0 handle send errors
            e.printStackTrace();
        }
    }

    /** */
    private IgniteTxManager tm() {
        return ctx.cache().context().tm();
    }

    /** */
    public static boolean belongToSameTx(MvccVersion v1, MvccVersion v2) {
        return v1.coordinatorVersion() == v2.coordinatorVersion() && v1.counter() == v2.counter();
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

            ctx.timeout().addTimeoutObject(computationTimeoutObj);
        }

        /** */
        public void cancel() {
            ctx.timeout().removeTimeoutObject(computationTimeoutObj);
        }
    }
}
