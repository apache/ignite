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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareFutureAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.baselineNode;

/**
 * Processes all stuff related to Consistent Cut.
 * <p>
 * ConsistentCut is a distributed algorithm that splits WAL on 2 global areas - BEFORE and AFTER. It guarantees that every
 * transaction committed Before also will be committed Before on every other node participated in the transaction.
 * It means that an Ignite nodes can safely recover themselves to the consistent BEFORE state without any coordination with each other.
 * <p>
 * The algorithm starts on Ignite node by snapshot creation command. Other nodes are notified with discovery message of snapshot
 * distributed process or by transaction messages {@link ConsistentCutAwareMessage}.
 * <p>
 * The algorithm consist of steps:
 * 1. On receiving new Consistent Cut ID it immediately creates new {@link ConsistentCut} before processing the message.
 * 2. It starts wrapping all transaction messages to {@link ConsistentCutAwareMessage}.
 * 3. Every transaction holds {@link IgniteTxAdapter#cutId()} AFTER which it committed. Value of this field is defined
 *    at node that commits first in distributed transaction.
 * 3. On baseline nodes:
 *    - it writes {@link ConsistentCutStartRecord} to limit amount of transactions on the AFTER side of Consistent Cut.
 *      After writing this record it's safe to miss transactions on the AFTER side.
 *    - it collects active transactions to check which side of Consistent Cut they belong to. This collection contains all
 *      not-committed yet transactions that are on the BEFORE side.
 *    - it awaits every transaction in this collection to be committed to decide which side of Consistent Cut they belong to.
 *    - after the all active transactions finished it finishes Consistent Cut with writing {@link ConsistentCutFinishRecord} that contains
 *      collection of transactions on the BEFORE and AFTER sides.
 * 8. After Consistent Cut finished globally, it clears {@link ConsistentCut} variable and stops wrapping messages.
 */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter implements PartitionsExchangeAware {
    /** It serves updates of {@link #cut} with CAS. */
    protected static final AtomicReferenceFieldUpdater<ConsistentCutManager, ConsistentCut> CONSISTENT_CUT =
        AtomicReferenceFieldUpdater.newUpdater(ConsistentCutManager.class, ConsistentCut.class, "cut");

    /** Current Consistent Cut, {@code null} if not running. */
    private volatile @Nullable ConsistentCut cut;

    /** ID of the last finished {@link ConsistentCut}. Required to avoid re-run {@link ConsistentCut} with the same id. */
    protected volatile UUID lastFinishedCutId;

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        super.start0();

        cctx.exchange().registerExchangeAwareComponent(this);
    }

    /** {@inheritDoc} */
    @Override public void stop0(boolean cancel) {
        cancelCut(new IgniteCheckedException("Ignite node is stopping."));
    }

    /**
     * Stops Consistent Cut in case of baseline topology changed.
     */
    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        if (fut.changedBaseline() || fut.isBaselineNodeFailed())
            cancelCut(new IgniteCheckedException("Ignite topology changed, can't finish Consistent Cut."));
    }

    /**
     * Registers transaction before it starts committing.
     *
     * @param tx Transaction.
     * @param firstCommit If {@code true} then it is first commit for distributed transaction.
     */
    public void registerBeforeCommit(IgniteInternalTx tx, boolean firstCommit) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut != null) {
            if (firstCommit)
                tx.cutId(cut.id());

            cut.addActiveTransaction(tx.finishFuture());
        }

        if (log.isDebugEnabled()) {
            log.debug("`registerBeforeCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + " , txMarker=" + tx.cutId() + ", cutId = " + (cut == null ? null : cut.id()));
        }
    }

    /**
     * Wraps a transaction message if Consistent Cut is running.
     *
     * @param txMsg Transaction message to wrap.
     * @param txCutId Consistent Cut ID after which transaction committed, if specified.
     */
    public GridCacheMessage wrapMessage(
        GridCacheMessage txMsg,
        @Nullable UUID txCutId
    ) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut != null)
            return new ConsistentCutAwareMessage(txMsg, cut.id(), txCutId);

        return txMsg;
    }

    /**
     * Handles received Consistent Cut ID from remote node. It compares it with the latest ID that local node is aware of.
     * Init local Consistent Cut procedure if received ID is a new one.
     *
     * @param id ID of {@link ConsistentCut}.
     */
    public void handleConsistentCutId(UUID id) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut == null && !id.equals(lastFinishedCutId)) {
            ConsistentCut newCut = newConsistentCut(id);

            if (CONSISTENT_CUT.compareAndSet(this, cut, newCut)) {
                if (cctx.kernalContext().clientNode() ||
                    !baselineNode(cctx.localNode(), cctx.kernalContext().state().clusterState())
                ) {
                    newCut.onDone();

                    return;
                }

                cctx.kernalContext().pools().getSnapshotExecutorService().submit(() -> {
                    try {
                        newCut.init();

                        if (log.isDebugEnabled())
                            log.debug("Prepared Consistent Cut: " + newCut);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to handle Consistent Cut version.", e);

                        newCut.onDone(e);
                    }
                });
            }
        }
    }

    /** Creates new Consistent Cut instance. */
    protected ConsistentCut newConsistentCut(UUID id) {
        return new ConsistentCut(cctx, id);
    }

    /**
     * Cancels local Consistent Cut with error.
     *
     * @param err Error.
     */
    public void cancelCut(Throwable err) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut != null && !cut.isDone())
            cut.onDone(err);

        cleanLocalCut();
    }

    /**
     * @return Current running Consistent Cut, if cut isn't running then {@code null}.
     */
    public @Nullable ConsistentCut cut() {
        return CONSISTENT_CUT.get(this);
    }

    /**
     * Cleans local Consistent Cut, stop signing outgoing messages.
     */
    public void cleanLocalCut() {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (log.isDebugEnabled())
            log.debug("`finishLocalCut` for " + cut);

        if (cut == null)
            return;

        lastFinishedCutId = cut.id();

        CONSISTENT_CUT.set(this, null);
    }
}
