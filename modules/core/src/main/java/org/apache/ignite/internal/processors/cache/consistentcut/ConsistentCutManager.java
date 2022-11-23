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

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.baselineNode;

/**
 * Processes all stuff related to Consistent Cut.
 * <p>
 * Consistent Cut is a distributed algorithm that defines two set of transactions - BEFORE and AFTER cut. It guarantees that every
 * transaction committed BEFORE also will be committed BEFORE on every other node participated in the transaction.
 * It means that an Ignite nodes can safely recover themselves to the consistent BEFORE state without any coordination with each other.
 * <p>
 * The algorithm starts on Ignite node by snapshot creation command. Other nodes are notified with discovery message of snapshot
 * distributed process or by transaction messages {@link ConsistentCutAwareMessage}.
 * <p>
 * The algorithm consist of steps:
 * 1. On receiving new Consistent Cut ID it immediately creates new {@link ConsistentCutFuture} before processing the message.
 * 2. It starts wrapping all transaction messages to {@link ConsistentCutAwareMessage}.
 * 3. Every transaction holds {@link IgniteTxAdapter#cutId()} AFTER which it committed. Value of this field is defined
 *    at node that commits first in distributed transaction.
 * 4. On baseline nodes in {@link BaselineConsistentCutFuture}:
 *    - it writes {@link ConsistentCutStartRecord} to limit amount of transactions on the AFTER side of Consistent Cut.
 *      After writing this record it's safe to miss transactions on the AFTER side.
 *    - it collects active transactions to check which side of Consistent Cut they belong to. This collection contains all
 *      not-committed yet transactions that are on the BEFORE side.
 *    - it awaits every transaction in this collection to be committed to decide which side of Consistent Cut they belong to.
 *    - after the all active transactions finished it finishes Consistent Cut with writing {@link ConsistentCutFinishRecord} that contains
 *      collection of transactions on the BEFORE and AFTER sides.
 * 5. After Consistent Cut finished globally, it clears {@link ConsistentCutFuture} variable and stops wrapping messages.
 */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter implements PartitionsExchangeAware {
    /** Current Consistent Cut, {@code null} if not running. */
    private final AtomicReference<ConsistentCutFuture> cutFutRef = new AtomicReference<>();

    /** ID of the last finished {@link ConsistentCutFuture}. Required to avoid re-run {@link ConsistentCutFuture} with the same id. */
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
     * Registers transaction before it is removed from {@link IgniteTxManager#activeTransactions()}.
     *
     * @param tx Transaction.
     */
    public void onRemoveActiveTransaction(IgniteInternalTx tx) {
        if (clientNode())
            return;

        ConsistentCutFuture cut = cutFutRef.get();

        if (cut != null)
            ((BaselineConsistentCutFuture)cut).onRemoveActiveTransaction(tx.finishFuture());
    }

    /**
     * Set transaction Consistent Cut ID.
     *
     * @param tx Transaction.
     */
    public void setTransactionCutId(IgniteInternalTx tx) {
        ConsistentCutFuture cut = cutFutRef.get();

        if (cut != null)
            tx.cutId(cut.id());
    }

    /**
     * Wraps a transaction message if Consistent Cut is running.
     *
     * @param txMsg Transaction message to wrap.
     * @param txCutId Consistent Cut ID after which transaction committed, if specified.
     */
    public static GridCacheMessage wrapMessage(
        GridCacheSharedContext<?, ?> cctx,
        GridCacheMessage txMsg,
        @Nullable UUID txCutId
    ) {
        if (cctx.consistentCutMgr() == null)
            return txMsg;

        ConsistentCutFuture cut = cctx.consistentCutMgr().cutFuture();

        if (cut != null)
            return new ConsistentCutAwareMessage(txMsg, cut.id(), txCutId);

        return txMsg;
    }

    /**
     * Handles received Consistent Cut ID from remote node. It compares it with the latest ID that local node is aware of.
     * Init local Consistent Cut procedure if received ID is a new one.
     *
     * @param id ID of {@link ConsistentCutFuture}.
     */
    public void handleConsistentCutId(UUID id) {
        ConsistentCutFuture cutFut = cutFutRef.get();

        if (cutFut != null || Objects.equals(id, lastFinishedCutId))
            return;

        ConsistentCutFuture newCutFut = clientNode() ? new ClientConsistentCutFuture(id) : newBaselineConsistentCut(id);

        if (!cutFutRef.compareAndSet(cutFut, newCutFut))
            return;

        if (newCutFut.isDone())
            return;

        cctx.kernalContext().pools().getSnapshotExecutorService().submit(() -> {
            BaselineConsistentCutFuture cut = (BaselineConsistentCutFuture)newCutFut;

            try {
                cut.init();

                if (log.isDebugEnabled())
                    log.debug("Prepared Consistent Cut: " + id);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to handle Consistent Cut: " + id, e);

                cut.onDone(e);
            }
        });
    }

    /** Creates new Consistent Cut future for baseline nodes. */
    protected BaselineConsistentCutFuture newBaselineConsistentCut(UUID id) {
        return new BaselineConsistentCutFuture(cctx, id);
    }

    /**
     * Cancels local Consistent Cut with error.
     *
     * @param err Error.
     */
    public void cancelCut(Throwable err) {
        ConsistentCutFuture cut = cutFutRef.get();

        if (cut != null && !cut.isDone())
            ((BaselineConsistentCutFuture)cut).onDone(err);

        cleanLocalCut();
    }

    /**
     * @return Current running Consistent Cut future, if cut isn't running then {@code null}.
     */
    public @Nullable ConsistentCutFuture cutFuture() {
        return cutFutRef.get();
    }

    /**
     * Cleans local Consistent Cut, stop signing outgoing messages.
     */
    public void cleanLocalCut() {
        ConsistentCutFuture cut = cutFutRef.get();

        if (log.isDebugEnabled())
            log.debug("Clean local cut: " + cut);

        if (cut == null)
            return;

        lastFinishedCutId = cut.id();

        cutFutRef.set(null);
    }

    /** */
    private boolean clientNode() {
        return cctx.kernalContext().clientNode() || !baselineNode(cctx.localNode(), cctx.kernalContext().state().clusterState());
    }
}
