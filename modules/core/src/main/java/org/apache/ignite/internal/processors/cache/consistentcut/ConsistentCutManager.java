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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.baselineNode;

/**
 * Processes all stuff related to Consistent Cut.
 * <p>
 * Consistent Cut is a distributed algorithm that defines two set of transactions - BEFORE and AFTER cut - on baseline nodes.
 * It guarantees that every transaction was included into BEFORE on one node also included into the BEFORE on every other node
 * participated in the transaction. It means that Ignite nodes can safely recover themselves to the consistent BEFORE
 * state without any coordination with each other.
 * <p>
 * The algorithm starts on Ignite node by snapshot creation command. Other nodes are notified with discovery message of snapshot
 * distributed process or by transaction messages wrapped in {@link ConsistentCutAwareMessage}.
 * <p>
 * The algorithm consists of steps:
 * 1. On receiving new Consistent Cut ID it immediately creates new {@link ConsistentCut} before processing the message.
 * 2. It starts wrapping all transaction messages to {@link ConsistentCutAwareMessage}.
 * 3. Every transaction holds {@link IgniteTxAdapter#cutId()} after which it committed. The value is initially set on
 *    originated node for two-phase-commit, and backup node for one-phase-commit. Then it is propagated to other nodes with
 *    {@link ConsistentCutAwareMessage}.
 * 4. On baseline nodes it awaits {@link BaselineConsistentCut}, that completes with pointer to {@link ConsistentCutFinishRecord}.
 * 5. After Consistent Cut finished on all nodes, it clears reference to {@link ConsistentCut} and stops wrapping messages.
 */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter implements PartitionsExchangeAware {
    /** Current Consistent Cut, {@code null} if not running. */
    @GridToStringInclude
    private volatile ConsistentCut consistentCut;

    /** ID of the last finished Consistent Cut. Required to avoid re-run Consistent Cut with the same id. */
    @GridToStringInclude
    private volatile UUID lastFinishedCutId;

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        super.start0();

        cctx.exchange().registerExchangeAwareComponent(this);
    }

    /** {@inheritDoc} */
    @Override public void stop0(boolean cancel) {
        cancelConsistentCut(new IgniteCheckedException("Ignite node is stopping."));
    }

    /**
     * Stops Consistent Cut in case of baseline topology changed.
     */
    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        if (fut.localJoinExchange() || fut.changedBaseline() || fut.isBaselineNodeFailed())
            cancelConsistentCut(new IgniteCheckedException("Ignite topology changed, can't finish Consistent Cut."));
    }

    /**
     * Registers transaction before it is committed and removed from {@link IgniteTxManager#activeTransactions()}.
     *
     * @param tx Transaction.
     */
    public void onCommit(IgniteInternalTx tx) {
        if (cctx.kernalContext().clientNode())
            return;

        ConsistentCut cut = consistentCut;

        if (cut != null && cut.baseline())
            ((BaselineConsistentCut)cut).onCommit(tx.finishFuture());
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

        ConsistentCut cut = cctx.consistentCutMgr().consistentCut();

        if (cut != null)
            return new ConsistentCutAwareMessage(txMsg, cut.id(), txCutId);

        return txMsg;
    }

    /**
     * Handles received Consistent Cut ID from remote node. It compares it with the latest ID that local node is aware of.
     * Init local Consistent Cut procedure if received ID is a new one.
     *
     * @param id Consistent Cut ID.
     */
    public void handleConsistentCutId(UUID id) {
        if (consistentCut != null || Objects.equals(id, lastFinishedCutId))
            return;

        ConsistentCut newCut;

        synchronized (this) {
            if (consistentCut != null || Objects.equals(id, lastFinishedCutId))
                return;

            consistentCut = newCut = baselineNode(cctx.localNode(), cctx.kernalContext().state().clusterState()) ?
                new BaselineConsistentCut(cctx, id) : new NonBaselineConsistentCut(id);
        }

        if (newCut.baseline()) {
            cctx.kernalContext().pools().getSnapshotExecutorService().submit(() ->
                ((BaselineConsistentCut)newCut).init()
            );
        }
    }

    /**
     * Cancels local Consistent Cut with error.
     *
     * @param err Error.
     */
    public void cancelConsistentCut(Throwable err) {
        ConsistentCut cut = consistentCut;

        if (cut != null)
            cut.cancel(err);

        cleanConsistentCut();
    }

    /**
     * @return Current running Consistent Cut, if cut isn't running then {@code null}.
     */
    public @Nullable ConsistentCut consistentCut() {
        return consistentCut;
    }

    /**
     * @return Current running Consistent Cut ID, if cut isn't running then {@code null}.
     */
    public @Nullable UUID consistentCutId() {
        ConsistentCut cut = consistentCut;

        return cut == null ? null : cut.id();
    }

    /**
     * Cleans local Consistent Cut.
     */
    public void cleanConsistentCut() {
        ConsistentCut cut = consistentCut;

        if (cut == null)
            return;

        synchronized (this) {
            lastFinishedCutId = cut.id();

            consistentCut = null;
        }
    }

    /** */
    public void cleanLastFinishedCutId() {
        lastFinishedCutId = null;
    }
}
