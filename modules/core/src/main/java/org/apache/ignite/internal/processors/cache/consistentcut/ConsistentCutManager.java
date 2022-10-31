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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutStartRecord;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedBaseMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareFutureAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

/**
 * Processes all stuff related to Consistent Cut.
 * <p>
 * Consistent Cut splits timeline on 2 global areas - BEFORE and AFTER. It guarantees that every transaction committed BEFORE
 * also will be committed BEFORE on every other node. It means that an Ignite node can safely recover itself to this
 * point without any coordination with other nodes.
 * <p>
 * The algorithm starts on Ignite node by snapshot creation command. Other nodes notifies with discovery message of snapshot
 * distributed process or by transaction messages with marker {@link ConsistentCutMarkerMessage}.
 * <p>
 * The algorithm consist of steps:
 * 1. On receiving new {@link ConsistentCutMarker} it immediately creates new {@link ConsistentCut} before it processed
 *    a message (that holds marker) itself.
 * 2. It starts wrapping all transaction messages to {@link ConsistentCutMarkerMessage} or {@link ConsistentCutMarkerTxFinishMessage}
 *    that contains actual {@link ConsistentCutMarker}.
 * 3. It writes {@link ConsistentCutStartRecord} to limit amount of transactions on the AFTER side of Consistent Cut.
 *    After writing this record it's safe to miss transactions on the AFTER side.
 * 4. It collects transactions with PREPARING+ state to check which side of Consistent Cut they belong to. This collection
 *    contains all transactions on the BEFORE side. It's guaranteed with:
 *        a) For 2PC case (and for 1PC near/primary nodes) there are 2 transaction messages (PrepareResponse, FinishRequest)
 *           to transfer transaction from {@link TransactionState#ACTIVE} to {@link TransactionState#COMMITTED}.
 *           In the point 1. it's guaranteed HB between versions in sent PrepareMessage and received FinishMessage.
 *           Then in such case transaction will never be on the BEFORE side.
 *        b) For 1PC case on backup node this node always choose the greatest version to send on other nodes. And then
 *           it will always be on the AFTER side.
 * 5. It awaits every transaction in this collection to be committed to decide which side of Consistent Cut they belong to.
 * 6. Every transaction is signed with the latest {@link ConsistentCutMarker} AFTER which it committed. This marker is defined
 *    at single node within a transaction before it starts committing {@link #registerBeforeCommit(IgniteInternalTx)}.
 * 7. After the check-list is empty it finishes Consistent Cut with writing {@link ConsistentCutFinishRecord} that contains
 *    collection of transactions on the BEFORE and AFTER sides.
 * 8. After Consistent Cut finished globally, it clears {@link ConsistentCut} variable and stops wrapping messages.
 */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter implements PartitionsExchangeAware {
    /** It serves updates of {@link #cut} with CAS. */
    protected static final AtomicReferenceFieldUpdater<ConsistentCutManager, ConsistentCut> CONSISTENT_CUT =
        AtomicReferenceFieldUpdater.newUpdater(ConsistentCutManager.class, ConsistentCut.class, "cut");

    /** {@link ConsistentCut}, if running. */
    private volatile @Nullable ConsistentCut cut;

    /** Marker of the last finished Consistent Cut. Required to avoid re-run Consistent Cut with the same marker. */
    protected volatile ConsistentCutMarker lastFinishedCutMarker;

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
        if (fut.changedBaseline() || fut.isBaselineNodeFailed() || fut.firstEvent().type() == EventType.EVT_NODE_JOINED)
            cancelCut(new IgniteCheckedException("Ignite topology changed, can't finish Consistent Cut."));
    }

    /**
     * Registers transaction before it starts committing.
     *
     * @param tx Transaction.
     */
    public void registerBeforeCommit(IgniteInternalTx tx) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut != null) {
            tx.marker(cut.marker());

            cut.addCommittingTransaction(tx.finishFuture());
        }

        if (log.isDebugEnabled()) {
            log.info("`registerBeforeCommit` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + " , txMarker=" + tx.marker() + ", cutMarker = " + (cut == null ? null : cut.marker()));
        }
    }

    /**
     * Registers specified committing transaction.
     *
     * @param msg Finish message signed with {@link ConsistentCutMarker}.
     */
    public void registerCommitting(ConsistentCutMarkerTxFinishMessage msg) {
        IgniteInternalTx tx = extractTransactionFromFinishMessage(msg.payload());

        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (tx == null) {
            if (cut != null) {
                U.warn(log, "Failed to find transaction for message [msg=" + msg.payload() + "]. Cut might be inconsistent.");

                cut.onDone(null, null);
            }

            return;
        }

        tx.marker(msg.txMarker());

        if (cut != null)
            cut.addCommittingTransaction(tx.finishFuture());

        if (log.isDebugEnabled()) {
            log.debug("`registerCommitting` from " + tx.nearXidVersion().asIgniteUuid() + " to " + tx.xid()
                + " , txMarker=" + tx.marker() + ", cutMarker = " + (cut == null ? null : cut.marker()));
        }
    }

    /**
     * @param msg Transaction finish message.
     * @return Transaction, or {@code null} if not found.
     */
    private @Nullable IgniteInternalTx extractTransactionFromFinishMessage(GridDistributedBaseMessage msg) {
        if (msg instanceof GridNearTxFinishRequest) {
            GridNearTxFinishRequest req = (GridNearTxFinishRequest)msg;

            GridCacheVersion dhtVer = cctx.tm().mappedVersion(req.version());

            return cctx.tm().tx(dhtVer);
        }
        else if (msg instanceof GridDhtTxFinishRequest) {
            GridDhtTxFinishRequest req = (GridDhtTxFinishRequest)msg;

            return cctx.tm().tx(req.version());
        }
        else if (msg instanceof GridDhtTxPrepareResponse) {
            GridDhtTxPrepareResponse res = (GridDhtTxPrepareResponse)msg;

            GridDhtTxPrepareFuture fut =
                (GridDhtTxPrepareFuture)cctx.mvcc().versionedFuture(res.version(), res.futureId());

            return fut.tx();
        }
        else if (msg instanceof GridNearTxPrepareResponse) {
            GridNearTxPrepareResponse res = (GridNearTxPrepareResponse)msg;

            GridNearTxPrepareFutureAdapter fut =
                (GridNearTxPrepareFutureAdapter)cctx.mvcc().versionedFuture(res.version(), res.futureId());

            return fut.tx();
        }

        return null;
    }

    /**
     * Wraps transaction prepare response message with marker message if cut is running.
     *
     * @param txMsg Transaction message to wrap.
     */
    public GridCacheMessage wrapTxPrepareResponse(
        GridDistributedBaseMessage txMsg,
        boolean onePhase,
        @Nullable ConsistentCutMarker txMarker
    ) {
        ConsistentCutMarker marker = runningCutMarker();

        if (marker != null) {
            if (onePhase)
                return new ConsistentCutMarkerTxFinishMessage(txMsg, marker, txMarker);

            return new ConsistentCutMarkerMessage(txMsg, marker);
        }

        return txMsg;
    }

    /**
     * Wraps transaction prepare request message with marker message if cut is running.
     *
     * @param txMsg Transaction message to wrap.
     */
    public GridCacheMessage wrapTxPrepareRequest(GridDistributedBaseMessage txMsg) {
        ConsistentCutMarker marker = runningCutMarker();

        if (marker != null)
            return new ConsistentCutMarkerMessage(txMsg, marker);

        return txMsg;
    }

    /**
     * Wraps transaction finish request message with marker message if cut is running.
     *
     * @param txMsg Transaction message to wrap.
     */
    public GridCacheMessage wrapTxFinishRequest(GridDistributedBaseMessage txMsg, @Nullable ConsistentCutMarker txMarker) {
        ConsistentCutMarker marker = runningCutMarker();

        if (marker != null)
            return new ConsistentCutMarkerTxFinishMessage(txMsg, marker, txMarker);

        return txMsg;
    }

    /**
     * Handles received marker from remote node. It compares it with the latest marker that local node is aware of.
     * Init local Consistent Cut procedure if received marker is a new one.
     *
     * @param marker Received Cut marker from different node.
     */
    public void handleConsistentCutMarker(ConsistentCutMarker marker) {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut == null && newMarker(marker)) {
            ConsistentCut newCut = newConsistentCut(marker);

            if (CONSISTENT_CUT.compareAndSet(this, cut, newCut)) {
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

    /** Checks whether {@code marker} is new one. */
    private boolean newMarker(ConsistentCutMarker marker) {
        ConsistentCutMarker m = lastFinishedCutMarker;

        return m == null || !marker.id().equals(m.id());
    }

    /** Creates new Consistent Cut instance. */
    protected ConsistentCut newConsistentCut(ConsistentCutMarker marker) {
        return new ConsistentCut(cctx, marker);
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

        finishLocalCut();
    }

    /**
     * @return Marker of current running Consistent Cut, if cut isn't running then {@code null}.
     */
    protected @Nullable ConsistentCutMarker runningCutMarker() {
        ConsistentCut cut = CONSISTENT_CUT.get(this);

        return cut == null ? null : cut.marker();
    }

    /**
     * @return Current running Consistent Cut, if cut isn't running then {@code null}.
     */
    public @Nullable ConsistentCut runningCut() {
        return CONSISTENT_CUT.get(this);
    }

    /**
     * Starts {@link ConsistentCut} in the discovery thread if not started yet within a transaction thread.
     *
     * @param marker Marker that inits consistent cut.
     */
    public void startLocalCut(ConsistentCutMarker marker) {
        if (log.isDebugEnabled())
            log.debug("`startLocalCut` for " + marker + " " + cctx.localNodeId());

        handleConsistentCutMarker(marker);
    }

    /**
     * Finishes local Consistent Cut, stop signing outgoing messages with marker.
     */
    public void finishLocalCut() {
        if (log.isDebugEnabled())
            log.debug("`finishLocalCut` for " + runningCutMarker());

        ConsistentCut cut = CONSISTENT_CUT.get(this);

        if (cut == null)
            return;

        lastFinishedCutMarker = cut.marker();

        CONSISTENT_CUT.set(this, null);
    }
}
