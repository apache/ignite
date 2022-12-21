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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.UUID;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.baselineNode;

/**
 * Consistent Cut is a distributed algorithm that defines two set of transactions - BEFORE and AFTER cut - on baseline nodes.
 * It guarantees that every transaction was included into BEFORE on one node also included into the BEFORE on every other node
 * participated in the transaction. It means that Ignite nodes can safely recover themselves to the consistent BEFORE
 * state without any coordination with each other.
 * <p>
 * The algorithm starts on Ignite node by snapshot creation command. Other nodes are notified with discovery message of snapshot
 * distributed process or by transaction messages wrapped in {@link ConsistentCutAwareMessage}.
 * <p>
 * There are two roles that node can play:
 * 1. Wraps outgoing transaction messages into {@link ConsistentCutAwareMessage}. For this role every node is responsible.
 * 2. Writes Consistent Cut records into WAL. Only baseline nodes do it.
 * <p>
 * Nodes start wrapping messages from the moment Consistent Cut started on the node, and finsihed after all baseline
 * nodes complete their role 2.
 */
public class ConsistentCut {
    /** Grid cache context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Consistent Cut ID. */
    private final UUID id;

    /**
     * If {@code true} it wraps messages into {@link ConsistentCutAwareMessage}. Becames {@code false} after every baseline node
     * completes {@link #markWalFut}.
     */
    private volatile boolean wrapMsg = true;

    /** Future that completes after last {@link ConsistentCutAwareMessage} was sent. */
    private final GridFutureAdapter<?> wrapMsgsFut = new GridFutureAdapter<>();

    /** Future that completes after {@link ConsistentCutFinishRecord} was written. */
    private final @Nullable ConsistentCutMarkWalFuture markWalFut;

    /** */
    ConsistentCut(GridCacheSharedContext<?, ?> cctx, UUID id) {
        this.cctx = cctx;
        this.id = id;

        markWalFut = baselineNode(cctx.localNode(), cctx.kernalContext().state().clusterState())
            ? new ConsistentCutMarkWalFuture(cctx, id) : null;
    }

    /**
     * Wraps a transaction message if needed.
     *
     * @param txMsg Transaction message to wrap.
     * @param txCutId Consistent Cut ID after which transaction committed, if specified.
     */
    GridCacheMessage wrapMessage(GridCacheMessage txMsg, @Nullable UUID txCutId) {
        if (wrapMsg)
            return new ConsistentCutAwareMessage(txMsg, id, txCutId);

        return txMsg;
    }

    /** Stops wrapping outging messages. */
    void stopWrapMessages() {
        wrapMsg = false;

        GridCompoundIdentityFuture<IgniteInternalTx> activeTxsFut = new GridCompoundIdentityFuture<>();

        for (IgniteInternalTx tx: cctx.tm().activeTransactions())
            activeTxsFut.add(tx.finishFuture());

        activeTxsFut.markInitialized();

        activeTxsFut.listen(f -> wrapMsgsFut.onDone());
    }

    /** */
    void init(GridCacheSharedContext<?, ?> cctx) {
        if (markWalFut != null)
            cctx.kernalContext().pools().getSnapshotExecutorService().submit(markWalFut::init);
    }

    /** */
    void onCommit(IgniteInternalTx tx) {
        if (markWalFut != null)
            markWalFut.onCommit(tx.finishFuture());
    }

    /** @return Consistent Cut ID. */
    UUID id() {
        return id;
    }

    /** @return Future that completes after last {@link ConsistentCutAwareMessage} were sent. */
    IgniteInternalFuture<?> wrapMessagesFinished() {
        return wrapMsgsFut;
    }

    /** @return Future that completes with pointer to {@link ConsistentCutFinishRecord}. */
    @Nullable IgniteInternalFuture<WALPointer> markWalFinished() {
        return markWalFut;
    }
}
