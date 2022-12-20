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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.record.ConsistentCutFinishRecord;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.baselineNode;

/**
 * Class represents Consistent Cut functionality that consist of 2 roles:
 * 1. Wrapping outgoing transaction messages into {@link ConsistentCutAwareMessage}.
 * 2. Writing Consistent Cut recors into WAL {@link ConsistentCutMarkWalRole}.
 */
public class ConsistentCut {
    /** Grid context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Consistent Cut ID. */
    private final UUID id;

    /**
     * If {@code true} it wraps messages into {@link ConsistentCutAwareMessage}. Becames {@code false} after every baseline node
     * finished {@link #walMarkRole}.
     */
    private boolean wrapMsgRole = true;

    /** Role is responsible for marking WAL with Consistent Cut records on baseline nodes only. */
    private final @Nullable ConsistentCutMarkWalRole walMarkRole;

    /** Future that completes after all transactions sending {@link ConsistentCutAwareMessage} finished. */
    private @Nullable IgniteInternalFuture<?> lastCutAwareMsgSentFut;

    /** */
    ConsistentCut(GridCacheSharedContext<?, ?> cctx, UUID id) {
        this.cctx = cctx;
        this.id = id;

        walMarkRole = baselineNode(cctx.localNode(), cctx.kernalContext().state().clusterState())
            ? new ConsistentCutMarkWalRole(cctx, id) : null;
    }

    /** */
    void init(GridCacheSharedContext<?, ?> cctx) {
        if (walMarkRole != null)
            cctx.kernalContext().pools().getSnapshotExecutorService().submit(walMarkRole::init);
    }

    /** */
    void cancel(Throwable err) {
        if (walMarkRole != null)
            walMarkRole.cancel(err);
    }

    /** */
    void onCommit(IgniteInternalTx tx) {
        if (walMarkRole != null)
            walMarkRole.onCommit(tx.finishFuture());
    }

    /**
     * Wraps a transaction message if needed.
     *
     * @param txMsg Transaction message to wrap.
     * @param txCutId Consistent Cut ID after which transaction committed, if specified.
     */
    GridCacheMessage wrapMessageIfNeeded(GridCacheMessage txMsg, @Nullable UUID txCutId) {
        if (wrapMsgRole)
            return new ConsistentCutAwareMessage(txMsg, id, txCutId);

        return txMsg;
    }

    /** @return Consistent Cut ID. */
    UUID id() {
        return id;
    }

    /** */
    void finish() {
        wrapMsgRole = false;

        GridCompoundIdentityFuture<IgniteInternalTx> activeTxsFut = new GridCompoundIdentityFuture<>();

        for (IgniteInternalTx tx: cctx.tm().activeTransactions())
            activeTxsFut.add(tx.finishFuture());

        activeTxsFut.markInitialized();

        lastCutAwareMsgSentFut = activeTxsFut;
    }

    /** @return Future that completes after last {@link ConsistentCutAwareMessage} were sent. */
    IgniteInternalFuture<?> messagesWrappingRoleFinished() {
        return lastCutAwareMsgSentFut;
    }

    /** @return Future that completes with pointer to {@link ConsistentCutFinishRecord}. */
    IgniteInternalFuture<WALPointer> walMarkingRoleFinished() {
        return walMarkRole.finishFuture();
    }
}
