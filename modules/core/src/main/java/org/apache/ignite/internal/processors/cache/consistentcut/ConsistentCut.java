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
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Basic class for Consistent Cut implementations. Common functionality is wrapping transaction messages
 * into {@link ConsistentCutAwareMessage}.
 */
abstract class ConsistentCut {
    /** Cache context. */
    protected final GridCacheSharedContext<?, ?> cctx;

    /** Consistent Cut ID. */
    @GridToStringInclude
    protected final UUID id;

    /** Future that completes after all transactions sending {@link ConsistentCutAwareMessage} finished. */
    private IgniteInternalFuture<IgniteInternalTx> lastCutAwareMsgSentFut;

    /** Wraps transaction message if {@code true}. */
    private boolean wrapMsg = true;

    /** */
    ConsistentCut(GridCacheSharedContext<?, ?> cctx, UUID id) {
        this.cctx = cctx;
        this.id = id;
    }

    /**
     * Wraps a transaction message if needed.
     *
     * @param txMsg Transaction message to wrap.
     * @param txCutId Consistent Cut ID after which transaction committed, if specified.
     */
    public GridCacheMessage wrapMessageIfNeeded(GridCacheMessage txMsg, @Nullable UUID txCutId) {
        return wrapMsg ? new ConsistentCutAwareMessage(txMsg, id, txCutId) : txMsg;
    }

    /** Stops wrapping messages. */
    public void stopWrapMessages() {
        wrapMsg = false;

        GridCompoundIdentityFuture<IgniteInternalTx> activeTxsFut = new GridCompoundIdentityFuture<>();

        for (IgniteInternalTx tx: cctx.tm().activeTransactions())
            activeTxsFut.add(tx.finishFuture());

        activeTxsFut.markInitialized();

        lastCutAwareMsgSentFut = activeTxsFut;
    }

    /** @return Future that completes after all transactions sending {@link ConsistentCutAwareMessage} finished. */
    public IgniteInternalFuture<IgniteInternalTx> lastCutAwareMsgSentFut() {
        return lastCutAwareMsgSentFut;
    }

    /** Consistent Cut ID. */
    public UUID id() {
        return id;
    }

    /** Cancels this Consistent Cut. */
    public abstract void cancel(Throwable err);

    /** @return {@code true} if instance is created for baseline node, otherwise {@code false}. */
    public abstract boolean baseline();

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConsistentCut.class, this);
    }
}
