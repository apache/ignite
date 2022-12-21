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
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.jetbrains.annotations.Nullable;

/** Processes all stuff related to Consistent Cut. */
public class ConsistentCutManager extends GridCacheSharedManagerAdapter {
    /** Current Consistent Cut, {@code null} if not running. */
    private volatile @Nullable ConsistentCut consistentCut;

    /**
     * Handles received Consistent Cut ID from remote node. It compares it with the latest ID that local node is aware of.
     * Init local Consistent Cut procedure if received ID is a new one.
     *
     * @param id Consistent Cut ID.
     */
    public void handleConsistentCutId(UUID id) {
        if (consistentCut != null)
            return;

        ConsistentCut newCut;

        synchronized (this) {
            if (consistentCut != null)
                return;

            consistentCut = newCut = new ConsistentCut(cctx, id);
        }

        newCut.init(cctx);
    }

    /** Clean Consistent Cut after cluster snapshot finished. */
    public synchronized void onClusterSnapshotFinished() {
        consistentCut = null;
    }

    /**
     * Registers transaction before it is committed and removed from {@link IgniteTxManager#activeTransactions()}.
     *
     * @param tx Transaction.
     */
    public void onCommit(IgniteInternalTx tx) {
        ConsistentCut cut = consistentCut;

        if (cut != null)
            cut.onCommit(tx);
    }

    /** @return Current running Consistent Cut ID, if cut isn't running then {@code null}. */
    public @Nullable UUID consistentCutId() {
        ConsistentCut cut = consistentCut;

        return cut == null ? null : cut.id();
    }

    /** Calls after all nodes finished Consistent Cut. */
    public void onConsistentCutFinished() {
        ConsistentCut cut = consistentCut;

        if (cut != null)
            cut.stopWrapMessages();
    }

    /** @return Future that completes after last {@link ConsistentCutAwareMessage} were sent. */
    public IgniteInternalFuture<?> wrapMessagesFinished() {
        ConsistentCut cut = consistentCut;

        return cut == null ? null : cut.wrapMessagesFinished();
    }

    /** @return Future that completes with pointer to {@link ConsistentCutFinishRecord}. */
    public IgniteInternalFuture<WALPointer> markingWalFinished() {
        ConsistentCut cut = consistentCut;

        return cut == null ? null : cut.markingWalFinished();
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

        return cut == null ? txMsg : cut.wrapMessage(txMsg, txCutId);
    }

    /** @return Current running Consistent Cut, if cut isn't running then {@code null}. */
    protected @Nullable ConsistentCut consistentCut() {
        return consistentCut;
    }
}
