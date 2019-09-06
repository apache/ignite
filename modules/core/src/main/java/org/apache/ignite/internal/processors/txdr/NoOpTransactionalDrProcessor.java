/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.txdr;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperation;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class NoOpTransactionalDrProcessor extends GridProcessorAdapter implements TransactionalDrProcessor {
    /**
     * @param ctx Context.
     */
    public NoOpTransactionalDrProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckPointBegin(long snapshotId, WALPointer ptr, SnapshotOperation snapshotOperation) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onPartitionsFullMessagePrepared(
            @Nullable GridDhtPartitionExchangeId exchId,
            GridDhtPartitionsFullMessage fullMsg
    ) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onChangeGlobalStateMessagePrepared(ChangeGlobalStateMessage msg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean shouldIgnoreAssignPartitionStates(GridDhtPartitionsExchangeFuture fut) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldScheduleRebalance(GridDhtPartitionsExchangeFuture fut) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldApplyUpdateCounterOnRebalance() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldSkipCounterConsistencyCheckOnPME() {
        return false;
    }
}
