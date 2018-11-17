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
package org.apache.ignite.internal.processors.txdr;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperation;
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
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
    }

    /** {@inheritDoc} */
    @Override public void onPartitionsFullMessagePrepared(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsFullMessage fullMsg) {
    }

    /** {@inheritDoc} */
    @Override public boolean shouldIgnoreAssignPartitionStates(GridDhtPartitionsExchangeFuture fut) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean shouldScheduleRebalance(GridDhtPartitionsExchangeFuture fut) {
        return false;
    }
}
