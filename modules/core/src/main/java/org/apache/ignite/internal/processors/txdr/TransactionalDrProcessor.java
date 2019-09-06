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

import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.SnapshotRecord;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperation;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface TransactionalDrProcessor extends GridProcessor, IgniteChangeGlobalStateSupport {
    /**
     * @param snapshotId Snapshot id.
     * @param ptr Pointer to the {@link SnapshotRecord}.
     * @param snapshotOperation Snapshot operation.
     */
    public void onMarkCheckPointBegin(long snapshotId, WALPointer ptr, SnapshotOperation snapshotOperation);

    /**
     * @param exchId Partition exchange id.
     *               This parameter has always a non-null value in case of the message is created for an exchange.
     * @param fullMsg Partitions full message.
     */
    public void onPartitionsFullMessagePrepared(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsFullMessage fullMsg);

    /**
     * @param msg Change global state message.
     */
    public void onChangeGlobalStateMessagePrepared(ChangeGlobalStateMessage msg);

    /**
     * Returns true if we should skip assigning MOVING state to partitions due to outdated counters.
     * The idea is to avoid redundant rebalance in case of random discovery events on REPLICA cluster.
     * If event is "special" and rebalance really should happen according to REPLICA lifecycle, method will return true.
     *
     * @param fut Current exchange future.
     */
    public boolean shouldIgnoreAssignPartitionStates(GridDhtPartitionsExchangeFuture fut);

    /**
     * Returns true if we should schedule rebalance for MOVING partitions even if ideal assignment wasn't changed.
     *
     * @param fut Current exchange future.
     */
    public boolean shouldScheduleRebalance(GridDhtPartitionsExchangeFuture fut);

    /**
     * Returns {@code true} if update counters that are placed into {@link GridDhtPartitionSupplyMessage#last()}
     * should be applied when the rebalance is finished.
     *
     * @return {@code true} if update counters should be applied when the rebalance is finished.
     */
    public boolean shouldApplyUpdateCounterOnRebalance();

    /**
     * @return {@code True} if partition counter consistency check should be skipped on partition map exchange.
     */
    public boolean shouldSkipCounterConsistencyCheckOnPME();
}
