/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot manager stub.
 *
 * @deprecated Use {@link IgniteSnapshotManager}.
 */
@Deprecated
public class IgniteCacheSnapshotManager<T extends SnapshotOperation> extends GridCacheSharedManagerAdapter implements IgniteChangeGlobalStateSupport {
    /** Snapshot started lock filename. */
    public static final String SNAPSHOT_RESTORE_STARTED_LOCK_FILENAME = "snapshot-started.loc";

    /** Temp files completeness marker. */
    public static final String TEMP_FILES_COMPLETENESS_MARKER = "finished.tmp";

    /**
     * Try to start local snapshot operation if it's required by discovery event.
     *
     * @param discoveryEvt Discovery event.
     * @param topVer topology version on the moment when this method was called
     *
     * @throws IgniteCheckedException if failed
     */
    @Nullable public IgniteInternalFuture tryStartLocalSnapshotOperation(
            @Nullable DiscoveryEvent discoveryEvt, AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        return null;
    }

    /**
     * @param initiatorNodeId Initiator node id.
     * @param snapshotOperation Snapshot operation.
     */
    @Nullable public IgniteInternalFuture startLocalSnapshotOperation(
        UUID initiatorNodeId,
        T snapshotOperation,
        AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        return null;
    }

    /**
     * @param snapshotOperation current snapshot operation.
     * @param map  (cacheId, partId) -> (lastAllocatedIndex, count)
     *
     * @return {@code true} if next operation must be snapshot, {@code false} if checkpoint must be executed.
     */
    public IgniteFuture<?> onMarkCheckPointBegin(
        T snapshotOperation,
        PartitionAllocationMap map
    ) throws IgniteCheckedException {
        return null;
    }

    /**
     *
     */
    public boolean partitionsAreFrozen(CacheGroupContext grp) {
        return false;
    }

    /**
     *
     */
    public boolean snapshotOperationInProgress() {
        return false;
    }

    /**
     *
     */
    public void beforeCheckpointPageWritten() {
        // No-op.
    }

    /**
     *
     */
    public void afterCheckpointPageWritten() {
        // No-op.
    }

    /**
     * @param fullId Full id.
     */
    public void beforePageWrite(FullPageId fullId) {
        // No-op.
    }

    /**
     * @param cctx Cctx.
     * @param destroy Destroy flag.
     */
    public void onCacheStop(GridCacheContext<?, ?> cctx, boolean destroy) {
        // No-op.
    }

    /**
     * @param gctx Cctx.
     * @param destroy Destroy flag.
     */
    public void onCacheGroupStop(CacheGroupContext gctx, boolean destroy) {
        // No-op.
    }

    /**
     *
     */
    public void onChangeTrackerPage(
        Long page,
        FullPageId fullId,
        PageMemory pageMem
    ) throws IgniteCheckedException {
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

    /**
     * @return {@code True} if TX READ records must be logged in WAL.
     */
    public boolean needTxReadLogging() {
        return false;
    }
}
