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

package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.snapshot.SnapshotOperation;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteCacheSnapshotManager extends GridCacheSharedManagerAdapter implements IgniteChangeGlobalStateSupport {
    /** Snapshot started lock filename. */
    public static final String SNAPSHOT_RESTORE_STARTED_LOCK_FILENAME = "snapshot-started.loc";

    /**
     * @param initiatorNodeId Initiator node id.
     * @param snapshotOperation Snapshot operation.
     */
    @Nullable public IgniteInternalFuture startLocalSnapshotOperation(
        UUID initiatorNodeId,
        SnapshotOperation snapshotOperation
    ) throws IgniteCheckedException {
        return null;
    }

    /**
     * @param snapOp current snapshot operation.
     *
     * @return {@code true} if next operation must be snapshot, {@code false} if checkpoint must be executed.
     */
    public boolean onMarkCheckPointBegin(
        SnapshotOperation snapOp,
        NavigableMap<T2<Integer, Integer>, T2<Integer, Integer>> map
    ) throws IgniteCheckedException {
        return false;
    }

    /**
     *
     */
    public void restoreState() throws IgniteCheckedException {
        // No-op.
    }

    public boolean snapshotOperationInProgress(){
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
     * @param fullId Full id.
     */
    public void onPageWrite(FullPageId fullId, ByteBuffer tmpWriteBuf) {
        // No-op.
    }

    /**
     * @param cctx Cctx.
     */
    public void onCacheStop(GridCacheContext cctx) {
        // No-op.
    }

    /**
     * @param gctx Cctx.
     */
    public void onCacheGroupStop(CacheGroupContext gctx) {
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

    /**
     *
     */
    public void flushDirtyPageHandler(
        FullPageId fullId,
        ByteBuffer pageBuf,
        Integer tag
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
}
