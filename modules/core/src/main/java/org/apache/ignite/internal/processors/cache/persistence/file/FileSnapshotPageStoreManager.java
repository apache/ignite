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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotPageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotProcessHandler;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.thread.IgniteThread;

/** */
public class FileSnapshotPageStoreManager extends GridCacheSharedManagerAdapter
    implements SnapshotPageStoreManager<FileSnapshotDescriptor> {
    /** */
    private static final String SNAPSHOT_CP_REASON = "Wakeup for snapshot checkpoint [id=%s, grpId=%s, parts=%s]";

    /**
     * Scheduled snapshot processes.
     * idx
     * grpId
     * partId
     * delta offset
     */

    /** Tracking partition files over all running snapshot processes. */
    private final Set<GroupPartitionId> trackList = new GridConcurrentHashSet<>();

    /** */
    private final IgniteLogger log;

    /** */
    private final GridCacheDatabaseSharedManager db;

    /** Thread local with buffers for handling copy-on-write over {@link PageStore} events. */
    private ThreadLocal<ByteBuffer> threadPageBuf;

    /** */
    private volatile SnapshotThread snapshotter;

    /** */
    public FileSnapshotPageStoreManager(GridKernalContext ctx) {
        assert CU.isPersistenceEnabled(ctx.config());

        log = ctx.log(getClass());
        db = (GridCacheDatabaseSharedManager)ctx.cache().context().database();
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        setThreadPageBuf(ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(db.pageSize()).order(ByteOrder.nativeOrder())));

        snapshotter = new SnapshotThread();
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        new IgniteThread(snapshotter).start();
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        U.cancel(snapshotter);
        U.join(snapshotter, log);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> snapshot(
        int idx,
        int grpId,
        Set<Integer> parts,
        SnapshotProcessHandler<FileSnapshotDescriptor> hndlr
    ) {
        CheckpointFuture cpFut = db.forceCheckpoint(String.format(SNAPSHOT_CP_REASON, idx, grpId, S.compact(parts)));

        if (cpFut == null)
            return new GridFinishedFuture<>(new IgniteCheckedException("Checkpoint thread is not running."));

        cpFut.finishFuture().listen(new IgniteInClosure<IgniteInternalFuture<Object>>() {
            @Override public void apply(IgniteInternalFuture<Object> future) {
                // Set partition file begin, end points.
                // Set delta file begin points.
            }
        });

        // 1. Check for the last checkpoint and run if not.
        // 2. Wait when checkpoint process ends.
        // 2. Fix the partition files sizes.
        // 3. Start tracking all incoming updates for all cache group files.

        // send partition

        // send delta

        // Use sync mode to execute provided task over partitons.
        // Submit to the worker.
        try {
            hndlr.handlePartition(new FileSnapshotDescriptor(null, 0, 0, 0));

            hndlr.handleDelta(new FileSnapshotDescriptor(null, 1, 0, 0));
        }
        catch (IgniteCheckedException e) {
            U.log(log, "An error occured while handling partition files.", e);
        }

        return null;
    }

    /** */
    public void setThreadPageBuf(final ThreadLocal<ByteBuffer> buf) {
        threadPageBuf = buf;
    }

    /** */
    private class SnapshotThread extends GridWorker {

        /** */
        public SnapshotThread() {
            super(cctx.igniteInstanceName(), "db-snapshot-thread", FileSnapshotPageStoreManager.this.log);

            // Params
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {

        }
    }
}
