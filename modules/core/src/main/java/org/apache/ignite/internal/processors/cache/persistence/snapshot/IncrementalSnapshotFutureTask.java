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

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCut;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutMarker;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

/** */
class IncrementalSnapshotFutureTask
    extends AbstractSnapshotFutureTask<IncrementalSnapshotFutureTaskResult>
    implements BiConsumer<String, File> {
    /** Metadata of the full snapshot. */
    private final Set<Integer> affectedCacheGrps;

    /** */
    private final ConsistentCutMarker marker;

    /** */
    public IncrementalSnapshotFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID reqNodeId,
        ConsistentCutMarker marker,
        SnapshotMetadata meta,
        File tmpWorkDir,
        FileIOFactory ioFactory
    ) {
        super(
            cctx,
            srcNodeId,
            reqNodeId,
            meta.snapshotName(),
            tmpWorkDir,
            ioFactory,
            new SnapshotSender(
                cctx.logger(IncrementalSnapshotFutureTask.class),
                cctx.kernalContext().pools().getSnapshotExecutorService()
            ) {
                @Override protected void init(int partsCnt) {
                    // No-op.
                }

                @Override protected void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    // No-op.
                }

                @Override protected void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
                    // No-op.
                }
            },
            null
        );

        affectedCacheGrps = new HashSet<>(meta.cacheGroupIds());
        this.marker = marker;

        cctx.cache().configManager().addConfigurationChangeListener(this);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> affectedCacheGroups() {
        return Collections.unmodifiableSet(affectedCacheGrps);
    }

    /** {@inheritDoc} */
    @Override public boolean start() {
        ConsistentCut cut = cctx.consistentCutMgr().runningCut();

        if (cut == null) {
            onDone(new IgniteCheckedException(
                String.format("Consistent Cut for marker [%s] wasn't  started.", marker)));

            return false;
        }

        cut.listen(snpPtrFut -> {
            if (snpPtrFut.error() != null)
                onDone(snpPtrFut.error());
            else
                onDone(new IncrementalSnapshotFutureTaskResult(snpPtrFut.result()));
        });

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable IncrementalSnapshotFutureTaskResult res, @Nullable Throwable err, boolean cancel) {
        cctx.cache().configManager().removeConfigurationChangeListener(this);

        return super.onDone(res, err, cancel);
    }

    /** {@inheritDoc} */
    @Override public void acceptException(Throwable th) {
        onDone(th);
    }

    /** {@inheritDoc} */
    @Override public void accept(String name, File file) {
        onDone(new IgniteException(IgniteSnapshotManager.cacheChangedException(CU.cacheId(name), name)));
    }
}
