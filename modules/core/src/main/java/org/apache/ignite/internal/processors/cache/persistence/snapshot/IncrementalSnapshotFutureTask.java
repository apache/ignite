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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

/** */
class IncrementalSnapshotFutureTask
    extends AbstractSnapshotFutureTask<IncrementalSnapshotFutureTaskResult>
    implements BiConsumer<String, File> {
    /** Index of incremental snapshot. */
    private final int incIdx;

    /** Snapshot path. */
    private final @Nullable String snpPath;

    /** Metadata of the full snapshot. */
    private final Set<Integer> affectedCacheGrps;

    /**
     * Pointer to the previous snapshot record.
     * In case first increment snapshot will point to the {@link ClusterSnapshotRecord}.
     * For second and subsequent incements on the previous consistent cut record.
     */
    private final WALPointer lowPtr;

    /** Current consistent cut WAL pointer. */
    private final WALPointer highPtr;

    /** */
    public IncrementalSnapshotFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID reqNodeId,
        SnapshotMetadata meta,
        @Nullable String snpPath,
        int incIdx,
        File tmpWorkDir,
        FileIOFactory ioFactory,
        WALPointer lowPtr,
        WALPointer highPtr
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

        this.incIdx = incIdx;
        this.snpPath = snpPath;
        this.affectedCacheGrps = new HashSet<>(meta.cacheGroupIds());
        this.lowPtr = lowPtr;
        this.highPtr = highPtr;

        cctx.cache().configManager().addConfigurationChangeListener(this);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> affectedCacheGroups() {
        return affectedCacheGrps;
    }

    /** {@inheritDoc} */
    @Override public boolean start() {
        try {
            File incSnpDir = cctx.snapshotMgr().incrementalSnapshotLocalDir(snpName, snpPath, incIdx);

            if (!incSnpDir.mkdirs()) {
                onDone(new IgniteException("Can't create snapshot directory[dir=" + incSnpDir.getAbsolutePath() + ']'));

                return false;
            }

            cctx.kernalContext().pools().getSnapshotExecutorService().submit(() -> {
                try {
                    // First increment must include low segment, because full snapshot knows nothing about WAL.
                    // All other begins from the next segment because lowPtr already saved inside previous increment.
                    long lowIdx = lowPtr.index() + (incIdx == 1 ? 0 : 1);
                    long highIdx = highPtr.index();

                    assert cctx.gridConfig().getDataStorageConfiguration().isWalCompactionEnabled()
                        : "WAL Compaction must be enabled";
                    assert lowIdx <= highIdx;

                    if (log.isInfoEnabled())
                        log.info("Waiting for WAL segments compression [lowIdx=" + lowIdx + ", highIdx=" + highIdx + ']');

                    cctx.wal().awaitCompacted(highPtr.index());

                    if (log.isInfoEnabled()) {
                        log.info("Linking WAL segments into incremental snapshot [lowIdx=" + lowIdx + ", " +
                            "highIdx=" + highIdx + ']');
                    }

                    for (; lowIdx <= highIdx; lowIdx++) {
                        File seg = cctx.wal().compactedSegment(lowIdx);

                        if (!seg.exists()) {
                            onDone(new IgniteException("WAL segment not found in archive [idx=" + lowIdx + ']'));

                            return;
                        }

                        Path segLink = incSnpDir.toPath().resolve(seg.getName());

                        if (log.isDebugEnabled())
                            log.debug("Creaing segment link [path=" + segLink.toAbsolutePath() + ']');

                        Files.createLink(segLink, seg.toPath());
                    }

                    onDone(new IncrementalSnapshotFutureTaskResult());
                }
                catch (Throwable e) {
                    onDone(e);
                }
            });

            return true;
        }
        finally {
            cctx.cache().configManager().removeConfigurationChangeListener(this);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptException(Throwable th) {
        cctx.cache().configManager().removeConfigurationChangeListener(this);

        onDone(th);
    }

    /** {@inheritDoc} */
    @Override public void accept(String name, File file) {
        onDone(new IgniteException(IgniteSnapshotManager.cacheChangedException(CU.cacheId(name), name)));
    }
}
