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
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree.IncrementalSnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

/** */
class IncrementalSnapshotFutureTask extends AbstractSnapshotFutureTask<Void> implements BiConsumer<String, File> {
    /** Incremental file tree. */
    private final IncrementalSnapshotFileTree ift;

    /** Metadata of the full snapshot. */
    private final Set<Integer> affectedCacheGrps;

    /**
     * Pointer to the previous snapshot record.
     * In case first increment snapshot will point to the {@link ClusterSnapshotRecord}.
     * For second and subsequent incements on the previous {@link IncrementalSnapshotFinishRecord}.
     */
    private final WALPointer lowPtr;

    /** Future that completes with WAL pointer to {@link IncrementalSnapshotFinishRecord}. */
    private final IgniteInternalFuture<WALPointer> highPtrFut;

    /** */
    public IncrementalSnapshotFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID reqNodeId,
        SnapshotMetadata meta,
        IncrementalSnapshotFileTree ift,
        WALPointer lowPtr,
        IgniteInternalFuture<WALPointer> highPtrFut
    ) {
        super(
            cctx,
            srcNodeId,
            reqNodeId,
            meta.snapshotName(),
            new SnapshotSender(
                cctx.logger(IncrementalSnapshotFutureTask.class),
                cctx.kernalContext().pools().getSnapshotExecutorService()
            ) {
                @Override protected void init(int partsCnt) {
                    // No-op.
                }

                @Override protected void sendPart0(File from, File to, @Nullable String storagePath, GroupPartitionId pair, Long length) {
                    // No-op.
                }

                @Override protected void sendDelta0(File delta, File snpPart, GroupPartitionId pair) {
                    // No-op.
                }
            },
            null
        );

        this.ift = ift;
        this.affectedCacheGrps = new HashSet<>(meta.cacheGroupIds());
        this.lowPtr = lowPtr;
        this.highPtrFut = highPtrFut;

        cctx.cache().configManager().addConfigurationChangeListener(this);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> affectedCacheGroups() {
        return affectedCacheGrps;
    }

    /** {@inheritDoc} */
    @Override public boolean start() {
        try {
            File incSnpDir = ift.root();

            if (!incSnpDir.mkdirs() && !incSnpDir.exists()) {
                onDone(new IgniteException("Can't create snapshot directory [dir=" + incSnpDir.getAbsolutePath() + ']'));

                return false;
            }

            highPtrFut.chain(() -> {
                if (highPtrFut.error() != null) {
                    onDone(highPtrFut.error());

                    return null;
                }

                try {
                    NodeFileTree ft = cctx.kernalContext().pdsFolderResolver().fileTree();

                    copyWal(ft, highPtrFut.result());

                    copyFiles(
                        ft.marshaller(),
                        ift.marshaller(),
                        NodeFileTree::notTmpFile
                    );

                    copyFiles(
                        ft.binaryMeta(),
                        ift.binaryMeta(),
                        NodeFileTree::binFile
                    );

                    onDone();
                }
                catch (Throwable e) {
                    onDone(e);
                }

                return null;
            }, cctx.kernalContext().pools().getSnapshotExecutorService());

            return true;
        }
        finally {
            cctx.cache().configManager().removeConfigurationChangeListener(this);
        }
    }

    /**
     * Copies WAL segments to the incremental snapshot directory.
     *
     * @param ft Node file tree.
     * @param highPtr High WAL pointer to copy.
     * @throws IgniteInterruptedCheckedException If failed.
     * @throws IOException If failed.
     */
    private void copyWal(NodeFileTree ft, WALPointer highPtr) throws IgniteInterruptedCheckedException, IOException {
        // First increment must include low segment, because full snapshot knows nothing about WAL.
        // All other begins from the next segment because lowPtr already saved inside previous increment.
        long lowIdx = lowPtr.index() + (ift.index() == 1 ? 0 : 1);
        long highIdx = highPtr.index();

        assert cctx.gridConfig().getDataStorageConfiguration().isWalCompactionEnabled() : "WAL Compaction must be enabled";
        assert lowIdx <= highIdx;

        if (log.isInfoEnabled())
            log.info("Waiting for WAL segments compression [lowIdx=" + lowIdx + ", highIdx=" + highIdx + ']');

        cctx.wal().awaitCompacted(highPtr.index());

        if (log.isInfoEnabled())
            log.info("Linking WAL segments into incremental snapshot [lowIdx=" + lowIdx + ", " + "highIdx=" + highIdx + ']');

        if (!ift.wal().mkdirs() && !ift.wal().exists())
            throw new IgniteException("Failed to create snapshot WAL directory [idx=" + ift.wal() + ']');

        for (; lowIdx <= highIdx; lowIdx++) {
            File seg = ft.zipWalArchiveSegment(lowIdx);

            if (!seg.exists())
                throw new IgniteException("WAL segment not found in archive [idx=" + lowIdx + ']');

            File segLink = ift.walSegment(lowIdx);

            if (log.isDebugEnabled())
                log.debug("Creaing segment link [path=" + segLink.getAbsolutePath() + ']');

            Files.createLink(segLink.toPath(), seg.toPath());
        }
    }

    /**
     * Copy files {@code fromDir} to {@code toDir}.
     *
     * @param fromDir From directory.
     * @param toDir To directory.
     * @param filter File filter.
     */
    private void copyFiles(File fromDir, File toDir, FileFilter filter) throws IOException {
        assert fromDir.exists() && fromDir.isDirectory();

        if (!toDir.isDirectory() && !toDir.mkdirs() && !toDir.exists())
            throw new IgniteException("Target directory can't be created [target=" + toDir.getAbsolutePath() + ']');

        for (File from : fromDir.listFiles(filter)) {
            try {
                Files.copy(from.toPath(), new File(toDir, from.getName()).toPath());
            }
            catch (FileAlreadyExistsException e) {
                // Skip, file might exist in case the marshaller directory is shared between multiple Ignite nodes.
            }
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
