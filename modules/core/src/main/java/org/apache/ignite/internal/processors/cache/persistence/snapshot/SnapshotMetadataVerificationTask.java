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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.incrementalSnapshotWalsDir;

/** Snapshot task to verify snapshot metadata on the baseline nodes for given snapshot name. */
@GridInternal
public class SnapshotMetadataVerificationTask
      extends ComputeTaskAdapter<SnapshotMetadataVerificationTaskArg, SnapshotMetadataVerificationTaskResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        SnapshotMetadataVerificationTaskArg arg
    ) throws IgniteException {
        Map<ComputeJob, ClusterNode> map = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid)
            map.put(new MetadataVerificationJob(arg), node);

        return map;
    }

    /** Job that verifies snapshot on an Ignite node. */
    private static class MetadataVerificationJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** */
        @LoggerResource
        private transient IgniteLogger log;

        /** */
        private final SnapshotMetadataVerificationTaskArg arg;

        /** */
        public MetadataVerificationJob(SnapshotMetadataVerificationTaskArg arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public List<SnapshotMetadata> execute() throws IgniteException {
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            List<SnapshotMetadata> snpMeta = snpMgr.readSnapshotMetadatas(arg.snapshotName(), arg.snapshotPath());

            if (arg.incrementalIndex() > 0) {
                List<SnapshotMetadata> metas = snpMeta.stream()
                    .filter(m -> m.consistentId().equals(ignite.localNode().consistentId()))
                    .collect(Collectors.toList());

                if (metas.size() != 1) {
                    throw new IgniteException("Failed to find snapshot metafile [metas=" + metas +
                        ", snpName=" + arg.snapshotName() + ", snpPath=" + arg.snapshotPath() + ']');
                }

                checkIncrementalSnapshots(metas.get(0), arg);
            }

            return snpMeta;
        }

        /** Checks that all incremental snapshots are present, contain correct metafile and WAL segments. */
        public void checkIncrementalSnapshots(SnapshotMetadata fullMeta, SnapshotMetadataVerificationTaskArg arg) {
            try {
                IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

                // Incremental snapshot must contain ClusterSnapshotRecord.
                long startSeg = fullMeta.snapshotRecordPointer().index();

                for (int inc = 1; inc <= arg.incrementalIndex(); inc++) {
                    File incSnpDir = snpMgr.incrementalSnapshotLocalDir(arg.snapshotName(), arg.snapshotPath(), inc);

                    if (!incSnpDir.exists()) {
                        throw new IgniteException("No incremental snapshot found " +
                            "[snpName=" + arg.snapshotName() + ", snpPath=" + arg.snapshotPath() + ", incIdx=" + inc + ']');
                    }

                    String metaFileName = IgniteSnapshotManager.snapshotMetaFileName(ignite.localNode().consistentId().toString());

                    File metafile = incSnpDir.toPath().resolve(metaFileName).toFile();

                    IncrementalSnapshotMetadata incMeta = snpMgr.readFromFile(metafile);

                    if (!incMeta.matchBaseSnapshot(fullMeta)) {
                        throw new IgniteException("Incremental snapshot doesn't match full snapshot " +
                            "[incMeta=" + incMeta + ", fullMeta=" + fullMeta + ']');
                    }

                    if (incMeta.incrementalIndex() != inc) {
                        throw new IgniteException(
                            "Incremental snapshot meta has wrong index [expectedIdx=" + inc + ", meta=" + incMeta + ']');
                    }

                    checkWalSegments(incMeta, startSeg, incrementalSnapshotWalsDir(incSnpDir, incMeta.folderName()));

                    // Incremental snapshots must not cross each other.
                    startSeg = incMeta.incSnpPointer().index() + 1;
                }
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** Check that incremental snapshot contains all required WAL segments. Throws {@link IgniteException} in case of any errors. */
        private void checkWalSegments(IncrementalSnapshotMetadata meta, long startWalSeg, File incSnpWalDir) {
            IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

            List<FileDescriptor> walSeg = factory.resolveWalFiles(
                new IgniteWalIteratorFactory.IteratorParametersBuilder()
                    .filesOrDirs(incSnpWalDir.listFiles(file ->
                        FileWriteAheadLogManager.WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches())));

            if (walSeg.isEmpty())
                throw new IgniteException("No WAL segments found for incremental snapshot [dir=" + incSnpWalDir + ']');

            long actFirstSeg = walSeg.get(0).idx();

            if (actFirstSeg != startWalSeg) {
                throw new IgniteException("Missed WAL segment [expectFirstSegment=" + startWalSeg
                    + ", actualFirstSegment=" + actFirstSeg + ", meta=" + meta + ']');
            }

            long expLastSeg = meta.incSnpPointer().index();
            long actLastSeg = walSeg.get(walSeg.size() - 1).idx();

            if (actLastSeg != expLastSeg) {
                throw new IgniteException("Missed WAL segment [expectLastSegment=" + startWalSeg
                    + ", actualLastSegment=" + actFirstSeg + ", meta=" + meta + ']');
            }

            List<?> walSegGaps = factory.hasGaps(walSeg);

            if (!walSegGaps.isEmpty())
                throw new IgniteException("Missed WAL segments [misses=" + walSegGaps + ", meta=" + meta + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public @Nullable SnapshotMetadataVerificationTaskResult reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<ClusterNode, List<SnapshotMetadata>> reduceRes = new HashMap<>();
        Map<ClusterNode, Exception> exs = new HashMap<>();

        SnapshotMetadata first = null;

        for (ComputeJobResult res: results) {
            if (res.getException() != null) {
                exs.put(res.getNode(), res.getException());

                continue;
            }

            List<SnapshotMetadata> metas = res.getData();

            for (SnapshotMetadata meta : metas) {
                if (first == null)
                    first = meta;

                if (!first.sameSnapshot(meta)) {
                    exs.put(res.getNode(),
                        new IgniteException("An error occurred during comparing snapshot metadata from cluster nodes " +
                            "[first=" + first + ", meta=" + meta + ", nodeId=" + res.getNode().id() + ']'));

                    continue;
                }

                reduceRes.computeIfAbsent(res.getNode(), n -> new ArrayList<>())
                        .add(meta);
            }
        }

        return new SnapshotMetadataVerificationTaskResult(reduceRes, exs);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        // Handle all exceptions during the `reduce` operation.
        return ComputeJobResultPolicy.WAIT;
    }
}
