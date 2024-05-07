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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
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
    @Override public @NotNull Map<MetadataVerificationJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        SnapshotMetadataVerificationTaskArg arg
    ) throws IgniteException {
        Map<MetadataVerificationJob, ClusterNode> map = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid)
            map.put(new MetadataVerificationJob(arg), node);

        return map;
    }

    /** Job that verifies snapshot on an Ignite node. */
    public static class MetadataVerificationJob extends ComputeJobAdapter {
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

            return execute(
                snpMgr.readSnapshotMetadatas(arg.snapshotName(), arg.snapshotPath()),
                ignite.localNode().consistentId(),
                arg,
                ignite.context().config().getEncryptionSpi().masterKeyDigest(),
                ignite.context().compress()
            );
        }

        /**
         * @param metas Metadatas to validate.
         * @param nodeConsistId Consistent if of the related node.
         * @param arg Command arguments.
         * @param masterKeyDigest Masterkey digest for encrypted snapshots.
         * @param compressionProc Compression processor. If {@code null} and snapshot has any compressed cache,
         *                        {@link UnsupportedOperationException} is thrown.
         * @return {@code metas}.
         */
        public List<SnapshotMetadata> execute(
            List<SnapshotMetadata> metas,
            Object nodeConsistId,
            SnapshotMetadataVerificationTaskArg arg,
            byte[] masterKeyDigest,
            @Nullable CompressionProcessor compressionProc
        ) {
            if (metas.isEmpty()) {
                throw error(metas, true, "Snapshot metadata not found [snapshot=" + arg.snapshotName()
                    + (arg.snapshotPath() != null ? ", baseDir=" + arg.snapshotPath() : "") + ", consistentId="
                    + nodeConsistId + ']');
            }

            for (SnapshotMetadata meta : metas) {
                byte[] snpMasterKeyDigest = meta.masterKeyDigest();

                if (masterKeyDigest == null && snpMasterKeyDigest != null) {
                    throw error(metas, true, "Snapshot '" + meta.snapshotName() + "' has encrypted caches " +
                        "while encryption is disabled. To restore this snapshot, start Ignite with configured " +
                        "encryption and the same master key.");
                }

                if (snpMasterKeyDigest != null && !Arrays.equals(snpMasterKeyDigest, masterKeyDigest)) {
                    throw error(metas, true, "Snapshot '" + meta.snapshotName() + "' has different master " +
                        "key digest. To restore this snapshot, start Ignite with the same master key.");
                }

                Collection<Integer> grpIds = new HashSet<>(F.isEmpty(arg.grpIds()) ? meta.cacheGroupIds() : arg.grpIds());

                if (meta.hasCompressedGroups() && grpIds.stream().anyMatch(meta::isGroupWithCompression)) {
                    try {
                        if (compressionProc == null) {
                            throw new UnsupportedOperationException("Compression processor is not set. Compressed " +
                                "snapshots aren't supported.");
                        }

                        compressionProc.checkPageCompressionSupported();
                    }
                    catch (IgniteCheckedException e) {
                        String grpWithCompr = grpIds.stream().filter(meta::isGroupWithCompression)
                            .map(String::valueOf).collect(Collectors.joining(", "));

                        String msg = "Requested cache groups [" + grpWithCompr + "] for check " +
                            "from snapshot '" + meta.snapshotName() + "' are compressed while " +
                            "disk page compression is disabled. To check these groups please " +
                            "start Ignite with ignite-compress module in classpath";

                        throw error(metas, false, msg);
                    }
                }

                grpIds.removeAll(meta.partitions().keySet());

                if (!grpIds.isEmpty() && !new HashSet<>(meta.cacheGroupIds()).containsAll(grpIds)) {
                    throw error(metas, true, "Cache group(s) was not found in the snapshot [groups=" +
                        grpIds + ", snapshot=" + arg.snapshotName() + ']');
                }
            }

            if (arg.incrementIndex() > 0)
                checkIncrementalSnapshots(metas.get(0), arg);

            return metas;
        }

        /** Checks that all incremental snapshots are present, contain correct metafile and WAL segments. */
        public void checkIncrementalSnapshots(SnapshotMetadata fullMeta, SnapshotMetadataVerificationTaskArg arg) {
            try {
                IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

                // Incremental snapshot must contain ClusterSnapshotRecord.
                long startSeg = fullMeta.snapshotRecordPointer().index();

                for (int inc = 1; inc <= arg.incrementIndex(); inc++) {
                    File incSnpDir = snpMgr.incrementalSnapshotLocalDir(arg.snapshotName(), arg.snapshotPath(), inc);

                    if (!incSnpDir.exists()) {
                        throw new IgniteException("No incremental snapshot found " +
                            "[snpName=" + arg.snapshotName() + ", snpPath=" + arg.snapshotPath() + ", incrementIndex=" + inc + ']');
                    }

                    String metaFileName = IgniteSnapshotManager.snapshotMetaFileName(ignite.localNode().consistentId().toString());

                    File metafile = incSnpDir.toPath().resolve(metaFileName).toFile();

                    IncrementalSnapshotMetadata incMeta = snpMgr.readFromFile(metafile);

                    if (!incMeta.matchBaseSnapshot(fullMeta)) {
                        throw new IgniteException("Incremental snapshot doesn't match full snapshot " +
                            "[incMeta=" + incMeta + ", fullMeta=" + fullMeta + ']');
                    }

                    if (incMeta.incrementIndex() != inc) {
                        throw new IgniteException(
                            "Incremental snapshot meta has wrong index [expectedIdx=" + inc + ", meta=" + incMeta + ']');
                    }

                    checkWalSegments(incMeta, startSeg, incrementalSnapshotWalsDir(incSnpDir, incMeta.folderName()));

                    // Incremental snapshots must not cross each other.
                    startSeg = incMeta.incrementalSnapshotPointer().index() + 1;
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

            long expLastSeg = meta.incrementalSnapshotPointer().index();
            long actLastSeg = walSeg.get(walSeg.size() - 1).idx();

            if (actLastSeg != expLastSeg) {
                throw new IgniteException("Missed WAL segment [expectLastSegment=" + startWalSeg
                    + ", actualLastSegment=" + actFirstSeg + ", meta=" + meta + ']');
            }

            List<?> walSegGaps = factory.hasGaps(walSeg);

            if (!walSegGaps.isEmpty())
                throw new IgniteException("Missed WAL segments [misses=" + walSegGaps + ", meta=" + meta + ']');
        }

        /** */
        private static InvalidMetadataException error(List<SnapshotMetadata> metas, boolean badParams, String errTxt) {
            return new InvalidMetadataException(metas, badParams ? new IllegalArgumentException(errTxt)
                : new IllegalStateException(errTxt));
        }
    }

    /** {@inheritDoc} */
    @Override public SnapshotMetadataVerificationTaskResult reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<ClusterNode, List<SnapshotMetadata>> reduceRes = new HashMap<>();
        Map<ClusterNode, Exception> exs = new HashMap<>();

        SnapshotMetadata first = null;

        boolean knownExceptions = true;

        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                exs.put(res.getNode(), res.getException());

                if (!(res.getException() instanceof InvalidMetadataException))
                    knownExceptions = false;

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

                reduceRes.computeIfAbsent(res.getNode(), n -> new ArrayList<>()).add(meta);
            }
        }

        return new SnapshotMetadataVerificationTaskResult(reduceRes, exs, !F.isEmpty(exs) && knownExceptions);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        // Handle all exceptions during the `reduce` operation.
        return ComputeJobResultPolicy.WAIT;
    }

    /** */
    private static final class InvalidMetadataException extends IgniteException {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        final List<SnapshotMetadata> nodeMetas;

        /** */
        final Exception nodeErr;

        /** */
        private InvalidMetadataException(List<SnapshotMetadata> metas, Exception err) {
            super(err);

            nodeMetas = metas;
            nodeErr = err;
        }
    }
}
