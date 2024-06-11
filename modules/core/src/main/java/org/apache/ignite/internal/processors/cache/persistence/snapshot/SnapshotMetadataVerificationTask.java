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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.incrementalSnapshotWalsDir;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.snapshotMetaFileName;

/** Snapshot task to verify snapshot metadata on the baseline nodes for given snapshot name. */
@GridInternal
public class SnapshotMetadataVerificationTask
      extends ComputeTaskAdapter<SnapshotMetadataVerificationTaskArg, SnapshotMetadataVerificationTaskResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private SnapshotMetadataVerificationTaskArg arg;

    /** */
    @IgniteInstanceResource
    private transient IgniteEx ignite;

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        SnapshotMetadataVerificationTaskArg arg
    ) throws IgniteException {
        this.arg = arg;

        Map<ComputeJob, ClusterNode> map = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid)
            map.put(new MetadataVerificationJob(arg), node);

        return map;
    }

    /** */
    public static List<SnapshotMetadata> readAndCheckMetas(
        GridKernalContext kctx,
        String snpName,
        @Nullable String snpPath,
        int incIdx,
        @Nullable Collection<Integer> grpIds
    ) {
        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        List<SnapshotMetadata> snpMeta = snpMgr.readSnapshotMetadatas(snpName, snpPath);

        Object consId = kctx.cluster().get().localNode().consistentId();

        for (SnapshotMetadata meta : snpMeta)
            checkMeta(meta, kctx, grpIds);

        if (incIdx > 0) {
            List<SnapshotMetadata> metas = snpMeta.stream()
                .filter(m -> m.consistentId().equals(consId))
                .collect(Collectors.toList());

            if (metas.size() != 1) {
                throw new IgniteException("Failed to find single snapshot metafile on local node [locNodeId="
                    + consId + ", metas=" + snpMeta + ", snpName=" + snpName
                    + ", snpPath=" + snpPath + "]. Incremental snapshots requires exactly one meta file " +
                    "per node because they don't support restoring on a different topology.");
            }

            checkIncrementalSnapshots(kctx, metas.get(0), snpName, snpPath, incIdx);
        }

        return snpMeta;
    }

    /** */
    private static void checkMeta(SnapshotMetadata meta, GridKernalContext kctx, @Nullable Collection<Integer> grpIds) {
        byte[] snpMasterKeyDigest = meta.masterKeyDigest();
        byte[] masterKeyDigest = kctx.config().getEncryptionSpi().masterKeyDigest();

        if (masterKeyDigest == null && snpMasterKeyDigest != null) {
            throw new IllegalStateException("Snapshot '" + meta.snapshotName() + "' has encrypted caches " +
                "while encryption is disabled. To restore this snapshot, start Ignite with configured " +
                "encryption and the same master key.");
        }

        if (snpMasterKeyDigest != null && !Arrays.equals(snpMasterKeyDigest, masterKeyDigest)) {
            throw new IllegalStateException("Snapshot '" + meta.snapshotName() + "' has different master " +
                "key digest. To restore this snapshot, start Ignite with the same master key.");
        }

        grpIds = new HashSet<>(F.isEmpty(grpIds) ? meta.cacheGroupIds() : grpIds);

        if (meta.hasCompressedGroups() && grpIds.stream().anyMatch(meta::isGroupWithCompression)) {
            try {
                kctx.compress().checkPageCompressionSupported();
            }
            catch (NullPointerException | IgniteCheckedException e) {
                String grpWithCompr = grpIds.stream().filter(meta::isGroupWithCompression)
                    .map(String::valueOf).collect(Collectors.joining(", "));

                String msg = "Requested cache groups [" + grpWithCompr + "] for check " +
                    "from snapshot '" + meta.snapshotName() + "' are compressed while " +
                    "disk page compression is disabled. To check these groups please " +
                    "start Ignite with ignite-compress module in classpath";

                throw new IllegalStateException(msg);
            }
        }

        grpIds.removeAll(meta.partitions().keySet());

        if (!grpIds.isEmpty() && !new HashSet<>(meta.cacheGroupIds()).containsAll(grpIds)) {
            throw new IllegalArgumentException("Cache group(s) was not found in the snapshot [groups=" + grpIds +
                ", snapshot=" + meta.snapshotName() + ']');
        }
    }

    /** Checks that all incremental snapshots are present, contain correct metafile and WAL segments. */
    private static void checkIncrementalSnapshots(
        GridKernalContext kctx,
        SnapshotMetadata fullMeta,
        String snpName,
        @Nullable String snpPath,
        int incIdx
    ) {
        try {
            IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

            // Incremental snapshot must contain ClusterSnapshotRecord.
            long startSeg = fullMeta.snapshotRecordPointer().index();

            for (int inc = 1; inc <= incIdx; inc++) {
                File incSnpDir = snpMgr.incrementalSnapshotLocalDir(snpName, snpPath, inc);

                if (!incSnpDir.exists()) {
                    throw new IllegalArgumentException("No incremental snapshot found " +
                        "[snpName=" + snpName + ", snpPath=" + snpPath + ", incrementIndex=" + inc + ']');
                }

                String metaFileName = snapshotMetaFileName(kctx.cluster().get().localNode().consistentId().toString());

                File metafile = incSnpDir.toPath().resolve(metaFileName).toFile();

                IncrementalSnapshotMetadata incMeta = snpMgr.readFromFile(metafile);

                if (!incMeta.matchBaseSnapshot(fullMeta)) {
                    throw new IllegalArgumentException("Incremental snapshot doesn't match full snapshot " +
                        "[incMeta=" + incMeta + ", fullMeta=" + fullMeta + ']');
                }

                if (incMeta.incrementIndex() != inc) {
                    throw new IgniteException(
                        "Incremental snapshot meta has wrong index [expectedIdx=" + inc + ", meta=" + incMeta + ']');
                }

                checkWalSegments(incMeta, startSeg, incrementalSnapshotWalsDir(incSnpDir, incMeta.folderName()),
                    kctx.log(SnapshotMetadataVerificationTask.class));

                // Incremental snapshots must not cross each other.
                startSeg = incMeta.incrementalSnapshotPointer().index() + 1;
            }
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private static void checkWalSegments(IncrementalSnapshotMetadata meta, long startWalSeg, File incSnpWalDir, IgniteLogger log) {
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

        /** Job that verifies snapshot on an Ignite node. */
    private static class MetadataVerificationJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** */
        private final SnapshotMetadataVerificationTaskArg arg;

        /** */
        public MetadataVerificationJob(SnapshotMetadataVerificationTaskArg arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public List<SnapshotMetadata> execute() {
            return readAndCheckMetas(ignite.context(), arg.snapshotName(), arg.snapshotPath(), arg.incrementIndex(), arg.grpIds());
        }
    }

    /** {@inheritDoc} */
    @Override public SnapshotMetadataVerificationTaskResult reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<ClusterNode, List<SnapshotMetadata>> metas = results.stream().filter(res -> res.getException() == null)
            .collect(Collectors.toMap(ComputeJobResult::getNode, ComputeJobResult::getData));

        Map<ClusterNode, Exception> errors = results.stream().filter(res -> res.getException() != null)
            .collect(Collectors.toMap(ComputeJobResult::getNode, ComputeJobResult::getException));

        return reduceClusterResults(metas, errors, arg.snapshotName(), arg.snapshotPath(), ignite.localNode());
    }

    /** */
    public static SnapshotMetadataVerificationTaskResult reduceClusterResults(
        Map<ClusterNode, List<SnapshotMetadata>> results,
        Map<ClusterNode, Exception> errors,
        String snpName,
        @Nullable String snpPath,
        ClusterNode locNode
    ) {
        assert errors != null;
        assert results != null;

        Map<ClusterNode, List<SnapshotMetadata>> reduceRes = new HashMap<>();
        Map<ClusterNode, Exception> exs = new HashMap<>();

        Collection<ClusterNode> nodes = new HashSet<>(results.keySet());
        nodes.addAll(errors.keySet());

        SnapshotMetadata first = null;
        Set<String> baselineMetasLeft = Collections.emptySet();

        for (ClusterNode node : nodes) {
            Exception nodeErr = errors.get(node);

            if (nodeErr != null) {
                exs.put(node, nodeErr);

                continue;
            }

            List<SnapshotMetadata> metas = results.get(node);

            assert metas != null;

            for (SnapshotMetadata meta : metas) {
                if (first == null) {
                    first = meta;

                    baselineMetasLeft = new HashSet<>(meta.baselineNodes());
                }

                baselineMetasLeft.remove(meta.consistentId());

                if (!first.sameSnapshot(meta)) {
                    exs.put(node,
                        new IgniteException("An error occurred during comparing snapshot metadata from cluster nodes " +
                            "[first=" + first + ", meta=" + meta + ", nodeId=" + node.id() + ']'));

                    continue;
                }

                reduceRes.computeIfAbsent(node, n -> new ArrayList<>()).add(meta);
            }
        }

        if (first == null && exs.isEmpty()) {
            assert !results.isEmpty();

            for (ClusterNode node : nodes) {
                Exception e = new IllegalArgumentException("Snapshot does not exists [snapshot=" + snpName
                    + (snpPath != null ? ", baseDir=" + snpPath : "") + ", consistentId="
                    + node.consistentId() + ']');

                exs.put(node, e);
            }
        }

        if (!F.isEmpty(baselineMetasLeft) && F.isEmpty(exs)) {
            exs.put(locNode, new IgniteException("No snapshot metadatas found for the baseline nodes " +
                "with consistent ids: " + String.join(", ", baselineMetasLeft)));
        }

        return new SnapshotMetadataVerificationTaskResult(reduceRes, exs);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        // Handle all exceptions during the `reduce` operation.
        return ComputeJobResultPolicy.WAIT;
    }
}
