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

import java.io.IOException;
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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree.IncrementalSnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.F;

/** Snapshot task to verify snapshot metadata on the baseline nodes for given snapshot name. */
public class SnapshotMetadataVerificationTask {
    /** */
    private final MetadataVerificationJob job;

    /** */
    public SnapshotMetadataVerificationTask(
        IgniteEx ignite,
        IgniteLogger log,
        SnapshotFileTree sft,
        int incrementIndex,
        Collection<Integer> grpIds
    ) {
        job = new MetadataVerificationJob(ignite, log, sft, incrementIndex, grpIds);
    }

    /** */
    public List<SnapshotMetadata> execute() {
        return job.execute();
    }

    /** Job that verifies snapshot on an Ignite node. */
    private static class MetadataVerificationJob {
        /** */
        private final IgniteEx ignite;

        /** */
        private final IgniteLogger log;

        /** */
        private final SnapshotFileTree sft;

        /** */
        private final int incrementIdx;

        /** */
        private final Collection<Integer> grpIds;

        /** */
        private MetadataVerificationJob(
            IgniteEx ignite,
            IgniteLogger log,
            SnapshotFileTree sft,
            int incrementIdx,
            Collection<Integer> grpIds
        ) {
            this.ignite = ignite;
            this.sft = sft;
            this.incrementIdx = incrementIdx;
            this.grpIds = grpIds;
            this.log = log;
        }

        /** */
        public List<SnapshotMetadata> execute() {
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            List<SnapshotMetadata> snpMeta = snpMgr.readSnapshotMetadatas(sft);

            for (SnapshotMetadata meta : snpMeta)
                checkMeta(meta);

            if (incrementIdx > 0) {
                List<SnapshotMetadata> metas = snpMeta.stream()
                    .filter(m -> m.consistentId().equals(sft.consistentId()))
                    .collect(Collectors.toList());

                if (metas.size() != 1) {
                    throw new IgniteException("Failed to find single snapshot metafile on local node [locNodeId="
                        + ignite.localNode().consistentId() + ", metas=" + snpMeta + ", snpName=" + sft.name()
                        + ", snpPath=" + sft.root() + "]. Incremental snapshots requires exactly one meta file " +
                        "per node because they don't support restoring on a different topology.");
                }

                checkIncrementalSnapshots(metas.get(0), sft, incrementIdx);
            }

            return snpMeta;
        }

        /** */
        private void checkMeta(SnapshotMetadata meta) {
            byte[] snpMasterKeyDigest = meta.masterKeyDigest();
            byte[] masterKeyDigest = ignite.context().config().getEncryptionSpi().masterKeyDigest();

            if (masterKeyDigest == null && snpMasterKeyDigest != null) {
                throw new IllegalStateException("Snapshot '" + meta.snapshotName() + "' has encrypted caches " +
                    "while encryption is disabled. To restore this snapshot, start Ignite with configured " +
                    "encryption and the same master key.");
            }

            if (snpMasterKeyDigest != null && !Arrays.equals(snpMasterKeyDigest, masterKeyDigest)) {
                throw new IllegalStateException("Snapshot '" + meta.snapshotName() + "' has different master " +
                    "key digest. To restore this snapshot, start Ignite with the same master key.");
            }

            Collection<Integer> grpIds = new HashSet<>(F.isEmpty(this.grpIds) ? meta.cacheGroupIds() : this.grpIds);

            if (meta.hasCompressedGroups() && grpIds.stream().anyMatch(meta::isGroupWithCompression)) {
                try {
                    ignite.context().compress().checkPageCompressionSupported();
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
                    ", snapshot=" + sft.name() + ']');
            }
        }

        /** Checks that all incremental snapshots are present, contain correct metafile and WAL segments. */
        public void checkIncrementalSnapshots(SnapshotMetadata fullMeta, SnapshotFileTree sft, int incIdx) {
            try {
                GridCacheSharedContext<Object, Object> ctx = ignite.context().cache().context();

                IgniteSnapshotManager snpMgr = ctx.snapshotMgr();

                // Incremental snapshot must contain ClusterSnapshotRecord.
                long startSeg = fullMeta.snapshotRecordPointer().index();

                for (int inc = 1; inc <= incIdx; inc++) {
                    IncrementalSnapshotFileTree ift = sft.incrementalSnapshotFileTree(inc);

                    if (!ift.root().exists()) {
                        throw new IllegalArgumentException("No incremental snapshot found " +
                            "[snpName=" + sft.name() + ", snpPath=" + sft.root() + ", incrementIndex=" + inc + ']');
                    }

                    IncrementalSnapshotMetadata incMeta = snpMgr.readIncrementalSnapshotMetadata(ift.meta());

                    if (!incMeta.matchBaseSnapshot(fullMeta)) {
                        throw new IllegalArgumentException("Incremental snapshot doesn't match full snapshot " +
                            "[incMeta=" + incMeta + ", fullMeta=" + fullMeta + ']');
                    }

                    if (incMeta.incrementIndex() != inc) {
                        throw new IgniteException(
                            "Incremental snapshot meta has wrong index [expectedIdx=" + inc + ", meta=" + incMeta + ']');
                    }

                    checkWalSegments(incMeta, startSeg, ift);

                    // Incremental snapshots must not cross each other.
                    startSeg = incMeta.incrementalSnapshotPointer().index() + 1;
                }
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** Check that incremental snapshot contains all required WAL segments. Throws {@link IgniteException} in case of any errors. */
        private void checkWalSegments(IncrementalSnapshotMetadata meta, long startWalSeg, IncrementalSnapshotFileTree ift) {
            IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

            List<FileDescriptor> walSeg = factory.resolveWalFiles(
                new IgniteWalIteratorFactory.IteratorParametersBuilder()
                    .filesOrDirs(ift.walCompactedSegments()));

            if (walSeg.isEmpty())
                throw new IgniteException("No WAL segments found for incremental snapshot [dir=" + ift.wal() + ']');

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
    }

    /** */
    public Map<ClusterNode, Exception> reduce(Map<ClusterNode, List<SnapshotMetadata>> results) throws IgniteException {
        Map<ClusterNode, Exception> exs = new HashMap<>();

        SnapshotMetadata first = null;
        Set<String> baselineMetasLeft = Collections.emptySet();

        for (Map.Entry<ClusterNode, List<SnapshotMetadata>> res : results.entrySet()) {
            List<SnapshotMetadata> metas = res.getValue();

            for (SnapshotMetadata meta : metas) {
                if (first == null) {
                    first = meta;

                    baselineMetasLeft = new HashSet<>(meta.baselineNodes());
                }

                baselineMetasLeft.remove(meta.consistentId());

                if (!first.sameSnapshot(meta)) {
                    exs.put(res.getKey(),
                        new IgniteException("An error occurred during comparing snapshot metadata from cluster nodes " +
                            "[first=" + first + ", meta=" + meta + ", nodeId=" + res.getKey().id() + ']'));
                }
            }
        }

        if (first == null && exs.isEmpty()) {
            SnapshotFileTree sft = job.sft;

            throw new IllegalArgumentException("Snapshot does not exists [snapshot=" + sft.name()
                + ", baseDir=" + sft.root() + ", consistentId=" + sft.consistentId() + ']');
        }

        if (!F.isEmpty(baselineMetasLeft) && F.isEmpty(exs)) {
            throw new IgniteException("No snapshot metadatas found for the baseline nodes " +
                "with consistent ids: " + String.join(", ", baselineMetasLeft));
        }

        return exs;
    }
}
