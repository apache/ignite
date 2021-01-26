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

package org.apache.ignite.internal.visor.snapshot;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheGroupName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cachePartitions;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.partId;

/** */
public class VisorVerifySnapshotPartitionsTask extends ComputeTaskAdapter<Void, IdleVerifyResultV2> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Snapshot metadata distribution for given snapshot. */
    private final Map<ClusterNode, List<SnapshotMetadata>> clusterMetas;

    /**
     * @param clusterMetas Snapshot metadata distribution for given snapshot.
     */
    public VisorVerifySnapshotPartitionsTask(
        Map<ClusterNode, List<SnapshotMetadata>> clusterMetas
    ) {
        this.clusterMetas = clusterMetas;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        @Nullable Void arg
    ) throws IgniteException {
        if (!subgrid.containsAll(clusterMetas.keySet())) {
            throw new IgniteException("Some of Ignite nodes left the cluster during execution " +
                "[curr=" + F.viewReadOnly(subgrid, F.node2id()) +
                ", init=" + F.viewReadOnly(clusterMetas.keySet(), F.node2id()) + ']');
        }

        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();
        Set<SnapshotMetadata> allParts = new HashSet<>();
        clusterMetas.values().forEach(allParts::addAll);

        for (int idx = 0; !allParts.isEmpty(); idx++) {
            for (Map.Entry<ClusterNode, List<SnapshotMetadata>> e : clusterMetas.entrySet()) {
                if (e.getValue().size() >= idx)
                    continue;

                SnapshotMetadata meta = e.getValue().get(idx);

                if (allParts.remove(meta)) {
                    jobs.put(new VisorVerifySnapshotPartitionsJob(meta.snapshotName(), meta.consistentId()),
                        e.getKey());
                }

                if (allParts.isEmpty())
                    break;
            }
        }

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public @Nullable IdleVerifyResultV2 reduce(List<ComputeJobResult> results) throws IgniteException {
        return VerifyBackupPartitionsTaskV2.reduce0(results);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        // Handle all exceptions during the `reduce` operation.
        return ComputeJobResultPolicy.WAIT;
    }

    /** Job that collects update counters of snapshot partitions on the node it executes. */
    private static class VisorVerifySnapshotPartitionsJob extends ComputeJobAdapter {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Snapshot name to validate. */
        private String snpName;

        /** Consistent snapshot metadata file name. */
        private String consId;

        /**
         * @param snpName Snapshot name to validate.
         * @param consId Consistent snapshot metadata file name.
         */
        public VisorVerifySnapshotPartitionsJob(String snpName, String consId) {
            this.snpName = snpName;
            this.consId = consId;
        }

        @Override public Map<PartitionKeyV2, PartitionHashRecordV2> execute() throws IgniteException {
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            if (log.isInfoEnabled()) {
                log.info("Verify snapshot partitions procedure has been initiated " +
                    "[snpName=" + snpName + ", consId=" + consId + ']');
            }

            SnapshotMetadata meta = snpMgr.readSnapshotMetadata(snpName, consId);
            Map<PartitionKeyV2, PartitionHashRecordV2> res = new HashMap<>();
            ByteBuffer pageBuf = ByteBuffer.allocate(meta.pageSize())
                .order(ByteOrder.nativeOrder());

            for (File dir : snpMgr.snapshotCacheDirectories(snpName, consId)) {
                String grpName = cacheGroupName(dir);
                int grpId = CU.cacheId(grpName);

                if (!meta.cacheGroupIds().contains(grpId)) {
                    throw new IgniteException("Snapshot data doesn't contain required cache group " +
                        "[grpName=" + grpName + ", snpName=" + snpName + ", consId=" + consId +
                        ", meta=" + meta + ']');
                }

                for (File part : cachePartitions(dir)) {
                    int partId = partId(part.getName());

                    if (!meta.partitions().get(grpId).contains(partId)) {
                        throw new IgniteException("Snapshot data doesn't contain required cache group partition " +
                            "[grpName=" + grpName + ", snpName=" + snpName + ", consId=" + consId +
                            ", partId=" + partId + ", meta=" + meta + ']');
                    }

                    PartitionKeyV2 key = new PartitionKeyV2(grpId, partId, grpName);

                    // Snapshot partitions must always be in OWNING state.
                    // There is no `primary` partitions for snapshot.
                    PartitionHashRecordV2 rec = new PartitionHashRecordV2(key, false, consId,
                        PartitionHashRecordV2.PartitionState.OWNING);

                    snpMgr.readSnapshotPartitionMeta(part, grpId, partId, pageBuf, rec::updateCounter, rec::size);
                    res.put(key, rec);
                }
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            VisorVerifySnapshotPartitionsJob job = (VisorVerifySnapshotPartitionsJob)o;

            return snpName.equals(job.snpName) && consId.equals(job.consId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(snpName, consId);
        }
    }
}
