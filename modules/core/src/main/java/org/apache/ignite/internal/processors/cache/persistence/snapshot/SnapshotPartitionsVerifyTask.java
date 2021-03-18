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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.fromOrdinal;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheGroupName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cachePartitionFiles;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.partId;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getTypeByPartId;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.calculatePartitionHash;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.checkPartitionsPageCrcSum;

/**
 * Task for checking snapshot partitions consistency the same way as {@link VerifyBackupPartitionsTaskV2} does.
 * Since a snapshot partitions already stored apart on disk the is no requirement for a cluster upcoming updates
 * to be hold on.
 */
@GridInternal
public class SnapshotPartitionsVerifyTask
    extends ComputeTaskAdapter<Map<ClusterNode, List<SnapshotMetadata>>, IdleVerifyResultV2> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        @Nullable Map<ClusterNode, List<SnapshotMetadata>> clusterMetas
    ) throws IgniteException {
        if (!subgrid.containsAll(clusterMetas.keySet())) {
            throw new IgniteSnapshotVerifyException(F.asMap(ignite.localNode(),
                new IgniteException("Some of Ignite nodes left the cluster during the snapshot verification " +
                "[curr=" + F.viewReadOnly(subgrid, F.node2id()) +
                ", init=" + F.viewReadOnly(clusterMetas.keySet(), F.node2id()) + ']')));
        }

        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();
        Set<SnapshotMetadata> allMetas = new HashSet<>();
        clusterMetas.values().forEach(allMetas::addAll);

        Set<String> missed = null;

        for (SnapshotMetadata meta : allMetas) {
            if (missed == null)
                missed = new HashSet<>(meta.baselineNodes());

            missed.remove(meta.consistentId());

            if (missed.isEmpty())
                break;
        }

        if (!missed.isEmpty()) {
            throw new IgniteSnapshotVerifyException(F.asMap(ignite.localNode(),
                new IgniteException("Some metadata is missing from the snapshot: " + missed)));
        }

        while (!allMetas.isEmpty()) {
            for (Map.Entry<ClusterNode, List<SnapshotMetadata>> e : clusterMetas.entrySet()) {
                SnapshotMetadata meta = F.find(e.getValue(), null, allMetas::remove);

                if (meta == null)
                    continue;

                jobs.put(new VisorVerifySnapshotPartitionsJob(meta.snapshotName(), meta.consistentId()), e.getKey());

                if (allMetas.isEmpty())
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
        private final String snpName;

        /** Consistent snapshot metadata file name. */
        private final String consId;

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
            Set<Integer> grps = new HashSet<>(meta.partitions().keySet());
            Set<File> partFiles = new HashSet<>();

            for (File dir : snpMgr.snapshotCacheDirectories(snpName, meta.folderName())) {
                int grpId = CU.cacheId(cacheGroupName(dir));

                if (!grps.remove(grpId))
                    continue;

                Set<Integer> parts = new HashSet<>(meta.partitions().get(grpId));

                for (File part : cachePartitionFiles(dir)) {
                    int partId = partId(part.getName());

                    if (!parts.remove(partId))
                        continue;

                    partFiles.add(part);
                }

                if (!parts.isEmpty()) {
                    throw new IgniteException("Snapshot data doesn't contain required cache group partition " +
                        "[grpId=" + grpId + ", snpName=" + snpName + ", consId=" + consId +
                        ", missed=" + parts + ", meta=" + meta + ']');
                }
            }

            if (!grps.isEmpty()) {
                throw new IgniteException("Snapshot data doesn't contain required cache groups " +
                    "[grps=" + grps + ", snpName=" + snpName + ", consId=" + consId +
                    ", meta=" + meta + ']');
            }

            Map<PartitionKeyV2, PartitionHashRecordV2> res = new ConcurrentHashMap<>();
            ThreadLocal<ByteBuffer> buff = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(meta.pageSize())
                .order(ByteOrder.nativeOrder()));

            try {
                U.doInParallel(
                    ignite.context().getSystemExecutorService(),
                    partFiles,
                    part -> {
                        String grpName = cacheGroupName(part.getParentFile());
                        int grpId = CU.cacheId(grpName);
                        int partId = partId(part.getName());

                        FilePageStoreManager storeMgr = (FilePageStoreManager)ignite.context().cache().context().pageStore();

                        try {
                            try (FilePageStore pageStore = (FilePageStore)storeMgr.getPageStoreFactory(grpId, false)
                                .createPageStore(getTypeByPartId(partId),
                                    part::toPath,
                                    val -> {
                                    })
                            ) {
                                if (partId == INDEX_PARTITION) {
                                    checkPartitionsPageCrcSum(() -> pageStore, INDEX_PARTITION, FLAG_IDX);

                                    return null;
                                }

                                ByteBuffer pageBuff = buff.get();
                                pageBuff.clear();
                                pageStore.read(0, pageBuff, true);

                                long pageAddr = GridUnsafe.bufferAddress(pageBuff);

                                PagePartitionMetaIO io = PageIO.getPageIO(pageBuff);
                                GridDhtPartitionState partState = fromOrdinal(io.getPartitionState(pageAddr));

                                if (partState != OWNING) {
                                    throw new IgniteCheckedException("Snapshot partitions must be in the OWNING " +
                                        "state only: " + partState);
                                }

                                long updateCntr = io.getUpdateCounter(pageAddr);
                                long size = io.getSize(pageAddr);

                                if (log.isDebugEnabled()) {
                                    log.debug("Partition [grpId=" + grpId
                                        + ", id=" + partId
                                        + ", counter=" + updateCntr
                                        + ", size=" + size + "]");
                                }

                                // Snapshot partitions must always be in OWNING state.
                                // There is no `primary` partitions for snapshot.
                                PartitionKeyV2 key = new PartitionKeyV2(grpId, partId, grpName);

                                PartitionHashRecordV2 hash = calculatePartitionHash(key,
                                    updateCntr,
                                    consId,
                                    GridDhtPartitionState.OWNING,
                                    false,
                                    size,
                                    snpMgr.partitionRowIterator(snpName, meta.folderName(), grpName, partId));

                                assert hash != null : "OWNING must have hash: " + key;

                                res.put(key, hash);
                            }
                        }
                        catch (IOException e) {
                            throw new IgniteCheckedException(e);
                        }

                        return null;
                    }
                );
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
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
