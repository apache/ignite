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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * The task for checking the consistency of snapshots in the cluster.
 */
public abstract class AbstractSnapshotVerificationTask extends
    ComputeTaskAdapter<SnapshotPartitionsVerifyTaskArg, SnapshotPartitionsVerifyTaskResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Map of snapshot metadata information found on each cluster node. */
    protected final Map<ClusterNode, List<SnapshotMetadata>> metas = new HashMap<>();

    /** Ignite instance. */
    @IgniteInstanceResource
    protected IgniteEx ignite;

    /** Injected logger. */
    @LoggerResource
    protected IgniteLogger log;

    /** {@inheritDoc} */
    @Override public Map<ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, SnapshotPartitionsVerifyTaskArg arg) {
        Map<ClusterNode, List<SnapshotMetadata>> clusterMetas = arg.clusterMetadata();

        if (!subgrid.containsAll(clusterMetas.keySet())) {
            throw new IgniteSnapshotVerifyException(F.asMap(ignite.localNode(),
                new IgniteException("Some of Ignite nodes left the cluster during the snapshot verification " +
                    "[curr=" + F.viewReadOnly(subgrid, F.node2id()) +
                    ", init=" + F.viewReadOnly(clusterMetas.keySet(), F.node2id()) + ']')));
        }

        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();
        Set<SnapshotMetadata> allMetas = new HashSet<>();
        clusterMetas.values().forEach(allMetas::addAll);
        metas.putAll(clusterMetas);

        while (!allMetas.isEmpty()) {
            for (Map.Entry<ClusterNode, List<SnapshotMetadata>> e : clusterMetas.entrySet()) {
                SnapshotMetadata meta = F.find(e.getValue(), null, allMetas::remove);

                if (meta == null)
                    continue;

                jobs.put(createJob(meta.snapshotName(), meta.folderName(), meta.consistentId(), arg), e.getKey());

                if (allMetas.isEmpty())
                    break;
            }
        }

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        // Handle all exceptions during the `reduce` operation.
        return ComputeJobResultPolicy.WAIT;
    }

    /**
     * @param name Snapshot name.
     * @param folderName Folder name for snapshot.
     * @param consId Consistent id of the related node.
     * @param args Check snapshot parameters.
     *
     * @return Compute job.
     */
    protected abstract AbstractSnapshotVerificationJob createJob(
        String name,
        String folderName,
        String consId,
        SnapshotPartitionsVerifyTaskArg args
    );

    /** */
    protected abstract static class AbstractSnapshotVerificationJob extends ComputeJobAdapter {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        protected IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        protected IgniteLogger log;

        /** Snapshot name. */
        protected final String snpName;

        /** Snapshot directory path. */
        @Nullable protected final String snpPath;

        /** Folder name for snapshot. */
        protected final String folderName;

        /** Consistent id of the snapshot data. */
        protected final String consId;

        /** Set of cache groups to be checked in the snapshot. {@code Null} or empty to check everything. */
        @Nullable protected final Collection<String> rqGrps;

        /** If {@code true}, calculates and compares partition hashes. Otherwise, only basic snapshot validation is launched. */
        protected final boolean check;

        /** Snapshot file tree. */
        protected transient SnapshotFileTree sft;

        /**
         * @param snpName Snapshot name.
         * @param snpPath Snapshot directory path.
         * @param folderName Folder name for snapshot.
         * @param consId Consistent id of the related node.
         * @param rqGrps Set of cache groups to be checked in the snapshot. {@code Null} or empty to check everything.
         * @param check If {@code true}, calculates and compares partition hashes. Otherwise, only basic snapshot validation is launched.
         */
        protected AbstractSnapshotVerificationJob(
            String snpName,
            @Nullable String snpPath,
            String folderName,
            String consId,
            @Nullable Collection<String> rqGrps,
            boolean check
        ) {
            this.snpName = snpName;
            this.snpPath = snpPath;
            this.folderName = folderName;
            this.consId = consId;
            this.rqGrps = rqGrps;
            this.check = check;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            sft = new SnapshotFileTree(ignite.context(), snpName, snpPath, folderName, consId);

            return execute0();
        }

        /** Exectues actual job. */
        protected abstract Object execute0();
    }
}
