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
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
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

                jobs.put(
                    createJob(
                        arg.requestId(),
                        meta.snapshotName(),
                        arg.snapshotPath(),
                        arg.incrementIndex(),
                        meta.consistentId(),
                        arg.cacheGroupNames(),
                        arg.check()
                    ),
                    e.getKey()
                );

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
     * @param reqId Snapshot request id.
     * @param name Snapshot name.
     * @param path Snapshot directory path.
     * @param incIdx Incremental snapshot index.
     * @param constId Snapshot metadata file name.
     * @param groups Cache groups to be restored from the snapshot. May be empty if all cache groups are being restored.
     * @param check If {@code true} check snapshot before restore.
     * @return Compute job.
     */
    protected abstract AbstractSnapshotPartitionsVerifyJob createJob(
        UUID reqId,
        String name,
        @Nullable String path,
        int incIdx,
        String constId,
        Collection<String> groups,
        boolean check
    );

    /** */
    abstract static class AbstractSnapshotPartitionsVerifyJob extends ComputeJobAdapter {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        protected IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        protected IgniteLogger log;

        /** Snapshot operation request id. */
        protected final UUID reqId;

        /** Snapshot name. */
        protected final String snpName;

        /** Snapshot directory path. */
        @Nullable protected final String snpPath;

        /** Consistent ID. */
        protected final String consId;

        /** Set of cache groups to be checked in the snapshot or {@code empty} to check everything. */
        protected final Collection<String> rqGrps;

        /** If {@code true} check snapshot before restore. */
        protected final boolean check;

        /**
         * @param reqId Snapshot operation request id.
         * @param snpName Snapshot name.
         * @param snpPath Snapshot directory path.
         * @param consId Consistent snapshot metadata file name.
         * @param rqGrps Set of cache groups to be checked in the snapshot or {@code empty} to check everything.
         * @param check If {@code true} check snapshot before restore.
         */
        protected AbstractSnapshotPartitionsVerifyJob(
            UUID reqId,
            String snpName,
            @Nullable String snpPath,
            String consId,
            Collection<String> rqGrps,
            boolean check
        ) {
            this.reqId = reqId;
            this.snpName = snpName;
            this.snpPath = snpPath;
            this.consId = consId;
            this.rqGrps = rqGrps;
            this.check = check;
        }
    }
}
