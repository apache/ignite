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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;

/**
 * The task for checking the consistency of snapshots in the cluster.
 *
 * @param <R> Type of the task result returning from {@link ComputeTask#reduce(List)} method.
 */
public abstract class AbstractSnapshotVerificationTask extends
    ComputeTaskAdapter<SnapshotPartitionsVerifyTaskArg, SnapshotPartitionsVerifyTaskResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Task argument. */
    protected final Map<ClusterNode, List<SnapshotMetadata>> metas = new HashMap<>();

    /** Ignite instance. */
    @IgniteInstanceResource
    protected IgniteEx ignite;

    /** Cache groups to be restored from the snapshot. May be empty if all cache groups are being restored. */
    protected Collection<String> grps;

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        SnapshotPartitionsVerifyTaskArg arg) throws IgniteException {
        grps = arg.cacheGroupNames();

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

        metas.putAll(clusterMetas);

        while (!allMetas.isEmpty()) {
            for (Map.Entry<ClusterNode, List<SnapshotMetadata>> e : clusterMetas.entrySet()) {
                SnapshotMetadata meta = F.find(e.getValue(), null, allMetas::remove);

                if (meta == null)
                    continue;

                jobs.put(makeJob(meta.snapshotName(), meta.consistentId(), arg.cacheGroupNames()),
                    e.getKey());

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
     * @param constId Snapshot metadata file name.
     * @param groups Cache groups to be restored from the snapshot. May be empty if all cache groups are being restored.
     * @return Compute job.
     */
    protected abstract ComputeJob makeJob(String name, String constId, Collection<String> groups);
}
