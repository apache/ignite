/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Snapshot restore management task.
 */
abstract class SnapshotRestoreManagementTask extends ComputeTaskAdapter<String, Boolean> {
   /**
     * @param param Compute job argument.
     * @return Compute job.
     */
    protected abstract ComputeJob makeJob(String param);

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        String snpName
    ) throws IgniteException {
        Map<ComputeJob, ClusterNode> map = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid)
            map.put(makeJob(snpName), node);

        return map;
    }

    /** {@inheritDoc} */
    @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteException {
        boolean ret = false;

        for (ComputeJobResult r : results) {
            if (r.getException() != null)
                throw new IgniteException("Failed to execute job [nodeId=" + r.getNode().id() + ']', r.getException());

            ret |= Boolean.TRUE.equals(r.getData());
        }

        return ret;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        // Handle all exceptions during the `reduce` operation.
        return ComputeJobResultPolicy.WAIT;
    }
}
