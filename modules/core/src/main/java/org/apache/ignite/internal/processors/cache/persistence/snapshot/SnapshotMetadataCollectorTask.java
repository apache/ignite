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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Snapshot task to collect snapshot metadata from the baseline nodes for given snapshot name. */
@GridInternal
public class SnapshotMetadataCollectorTask
    extends ComputeTaskAdapter<String, Map<ClusterNode, List<SnapshotMetadata>>> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        @Nullable String snpName
    ) throws IgniteException {
        Map<ComputeJob, ClusterNode> map = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid) {
            map.put(new ComputeJobAdapter(snpName) {
                        @IgniteInstanceResource
                        private transient IgniteEx ignite;

                        @Override public List<SnapshotMetadata> execute() throws IgniteException {
                            return ignite.context().cache().context().snapshotMgr()
                                .readSnapshotMetadatas(snpName);
                        }
                    }, node);
        }

        return map;
    }

    @Override public @Nullable Map<ClusterNode, List<SnapshotMetadata>> reduce(
        List<ComputeJobResult> results
    ) throws IgniteException {
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

        if (exs.isEmpty())
            return reduceRes;
        else
            throw new IgniteSnapshotVerifyException(exs);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        // Handle all exceptions during the `reduce` operation.
        return ComputeJobResultPolicy.WAIT;
    }
}
