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
import java.util.Collection;
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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            return snpMgr.checker().checkLocalMetas(snpMgr.snapshotLocalDir(arg.snapshotName(), arg.snapshotPath()),
                arg.incrementIndex(), arg.grpIds(), ignite.localNode().consistentId()).join();
        }
    }

    /** {@inheritDoc} */
    @Override public @Nullable SnapshotMetadataVerificationTaskResult reduce(
        List<ComputeJobResult> results) throws IgniteException {
        Map<ClusterNode, List<SnapshotMetadata>> reduceRes = new HashMap<>();
        Map<ClusterNode, Exception> exs = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                exs.put(res.getNode(), res.getException());

                continue;
            }

            if (!F.isEmpty((Collection<?>)res.getData()))
                reduceRes.computeIfAbsent(res.getNode(), n -> new ArrayList<>()).addAll(res.getData());
        }

        exs = SnapshotChecker.reduceMetasResults(arg.snapshotName(), arg.snapshotPath(), reduceRes, exs, ignite.localNode().consistentId());

        return new SnapshotMetadataVerificationTaskResult(reduceRes, exs);
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        // Handle all exceptions during the `reduce` operation.
        return ComputeJobResultPolicy.WAIT;
    }
}
