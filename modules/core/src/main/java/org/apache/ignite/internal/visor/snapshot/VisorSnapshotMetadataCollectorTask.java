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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.sameSnapshotMetadata;

/** Snapshot task to collect snapshot metadata from the baseline nodes for given snapshot name. */
@GridInternal
public class VisorSnapshotMetadataCollectorTask
    extends VisorMultiNodeTask<String, Map<ClusterNode, List<SnapshotMetadata>>, List<SnapshotMetadata>> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected @Nullable Map<ClusterNode, List<SnapshotMetadata>> reduce0(List<ComputeJobResult> results) throws IgniteException {
        Map<ClusterNode, List<SnapshotMetadata>> reduceRes = new HashMap<>();

        SnapshotMetadata first = null;

        for (ComputeJobResult res: results) {
            if (res.getException() != null) {
                throw new IgniteException("An error occurred while getting snapshot metadata " +
                    "from baseline nodes: " + res.getNode().id(), res.getException());
            }

            Set<SnapshotMetadata> metas = res.getData();

            for (SnapshotMetadata meta : metas) {
                if (first == null)
                    first = meta;

                if (!sameSnapshotMetadata(first, meta)) {
                    throw new IgniteException("An error occurred during comparing snapshot metadata from cluster nodes " +
                        "[first=" + first + ", meta=" + meta + ", nodeId=" + res.getNode().id() + ']');
                }

                reduceRes.computeIfAbsent(res.getNode(), n -> new ArrayList<>())
                    .add(meta);
            }
        }

        return reduceRes;
    }

    /** {@inheritDoc} */
    @Override protected VisorJob<String, List<SnapshotMetadata>> job(String snpName) {
        return new VisorSnapshotMetadataCollectorJob(snpName);
    }

    /** Compute job which collects snapshot metadata files on the node it run. */
    private static class VisorSnapshotMetadataCollectorJob
        extends VisorJob<String, List<SnapshotMetadata>> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Snapshot arguments to check.
         */
        public VisorSnapshotMetadataCollectorJob(@Nullable String arg) {
            super(arg, false);
        }

        /** {@inheritDoc} */
        @Override protected List<SnapshotMetadata> run(@Nullable String snpName) throws IgniteException {
            return ignite.context().cache().context().snapshotMgr().localSnapshotMetadata(snpName);
        }
    }
}
