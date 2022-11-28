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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot restore operation handling task.
 */
public class SnapshotHandlerRestoreTask extends AbstractSnapshotVerificationTask {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Injected ignite logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override protected ComputeJob createJob(
        String name,
        @Nullable String path,
        String constId,
        Collection<String> groups
    ) {
        return new SnapshotHandlerRestoreJob(name, path, constId, groups);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("rawtypes")
    @Nullable @Override public SnapshotPartitionsVerifyTaskResult reduce(List<ComputeJobResult> results) {
        Map<String, List<SnapshotHandlerResult<?>>> clusterResults = new HashMap<>();
        Collection<UUID> execNodes = new ArrayList<>(results.size());

        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();

            // Depending on the job mapping, we can get several different results from one node.
            execNodes.add(res.getNode().id());

            Map<String, SnapshotHandlerResult> nodeDataMap = res.getData();

            assert nodeDataMap != null : "At least the default snapshot restore handler should have been executed ";

            for (Map.Entry<String, SnapshotHandlerResult> entry : nodeDataMap.entrySet()) {
                String hndName = entry.getKey();

                clusterResults.computeIfAbsent(hndName, v -> new ArrayList<>()).add(entry.getValue());
            }
        }

        String snapshotName = F.first(F.first(metas.values())).snapshotName();

        try {
            ignite.context().cache().context().snapshotMgr().handlers().completeAll(
                SnapshotHandlerType.RESTORE, snapshotName, clusterResults, execNodes, wrns -> {});
        }
        catch (Exception e) {
            log.warning("The snapshot operation will be aborted due to a handler error [snapshot=" + snapshotName + "].", e);

            throw new IgniteException(e);
        }

        return new SnapshotPartitionsVerifyTaskResult(metas, null);
    }

    /** Invokes all {@link SnapshotHandlerType#RESTORE} handlers locally. */
    private static class SnapshotHandlerRestoreJob extends ComputeJobAdapter {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Snapshot name. */
        private final String snpName;

        /** String representation of the consistent node ID. */
        private final String consistentId;

        /** Cache group names. */
        private final Collection<String> grps;

        /** Snapshot directory path. */
        private final String snpPath;

        /**
         * @param snpName Snapshot name.
         * @param snpPath Snapshot directory path.
         * @param consistentId String representation of the consistent node ID.
         * @param grps Cache group names.
         */
        public SnapshotHandlerRestoreJob(String snpName, @Nullable String snpPath, String consistentId, Collection<String> grps) {
            this.snpName = snpName;
            this.snpPath = snpPath;
            this.consistentId = consistentId;
            this.grps = grps;
        }

        /** {@inheritDoc} */
        @Override public Map<String, SnapshotHandlerResult<Object>> execute() {
            try {
                IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();
                File snpDir = snpMgr.snapshotLocalDir(snpName, snpPath);
                SnapshotMetadata meta = snpMgr.readSnapshotMetadata(snpDir, consistentId);

                return snpMgr.handlers().invokeAll(SnapshotHandlerType.RESTORE,
                    new SnapshotHandlerContext(meta, grps, ignite.localNode(), snpDir, false));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}
