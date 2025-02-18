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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot restore operation handling task.
 */
public class SnapshotHandlerRestoreTask extends AbstractSnapshotVerificationTask {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected SnapshotHandlerRestoreJob createJob(
        String name,
        String folderName,
        String consId,
        SnapshotPartitionsVerifyTaskArg args
    ) {
        return new SnapshotHandlerRestoreJob(name, args.snapshotPath(), folderName, consId, args.cacheGroupNames(), args.check());
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
    private static class SnapshotHandlerRestoreJob extends AbstractSnapshotVerificationJob {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param snpName Snapshot name.
         * @param snpPath Snapshot directory path.
         * @param folderName Folder name for snapshot.
         * @param consId Consistent id of the related node.
         * @param grps Cache group names.
         * @param check If {@code true} check snapshot before restore.
         */
        public SnapshotHandlerRestoreJob(
            String snpName,
            @Nullable String snpPath,
            String folderName,
            String consId,
            Collection<String> grps,
            boolean check
        ) {
            super(snpName, snpPath, folderName, consId, grps, check);
        }

        /** {@inheritDoc} */
        @Override public Map<String, SnapshotHandlerResult<Object>> execute0() {
            try {
                IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();
                SnapshotMetadata meta = snpMgr.readSnapshotMetadata(sft.meta());

                return snpMgr.handlers().invokeAll(SnapshotHandlerType.RESTORE,
                    new SnapshotHandlerContext(meta, rqGrps, ignite.localNode(), sft.root(), false, check));
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
        }
    }
}
