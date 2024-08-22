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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
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
    @Override protected SnapshotHandlerRestoreJob createJob(String name, String consId, SnapshotPartitionsVerifyTaskArg args) {
        return new SnapshotHandlerRestoreJob(name, args.snapshotPath(), consId, args.cacheGroupNames(), args.check());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("rawtypes")
    @Nullable @Override public SnapshotPartitionsVerifyTaskResult reduce(List<ComputeJobResult> results) {
        String snpName = F.first(F.first(metas.values())).snapshotName();

        Map<ClusterNode, Map<String, SnapshotHandlerResult<?>>> resMap = new HashMap<>();

        results.forEach(jobRes -> {
            if (jobRes.getException() != null)
                throw jobRes.getException();
            else
                resMap.put(jobRes.getNode(), jobRes.getData());
        });

        try {
            ignite.context().cache().context().snapshotMgr().checker().checkCustomHandlersResults(snpName, resMap);
        }
        catch (Exception e) {
            log.warning("The snapshot operation will be aborted due to a handler error [snapshot=" + snpName + "].", e);

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
         * @param consId Consistent id of the related node.
         * @param grps Cache group names.
         * @param check If {@code true} check snapshot before restore.
         */
        public SnapshotHandlerRestoreJob(
            String snpName,
            @Nullable String snpPath,
            String consId,
            Collection<String> grps,
            boolean check
        ) {
            super(snpName, snpPath, consId, grps, check);
        }

        /** {@inheritDoc} */
        @Override public Map<String, SnapshotHandlerResult<Object>> execute() {
            try {
                IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

                return snpMgr.checker().invokeCustomHandlers(snpName, consId, snpPath, rqGrps, check).get();
            }
            catch (Exception e) {
                throw new IgniteException("Filed to invoke all the snapshot validation handlers.", e);
            }
        }
    }
}
