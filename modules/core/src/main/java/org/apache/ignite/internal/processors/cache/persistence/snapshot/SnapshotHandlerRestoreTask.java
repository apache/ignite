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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    /** {@inheritDoc} */
    @Override protected ComputeJob makeJob(String snpName, String constId, Collection<String> groups) {
        return new RestoreHandleJob(snpName, constId, groups);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Nullable @Override public SnapshotPartitionsVerifyTaskResult reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<String, List<SnapshotHandlerResult>> clusterResults = new HashMap<>();

        for (ComputeJobResult res : results) {
            // Unhandled exception.
            if (res.getException() != null)
                throw res.getException();

            Map<String, SnapshotHandlerResult> nodeDataMap = res.getData();

            for (Map.Entry<String, SnapshotHandlerResult> entry : nodeDataMap.entrySet()) {
                String lsnrName = entry.getKey();

                clusterResults.computeIfAbsent(lsnrName, v -> new ArrayList<>()).add(entry.getValue());
            }
        }

        String snpName = F.first(F.first(metas.values())).snapshotName();

        for (SnapshotHandler hnd : ignite.context().cache().context().snapshotMgr().handlers(SnapshotHandlerType.RESTORE)) {
            List<SnapshotHandlerResult> res = clusterResults.get(hnd.getClass().getName());

            if (res == null)
                continue;

            try {
                hnd.reduce(snpName, res);
            } catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        return new SnapshotPartitionsVerifyTaskResult(metas, null);
    }

    static class RestoreHandleJob extends ComputeJobAdapter {
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

        /** Cache groups to be restored from the snapshot. May be empty if all cache groups are being restored. */
        Collection<String> grps;

        /**
         * @param snpName Snapshot name.
         * @param consistentId String representation of the consistent node ID.
         * @param grps Cache groups to be restored from the snapshot. May be empty if all cache groups are being restored.
         */
        public RestoreHandleJob(String snpName, String consistentId, Collection<String> grps) {
            this.snpName = snpName;
            this.consistentId = consistentId;
            this.grps = grps;
        }

        /** {@inheritDoc} */
        @Override public Map<String, SnapshotHandlerResult<Object>> execute() {
            Map<String, SnapshotHandlerResult<Object>> resMap = new HashMap<>();
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();
            SnapshotMetadata meta = snpMgr.readSnapshotMetadata(snpName, consistentId);
            SnapshotHandlerContext ctx = new SnapshotHandlerContext(meta, grps);

            for (SnapshotHandler<?> hnd : snpMgr.handlers(SnapshotHandlerType.RESTORE)) {
                SnapshotHandlerResult<Object> res;

                try {
                    res = new SnapshotHandlerResult<>(hnd.handle(ctx), null, ignite.localNode());
                }
                catch (Exception e) {
                    res = new SnapshotHandlerResult<>(null, e, ignite.localNode());
                }

                resMap.put(hnd.getClass().getName(), res);
            }

            return resMap;
        }
    }
}
