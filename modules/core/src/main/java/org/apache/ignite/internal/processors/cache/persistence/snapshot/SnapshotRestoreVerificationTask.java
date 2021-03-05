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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;

/**
 * Verification task for restoring a cache group from a snapshot.
 */
public class SnapshotRestoreVerificationTask extends ComputeTaskAdapter<Void, Map<UUID, List<StoredCacheData>>> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Snapshot name. */
    private final String snpName;

    /** List of cache group names to restore from the snapshot. */
    @GridToStringInclude
    private final Collection<String> grps;

    /**
     * @param snpName Snapshot name.
     * @param grps List of cache group names to restore from the snapshot.
     */
    public SnapshotRestoreVerificationTask(String snpName, Collection<String> grps) {
        this.snpName = snpName;
        this.grps = grps;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        Void arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        for (ClusterNode node : subgrid)
            jobs.put(new SnapshotRestoreVerificationJob(snpName, grps), node);

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, List<StoredCacheData>> reduce(List<ComputeJobResult> results) throws IgniteException {
        Map<UUID, List<StoredCacheData>> resMap = new HashMap<>();
        ComputeJobResult firstNodeRes = null;

        for (ComputeJobResult jobRes : results) {
            List<StoredCacheData> res = jobRes.getData();

            if (res == null)
                continue;

            resMap.put(jobRes.getNode().id(), res);

            if (firstNodeRes == null) {
                firstNodeRes = jobRes;

                continue;
            }

            int expCfgCnt = ((Collection<StoredCacheData>)firstNodeRes.getData()).size();

            if (expCfgCnt != res.size()) {
                throw new IgniteException("Count of cache configs mismatch [" +
                    "node1=" + firstNodeRes.getNode().id() + ", cnt1=" + expCfgCnt +
                    ", node2=" + jobRes.getNode().id() + ", cnt2=" + res.size() + ']');
            }
        }

        return resMap;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        IgniteException e = res.getException();

        // Don't failover this job, if topology changed - user should restart operation.
        if (e != null)
            throw e;

        return super.result(res, rcvd);
    }

    /** */
    private static class SnapshotRestoreVerificationJob extends ComputeJobAdapter {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** Snapshot name. */
        private final String snpName;

        /** List of cache group names to restore from the snapshot. */
        @GridToStringInclude
        private final Collection<String> grps;

        /**
         * @param snpName Snapshot name.
         * @param grps List of cache group names to restore from the snapshot.
         */
        public SnapshotRestoreVerificationJob(String snpName, Collection<String> grps) {
            this.snpName = snpName;
            this.grps = grps;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            assert !ignite.context().clientNode();

            try {
                Map<String, StoredCacheData> ccfgs = new HashMap<>();
                GridCacheSharedContext<?, ?> cctx = ignite.context().cache().context();
                String folderName = ignite.context().pdsFolderResolver().resolveFolders().folderName();

                List<SnapshotMetadata> metas = cctx.snapshotMgr().readSnapshotMetadatas(snpName);

                if (F.isEmpty(metas))
                    return null;

                SnapshotMetadata meta = metas.get(0);

                if (!meta.consistentId().equals(cctx.localNode().consistentId().toString()))
                    return null;

                if (meta.pageSize() != cctx.database().pageSize()) {
                    throw new IgniteCheckedException("Incompatible memory page size " +
                        "[snapshotPageSize=" + meta.pageSize() +
                        ", local=" + cctx.database().pageSize() +
                        ", snapshot=" + snpName +
                        ", nodeId=" + cctx.localNodeId() + ']');
                }

                // Collect cache configuration(s) and verify cache groups page size.
                for (File cacheDir : cctx.snapshotMgr().snapshotCacheDirectories(snpName, folderName)) {
                    String grpName = FilePageStoreManager.cacheGroupName(cacheDir);

                    if (!grps.contains(grpName))
                        continue;

                    ((FilePageStoreManager)cctx.pageStore()).readCacheConfigurations(cacheDir, ccfgs);
                }

                if (ccfgs.isEmpty())
                    return null;

                File binDir = binaryWorkDir(
                    cctx.snapshotMgr().snapshotLocalDir(snpName).getAbsolutePath(),
                    folderName);

                ignite.context().cacheObjects().checkMetadata(binDir);

                return new ArrayList<>(ccfgs.values());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}
