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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;

/**
 * Verification task for restoring a cache group from a snapshot.
 */
public class SnapshotRestoreVerificatioTask extends
    ComputeTaskAdapter<SnapshotRestoreVerificationArg, SnapshotRestoreVerificationResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        SnapshotRestoreVerificationArg arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        for (ClusterNode node : subgrid)
            jobs.put(new SnapshotRestoreVerificationJob(arg), node);

        return jobs;
    }
    SnapshotRestoreRequest
    /** {@inheritDoc} */
    @Override public SnapshotRestoreVerificationResult reduce(List<ComputeJobResult> results) throws IgniteException {
        SnapshotRestoreVerificationResult firstRes = null;

        for (ComputeJobResult jobRes : results) {
            SnapshotRestoreVerificationResult res = jobRes.getData();

            if (res == null)
                continue;

            if (firstRes == null) {
                firstRes = res;

                continue;
            }

            if (firstRes.configs().size() != res.configs().size()) {
                throw new IgniteException("Count of cache configs mismatch [" +
                    "node1=" + firstRes.localNodeId() + ", cnt1=" + firstRes.configs().size() +
                    ", node2=" + res.localNodeId() + ", cnt2=" + res.configs().size() + ']');
            }
        }

        return firstRes;
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

        /** Job argument. */
        private final SnapshotRestoreVerificationArg arg;

        /**
         * @param arg Job argument.
         */
        public SnapshotRestoreVerificationJob(SnapshotRestoreVerificationArg arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            assert !ignite.context().clientNode();

            try {
                return resolveRestoredConfigs();
            }
            catch (BinaryObjectException e) {
                throw new IgniteException("Incompatible binary types found: " + e.getMessage());
            } catch (IOException | IgniteCheckedException e) {
                throw F.wrap(e);
            }
        }

        /**
         * Collect cache configurations and verify binary compatibility of specified cache groups.
         *
         * @return List of stored cache configurations with local node ID.
         * @throws IgniteCheckedException If the snapshot is incompatible.
         * @throws IOException In case of I/O errors while reading the memory page size
         */
        private SnapshotRestoreVerificationResult resolveRestoredConfigs() throws IgniteCheckedException, IOException {
            GridCacheSharedContext<?, ?> cctx = ignite.context().cache().context();
            Map<String, StoredCacheData> cacheCfgs = new HashMap<>();

            // Collect cache configuration(s) and verify cache groups page size.
            for (String grpName : arg.groups()) {
                File cacheDir = cctx.snapshotMgr().resolveSnapshotCacheDir(arg.snapshotName(), grpName);

                if (!cacheDir.exists())
                    return null;

                ((FilePageStoreManager)cctx.pageStore()).readCacheConfigurations(cacheDir, cacheCfgs);

                File[] parts = cacheDir.listFiles(f -> f.getName().startsWith(PART_FILE_PREFIX) && !f.isDirectory());

                if (F.isEmpty(parts))
                    continue;

                int pageSize =
                    ((GridCacheDatabaseSharedManager)cctx.database()).resolvePageSizeFromPartitionFile(parts[0].toPath());

                if (pageSize != cctx.database().pageSize()) {
                    throw new IgniteCheckedException("Incompatible memory page size " +
                        "[snapshotPageSize=" + pageSize +
                        ", nodePageSize=" + cctx.database().pageSize() +
                        ", group=" + grpName +
                        ", snapshot=" + arg.snapshotName() + ']');
                }
            }

            if (cacheCfgs.isEmpty())
                return null;

            File binDir = binaryWorkDir(cctx.snapshotMgr().snapshotLocalDir(arg.snapshotName()).getAbsolutePath(),
                ignite.context().pdsFolderResolver().resolveFolders().folderName());

            ignite.context().cacheObjects().checkMetadata(binDir);

            return new SnapshotRestoreVerificationResult(new ArrayList<>(cacheCfgs.values()), ignite.localNode().id());
        }
    }
}
