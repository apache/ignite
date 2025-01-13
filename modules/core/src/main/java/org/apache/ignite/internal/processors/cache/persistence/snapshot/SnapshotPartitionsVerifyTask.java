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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.cache.PartitionKeyV2;
import org.apache.ignite.internal.management.cache.VerifyBackupPartitionsTask;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.management.cache.VerifyBackupPartitionsTask.reduce0;

/**
 * Task for checking snapshot partitions consistency the same way as {@link VerifyBackupPartitionsTask} does.
 * Since a snapshot partitions already stored apart on disk the is no requirement for a cluster upcoming updates
 * to be hold on.
 */
@GridInternal
public class SnapshotPartitionsVerifyTask extends AbstractSnapshotVerificationTask {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VerifySnapshotPartitionsJob createJob(String name, String consId, SnapshotPartitionsVerifyTaskArg args) {
        return new VerifySnapshotPartitionsJob(name, args.snapshotPath(), consId, args.cacheGroupNames(), args.check());
    }

    /** {@inheritDoc} */
    @Override public @Nullable SnapshotPartitionsVerifyTaskResult reduce(List<ComputeJobResult> results) throws IgniteException {
        return new SnapshotPartitionsVerifyTaskResult(metas, reduce0(results));
    }

    /** Job that collects update counters of snapshot partitions on the node it executes. */
    private static class VerifySnapshotPartitionsJob extends AbstractSnapshotVerificationJob {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param snpName Snapshot name to validate.
         * @param consId Consistent id of the related node.
         * @param rqGrps Set of cache groups to be checked in the snapshot or {@code empty} to check everything.
         * @param snpPath Snapshot directory path.
         * @param check If {@code true} check snapshot before restore.
         */
        public VerifySnapshotPartitionsJob(
            String snpName,
            @Nullable String snpPath,
            String consId,
            Collection<String> rqGrps,
            boolean check
        ) {
            super(snpName, snpPath, consId, rqGrps, check);
        }

        /** {@inheritDoc} */
        @Override public Map<PartitionKeyV2, PartitionHashRecordV2> execute() throws IgniteException {
            GridCacheSharedContext<?, ?> cctx = ignite.context().cache().context();

            if (log.isInfoEnabled()) {
                log.info("Verify snapshot partitions procedure has been initiated " +
                    "[snpName=" + snpName + ", consId=" + consId + ']');
            }

            try {
                File snpDir = cctx.snapshotMgr().snapshotLocalDir(snpName, snpPath);
                SnapshotMetadata meta = cctx.snapshotMgr().readSnapshotMetadata(snpDir, consId);

                return new SnapshotPartitionsVerifyHandler(cctx)
                    .invoke(new SnapshotHandlerContext(meta, rqGrps, ignite.localNode(), snpDir, false, check));
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
            finally {
                if (log.isInfoEnabled()) {
                    log.info("Verify snapshot partitions procedure has been finished " +
                        "[snpName=" + snpName + ", consId=" + consId + ']');
                }
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            VerifySnapshotPartitionsJob job = (VerifySnapshotPartitionsJob)o;

            return snpName.equals(job.snpName) && consId.equals(job.consId) &&
                Objects.equals(rqGrps, job.rqGrps) && Objects.equals(snpPath, job.snpPath) &&
                Objects.equals(check, job.check);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(snpName, consId, rqGrps, snpPath, check);
        }
    }
}
