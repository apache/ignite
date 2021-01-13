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

package org.apache.ignite.internal.visor.snapshot;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheGroupName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cachePartitions;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.partId;

/**
 * Task for checking snapshot partitions consistency the same way as {@link VerifyBackupPartitionsTaskV2} does.
 * Since a snapshot partitions already stored apart on disk the is no requirement for a cluster upcoming updates
 * to be hold on.
 */
@GridInternal
public class VerifySnapshotPartitionsTask extends VisorMultiNodeTask<VerifySnapshotPartitionsTaskArg, IdleVerifyResultV2, Map<PartitionKeyV2, PartitionHashRecordV2>> {
    /** {@inheritDoc} */
    @Override protected Map<? extends ComputeJob, ClusterNode> map0(
        List<ClusterNode> subgrid,
        VisorTaskArgument<VerifySnapshotPartitionsTaskArg> arg
    ) {
        // Nodes
        List<UUID> nodes = arg.getNodes();

        return super.map0(subgrid, arg);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable IdleVerifyResultV2 reduce0(List<ComputeJobResult> results) throws IgniteException {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected VisorJob<VerifySnapshotPartitionsTaskArg, Map<PartitionKeyV2, PartitionHashRecordV2>> job(
        VerifySnapshotPartitionsTaskArg arg) {
        return new VerifySnapshotPartitionsJob(arg, false);
    }

    /**
     * Job collects and calculates snapshot partition hashes on the node it executes.
     */
    private static class VerifySnapshotPartitionsJob extends VisorJob<VerifySnapshotPartitionsTaskArg, Map<PartitionKeyV2, PartitionHashRecordV2>> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /** Counter of processed partitions. */
        private final AtomicInteger completionCntr = new AtomicInteger(0);

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        public VerifySnapshotPartitionsJob(
            @Nullable VerifySnapshotPartitionsTaskArg arg,
            boolean debug
        ) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Map<PartitionKeyV2, PartitionHashRecordV2> run(
            @Nullable VerifySnapshotPartitionsTaskArg arg
        ) throws IgniteException {
            Map<PartitionKeyV2, PartitionHashRecordV2> res = new HashMap<>();
            List<T2<File, File>> pairs = new ArrayList<>();

            completionCntr.set(0);

            ignite.context().cache().context().snapshotMgr()
                .snapshotCacheDirectories(arg.snapshotName())
                .forEach(cacheDir ->
                    cachePartitions(cacheDir)
                        .forEach(partFile ->
                            pairs.add(new T2<>(cacheDir, partFile))));

            try {
                U.doInParallel(ignite.context().getSystemExecutorService(),
                    pairs,
                    pair -> {
                        String cacheGroupName = cacheGroupName(pair.get1());
                        int grpId = CU.cacheId(cacheGroupName);
                        int partId = partId(pair.get2().getName());

                        // IdleVerifyUtility.calculatePartitionHash()
                        // long updCntr,
                        // int grpId,
                        // int partId,
                        // String grpName,
                        // Object consId,
                        // GridDhtPartitionState state,
                        // boolean isPrimary,
                        // long partSize,
                        // GridIterator<CacheDataRow> it

                        res.putAll(IdleVerifyUtility.calculatePartitionHash(0,
                            grpId,
                            partId,
                            cacheGroupName,
                            "",
                            GridDhtPartitionState.OWNING,
                            true,
                            0,
                            ignite.context().cache().context().snapshotMgr()
                                .getPartitionDataRows(pair.get2(), grpId, partId, arg.checkCrc())));

                        completionCntr.incrementAndGet();

                        return null;
                    });
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return res;
        }
    }
}
