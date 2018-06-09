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
package org.apache.ignite.internal.processors.cache.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Task for comparing update counters and checksums between primary and backup partitions of specified caches.
 * <br>
 * Argument: Set of cache names, 'null' will trigger verification for all caches.
 * <br>
 * Result: {@link IdleVerifyResultV2} with conflict partitions.
 * <br>
 * Works properly only on idle cluster - there may be false positive conflict reports if data in cluster is being
 * concurrently updated.
 */
@GridInternal
public class VerifyBackupPartitionsTaskV2 extends ComputeTaskAdapter<VisorIdleVerifyTaskArg, IdleVerifyResultV2> {
    /** First version of Ignite that is capable of executing Idle Verify V2. */
    public static final IgniteProductVersion V2_SINCE_VER = IgniteProductVersion.fromString("2.5.3");

    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid, VisorIdleVerifyTaskArg arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        for (ClusterNode node : subgrid)
            jobs.put(new VerifyBackupPartitionsTask.VerifyBackupPartitionsJob(arg.getCaches()), node);

        return jobs;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IdleVerifyResultV2 reduce(List<ComputeJobResult> results)
        throws IgniteException {
        Map<PartitionKey, List<PartitionHashRecord>> clusterHashes = new HashMap<>();

        for (ComputeJobResult res : results) {
            Map<PartitionKey, PartitionHashRecord> nodeHashes = res.getData();

            for (Map.Entry<PartitionKey, PartitionHashRecord> e : nodeHashes.entrySet()) {
                List<PartitionHashRecord> records = clusterHashes.computeIfAbsent(e.getKey(), k -> new ArrayList<>());

                records.add(e.getValue());
            }
        }

        Map<PartitionKey, List<PartitionHashRecord>> hashConflicts = new HashMap<>();

        Map<PartitionKey, List<PartitionHashRecord>> updateCntrConflicts = new HashMap<>();

        Map<PartitionKey, List<PartitionHashRecord>> movingParts = new HashMap<>();

        for (Map.Entry<PartitionKey, List<PartitionHashRecord>> e : clusterHashes.entrySet()) {
            Integer partHash = null;
            Long updateCntr = null;

            for (PartitionHashRecord record : e.getValue()) {
                if (record.size() == PartitionHashRecord.MOVING_PARTITION_SIZE) {
                    List<PartitionHashRecord> records = movingParts.computeIfAbsent(
                        e.getKey(), k -> new ArrayList<>());

                    records.add(record);

                    continue;
                }

                if (partHash == null) {
                    partHash = record.partitionHash();

                    updateCntr = record.updateCounter();
                }
                else {
                    if (record.updateCounter() != updateCntr)
                        updateCntrConflicts.putIfAbsent(e.getKey(), e.getValue());

                    if (record.partitionHash() != partHash)
                        hashConflicts.putIfAbsent(e.getKey(), e.getValue());
                }
            }
        }

        return new IdleVerifyResultV2(updateCntrConflicts, hashConflicts, movingParts);
    }
}
