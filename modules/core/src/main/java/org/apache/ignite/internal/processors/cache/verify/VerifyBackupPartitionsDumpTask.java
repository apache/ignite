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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task for collection checksums primary and backup partitions of specified caches.
 * <br> Argument: Set of cache names, 'null' will trigger verification for all caches.
 * <br> Result: {@link IdleVerifyDumpResult} with all found partitions.
 * <br> Works properly only on idle cluster - there may be false positive conflict reports if data in cluster is being
 * concurrently updated.
 */
@GridInternal
public class VerifyBackupPartitionsDumpTask extends ComputeTaskAdapter<VisorIdleVerifyTaskArg, IdleVerifyDumpResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Delegate for map execution */
    private final VerifyBackupPartitionsTaskV2 delegate = new VerifyBackupPartitionsTaskV2();

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid, VisorIdleVerifyTaskArg arg) throws IgniteException {
        return delegate.map(subgrid, arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IdleVerifyDumpResult reduce(List<ComputeJobResult> results)
        throws IgniteException {
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> clusterHashes = new TreeMap<>(buildPartitionKeyComparator());

        for (ComputeJobResult res : results) {
            Map<PartitionKeyV2, PartitionHashRecordV2> nodeHashes = res.getData();

            for (Map.Entry<PartitionKeyV2, PartitionHashRecordV2> e : nodeHashes.entrySet()) {
                clusterHashes
                    .computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                    .add(e.getValue());
            }
        }

        Comparator<PartitionHashRecordV2> recordV2Comparator = buildRecordComparator().reversed();

        Map<PartitionKeyV2, List<PartitionHashRecordV2>> partitions = new LinkedHashMap<>();

        for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : clusterHashes.entrySet()) {
            entry.getValue().sort(recordV2Comparator);

            partitions.put(entry.getKey(), entry.getValue());
        }

        return new IdleVerifyDumpResult(partitions);
    }

    /**
     * @return Comparator for {@link PartitionHashRecordV2}.
     */
    @NotNull private Comparator<PartitionHashRecordV2> buildRecordComparator() {
        return (o1, o2) -> {
            int compare = Boolean.compare(o1.isPrimary(), o2.isPrimary());

            if (compare != 0)
                return compare;

            return o1.consistentId().toString().compareTo(o2.consistentId().toString());
        };
    }

    /**
     * @return Comparator for {@link PartitionKeyV2}.
     */
    @NotNull private Comparator<PartitionKeyV2> buildPartitionKeyComparator() {
        return (o1, o2) -> {
            int compare = Integer.compare(o1.groupId(), o2.groupId());

            if (compare != 0)
                return compare;

            return Integer.compare(o1.partitionId(), o2.partitionId());
        };
    }
}
