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

package org.apache.ignite.ml.dlearn.context.cache.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.ml.dlearn.utils.DLearnContextPartitionKey;

/**
 * This affinity function is used to identify a partition by key and node to place the partition. This function is
 * initialized with {@link #initAssignment} parameter which contains information about upstream cache distribution
 * across the cluster. This information allows function to place context partitions on the same nodes as partitions
 * of the upstream cache. Be aware that this affinity functions supports only {@link DLearnContextPartitionKey} keys.
 */
public class DLearnPartitionAffinityFunction implements AffinityFunction {
    /** */
    private static final long serialVersionUID = 7735390384525189270L;

    /**
     * Initial distribution of the partitions (copy of upstream cache partitions distribution).
     */
    private final List<UUID> initAssignment;

    /**
     * Version of the topology used to collect the {@link #initAssignment}.
     */
    private final long initTopVer;

    /**
     * Number of partition backups.
     */
    private final int backups;

    /**
     * Creates new instance of d-learn partition affinity function initialized with initial distribution.
     *
     * @param initAssignment initial distribution of the partitions (copy of upstream cache partitions distribution)
     * @param initTopVer version of the topology used to collect the {@link #initAssignment}
     * @param backups number of partition backups
     */
    public DLearnPartitionAffinityFunction(List<UUID> initAssignment, long initTopVer, int backups) {
        this.initAssignment = initAssignment;
        this.initTopVer = initTopVer;
        this.backups = backups;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // do nothing
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return initAssignment.size();
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        BinaryObject bo = (BinaryObject) key;
        DLearnContextPartitionKey datasetPartKey = bo.deserialize();
        return datasetPartKey.getPart();
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        long currTopVer = affCtx.currentTopologyVersion().topologyVersion();
        List<List<ClusterNode>> assignment = new ArrayList<>(initAssignment.size());
        if (currTopVer == initTopVer) {
            Map<UUID, ClusterNode> topSnapshotIdx = new HashMap<>();
            for (ClusterNode node : affCtx.currentTopologySnapshot())
                topSnapshotIdx.put(node.id(), node);
            for (int part = 0; part < initAssignment.size(); part++) {
                UUID partNodeId = initAssignment.get(part);
                ClusterNode partNode = topSnapshotIdx.get(partNodeId);
                assignment.add(Collections.singletonList(partNode));
            }
            return assignment;
        }
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // do nothing
    }
}
