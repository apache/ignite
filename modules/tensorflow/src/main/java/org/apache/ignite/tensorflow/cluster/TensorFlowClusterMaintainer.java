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

package org.apache.ignite.tensorflow.cluster;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessState;
import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessStatus;

/**
 * TensorFlow cluster service that maintains TensorFlow cluster.
 */
public class TensorFlowClusterMaintainer implements Service {
    /** */
    private static final long serialVersionUID = -3220563310643566419L;

    /** Upstream cache name. */
    private final String cacheName;

    /** Topic name. */
    private final String topicName;

    /** TensorFlow cluster manager. */
    private final TensorFlowClusterManager clusterMgr;

    /** Previous partition mapping. */
    private UUID[] prev;

    /**
     * Constructs a new instance of TensorFlow cluster service.
     *
     * @param cacheName Upstream cache name.
     * @param topicName Topic name.
     */
    public TensorFlowClusterMaintainer(String cacheName, String topicName) {
        assert cacheName != null : "Cache name should not be null";
        assert topicName != null : "Topic name should not be null";

        this.clusterMgr = new TensorFlowClusterManager((Supplier<Ignite> & Serializable)Ignition::ignite);
        this.cacheName = cacheName;
        this.topicName = topicName;
    }

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        clusterMgr.stopClusterIfExists(cacheName);
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) {
        clusterMgr.init();
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        while (!ctx.isCancelled()) {
            Thread.sleep(1000);

            boolean restartRequired = hasAffinityChanged();

            if (!restartRequired) {
                TensorFlowCluster cluster = clusterMgr.getCache().get(cacheName);
                Map<UUID, List<LongRunningProcessStatus>> statuses = clusterMgr.getSrvProcMgr()
                    .ping(cluster.getProcesses());

                for (UUID nodeId : statuses.keySet()) {
                    for (LongRunningProcessStatus status : statuses.get(nodeId)) {
                        if (status.getState().equals(LongRunningProcessState.DONE)) {
                            restartRequired = true;
                            break;
                        }
                    }
                }
            }

            if (restartRequired) {
                clusterMgr.stopClusterIfExists(cacheName);

                TensorFlowCluster cluster =  clusterMgr.getOrCreateCluster(cacheName);

                IgniteMessaging messaging = Ignition.ignite().message();
                messaging.send(topicName, cluster);
            }
        }
    }

    /**
     * Checks if affinity mapping has been changed.
     *
     * @return True if mapping has been changed, otherwise false.
     */
    private boolean hasAffinityChanged() {
        Affinity<?> affinity = Ignition.ignite().affinity(cacheName);

        int parts = affinity.partitions();

        UUID[] ids = new UUID[parts];

        for (int part = 0; part < parts; part++) {
            ClusterNode node = affinity.mapPartitionToNode(part);
            UUID nodeId = node.id();
            ids[part] = nodeId;
        }

        if (prev == null || !Arrays.equals(ids, prev)) {
            prev = ids;
            return true;
        }

        return false;
    }
}
