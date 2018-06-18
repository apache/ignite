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
public class TensorFlowClusterService implements Service {
    /** */
    private static final long serialVersionUID = -3220563310643566419L;

    /** Upstream cache name. */
    private final String cacheName;

    /** TensorFlow cluster manager. */
    private final TensorFlowClusterManager clusterMgr;

    /** Previous partition mapping. */
    private UUID[] prev;

    /**
     * Constructs a new instance of TensorFlow cluster service.
     *
     * @param cacheName Upstream cache name.
     */
    public TensorFlowClusterService(String cacheName) {
        assert cacheName != null : "Cache name should not be null";

        this.clusterMgr = new TensorFlowClusterManager((Supplier<Ignite> & Serializable)Ignition::ignite);
        this.cacheName = cacheName;
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
        int cnt = 0;
        while (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(1000);
            cnt++;

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
                clusterMgr.getOrCreateCluster(cacheName);
            }

            if (cnt % 10 == 0) {
                Ignite ignite = Ignition.ignite();

                StringBuilder builder = new StringBuilder();
                builder.append("------------------- TensorFlow Cluster Service Info -------------------").append('\n');
                builder.append("Cache : ").append(cacheName).append("\n");
                builder.append("Node : ").append(ignite.cluster().localNode().id().toString().substring(0, 8)).append("\n");

                builder.append("Specification : ").append('\n');

                TensorFlowCluster cluster = clusterMgr.getCache().get(cacheName);

                String clusterSpec = clusterMgr.getSrvProcMgr().formatClusterSpec(cluster.getSpec());
                builder.append(clusterSpec).append('\n');

                Map<UUID, List<LongRunningProcessStatus>> statuses = clusterMgr.getSrvProcMgr().ping(cluster.getProcesses());

                builder.append("State : ").append('\n');

                for (UUID nodeId : cluster.getProcesses().keySet()) {
                    List<UUID> pr = cluster.getProcesses().get(nodeId);
                    List<LongRunningProcessStatus> st = statuses.get(nodeId);

                    builder.append("Node ").append(nodeId.toString().substring(0, 8)).append(" -> ").append('\n');
                    for (int i = 0; i < pr.size(); i++) {
                        builder.append("\tProcess ")
                            .append(pr.get(i).toString().substring(0, 8))
                            .append(" with status ")
                            .append(st.get(i).getState());

                        if (st.get(i).getException() != null)
                            builder.append(" (").append(st.get(i).getException()).append(")");

                        builder.append('\n');
                    }
                }

                builder.append("-----------------------------------------------------------------------").append('\n');

                System.out.println(builder);
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
