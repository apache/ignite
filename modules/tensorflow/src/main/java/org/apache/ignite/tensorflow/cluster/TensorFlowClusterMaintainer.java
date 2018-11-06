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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
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

    /** Ignite instance. */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** Logger. */
    @LoggerResource
    private transient IgniteLogger log;

    /** TensorFlow cluster identifier. */
    private final UUID clusterId;

    /** Job archive. */
    private final TensorFlowJobArchive jobArchive;

    /** Topic name. */
    private final String topicName;

    /** TensorFlow cluster manager. */
    private transient TensorFlowClusterManager clusterMgr;

    /** Previous partition mapping. */
    private transient UUID[] prev;

    /**
     * Constructs a new instance of TensorFlow cluster service.
     *
     * @param clusterId Cluster identifier.
     * @param jobArchive Job archive.
     * @param topicName Topic name.
     */
    public TensorFlowClusterMaintainer(UUID clusterId, TensorFlowJobArchive jobArchive, String topicName) {
        assert clusterId != null : "Cluster identifier should not be null";
        assert jobArchive != null : "Job archive should not be null";
        assert topicName != null : "Topic name should not be null";

        this.clusterId = clusterId;
        this.jobArchive = jobArchive;
        this.topicName = topicName;
    }

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        clusterMgr.stopClusterIfExists(clusterId);
        log.debug("Cluster maintainer canceled [clusterId=" + clusterId + "]");
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) {
        clusterMgr = new TensorFlowClusterManager(ignite);
        log.debug("Cluster maintainer initialized [clusterId=" + clusterId + "]");
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) {
        while (!ctx.isCancelled()) {
            LockSupport.parkNanos(1_000_000);

            boolean completed = clusterMgr.isUserScriptCompleted(clusterId);
            if (completed)
                break;

            boolean restartRequired = hasAffinityChanged();

            if (restartRequired)
                log.debug("Affinity mapping changed, cluster will be restarted [clusterId=" + clusterId + "]");

            if (!restartRequired) {
                try {
                    TensorFlowCluster cluster = clusterMgr.getCluster(clusterId);
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
                catch (Exception e) {
                    log.error("Failed to check process statuses", e);
                    restartRequired = true;
                }

                if (restartRequired)
                    log.debug("Fail detected, cluster will be restarted [clusterId=" + clusterId + "]");
            }

            if (restartRequired) {
                clusterMgr.stopClusterIfExists(clusterId);

                TensorFlowCluster cluster = clusterMgr.createCluster(
                    clusterId,
                    jobArchive,
                    str -> ignite.message().sendOrdered("us_out_" + clusterId, str, 60 * 1000),
                    str -> ignite.message().sendOrdered("us_err_" + clusterId, str, 60 * 1000)
                );

                ignite.message().send(topicName, Optional.of(cluster));
            }
        }

        clusterMgr.stopClusterIfExists(clusterId);

        ignite.message().send(topicName, Optional.empty());

        log.debug("Cluster maintainer completed [clusterId=" + clusterId + "]");
    }

    /**
     * Checks if affinity mapping has been changed.
     *
     * @return True if mapping has been changed, otherwise false.
     */
    private boolean hasAffinityChanged() {
        Affinity<?> affinity = ignite.affinity(jobArchive.getUpstreamCacheName());

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
