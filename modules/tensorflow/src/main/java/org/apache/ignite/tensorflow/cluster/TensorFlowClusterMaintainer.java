/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
        while (!ctx.isCancelled() && !hasUserScriptCompletedSuccessfully()) {
            LockSupport.parkNanos(1_000_000);

            boolean restartRequired = hasAffinityChanged()
                || hasAnyWorkerFailed()
                || hasChiefFailed()
                || hasUserScriptFailed();

            if (restartRequired) {
                log.debug("Cluster will be restarted [clusterId=" + clusterId + "]");

                restartCluster();
            }
        }

        stopCluster(true);

        log.debug("Cluster maintainer completed [clusterId=" + clusterId + "]");
    }

    /**
     * Restarts TensorFlow cluster.
     */
    private void restartCluster() {
        stopCluster(false);
        startCluster();
    }

    /**
     * Stops TensorFlow cluster.
     *
     * @param terminate Terminate TensorFlow cluster and notify all listeners that cluster won't be started again.
     */
    private void stopCluster(boolean terminate) {
        clusterMgr.stopClusterIfExists(clusterId);

        if (terminate)
            ignite.message().send(topicName, Optional.empty());
    }

    /**
     * Starts TensorFlow cluster.
     */
    private void startCluster() {
        TensorFlowCluster cluster = clusterMgr.createCluster(
            clusterId,
            jobArchive,
            str -> ignite.message().sendOrdered("us_out_" + clusterId, str, 60 * 1000),
            str -> ignite.message().sendOrdered("us_err_" + clusterId, str, 60 * 1000)
        );

        ignite.message().send(topicName, Optional.of(cluster));
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

    /**
     * Checks is any worker has failed.
     *
     * @return True if any worker has failed, otherwise false.
     */
    private boolean hasAnyWorkerFailed() {
        TensorFlowCluster cluster = clusterMgr.getCluster(clusterId);

        Map<UUID, List<LongRunningProcessStatus>> statuses;
        try {
            statuses = clusterMgr.getSrvProcMgr().ping(cluster.getProcesses());
        }
        catch (Exception e) {
            log.error("Failed to check process statuses", e);

            return true;
        }

        for (UUID nodeId : statuses.keySet()) {
            for (LongRunningProcessStatus status : statuses.get(nodeId)) {
                if (status.getState().equals(LongRunningProcessState.DONE))
                    return true;
            }
        }

        return false;
    }

    /**
     * Checks if chief has failed.
     *
     * @return True if chief has failed, otherwise false.
     */
    private boolean hasChiefFailed() {
        return clusterMgr.getChiefException(clusterId) != null;
    }

    /**
     * Checks if user script failed.
     *
     * @return True if user script has failed, otherwise false.
     */
    private boolean hasUserScriptFailed() {
        return clusterMgr.getUserScriptException(clusterId) != null;
    }

    /**
     * Checks if user script has completed successfully.
     *
     * @return True if user script has completed successfully, otherwise false.
     */
    private boolean hasUserScriptCompletedSuccessfully() {
        return clusterMgr.isUserScriptCompleted(clusterId)
            && clusterMgr.getUserScriptException(clusterId) == null;
    }
}
