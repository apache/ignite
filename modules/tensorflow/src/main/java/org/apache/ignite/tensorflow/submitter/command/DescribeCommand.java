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

package org.apache.ignite.tensorflow.submitter.command;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.tensorflow.cluster.TensorFlowCluster;
import org.apache.ignite.tensorflow.cluster.TensorFlowClusterManager;
import org.apache.ignite.tensorflow.cluster.tfrunning.TensorFlowServerManager;

/**
 * Describe command that prints configuration of the specified TensorFlow cluster.
 */
public class DescribeCommand implements Command {
    /** Cluster identifier. */
    private final UUID clusterId;

    /**
     * Constructs a new instance of command "describe".
     *
     * @param clusterId Cluster identifier.
     */
    public DescribeCommand(UUID clusterId) {
        this.clusterId = clusterId;
    }

    /** {@inheritDoc} */
    @Override public void runWithinIgnite(Ignite ignite) {
        TensorFlowClusterManager mgr = new TensorFlowClusterManager(ignite);

        TensorFlowCluster cluster = mgr.getCluster(clusterId);

        if (cluster != null) {
            TensorFlowServerManager s = new TensorFlowServerManager(ignite);

            System.out.println("========== Cluster ID =====================");
            System.out.println(clusterId);
            System.out.println("========== Cluster Specification ==========");
            System.out.println(cluster.getSpec().format(ignite));
            System.out.println("========== Cluster Layout =================");
            for (UUID nodeId : cluster.getProcesses().keySet()) {
                List<UUID> processes = cluster.getProcesses().get(nodeId);
                System.out.println(nodeId + " (" + processes.size() + ")");
            }
        }
        else
            System.out.println("Cluster with ID " + clusterId + " not found");
    }
}
