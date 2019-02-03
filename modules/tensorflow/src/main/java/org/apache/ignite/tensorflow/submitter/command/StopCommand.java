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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.tensorflow.cluster.TensorFlowClusterGatewayManager;
import picocli.CommandLine;

/**
 * Command "stop" that is used to stop TensorFlow cluster.
 */
@CommandLine.Command(
    name = "stop",
    description = "Stops a running TensorFlow cluster.",
    mixinStandardHelpOptions = true
)
public class StopCommand extends AbstractCommand {
    /** Cluster identifier. */
    @CommandLine.Parameters(paramLabel = "CLUSTER_ID", description = "Cluster identifier.")
    private UUID clusterId;

    /** {@inheritDoc} */
    @Override public void run() {
        try (Ignite ignite = getIgnite()) {
            TensorFlowClusterGatewayManager mgr = new TensorFlowClusterGatewayManager(ignite);
            mgr.stopClusterIfExists(clusterId);
        }
    }

    /** */
    public void setClusterId(UUID clusterId) {
        this.clusterId = clusterId;
    }
}
