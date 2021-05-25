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

package org.apache.ignite.network;

/**
 * Class that represents the network-related resources of a node and provides entry points for working with the
 * network members of a cluster.
 */
public interface ClusterService {
    /**
     * Returns the {@link TopologyService} for working with the cluster topology.
     */
    TopologyService topologyService();

    /**
     * Returns the {@link TopologyService} for sending messages to the cluster members.
     */
    MessagingService messagingService();

    /**
     * Returns the context associated with the current node.
     */
    ClusterLocalConfiguration localConfiguration();

    /**
     * Starts the current node, allowing it to join the cluster and start receiving messages.
     */
    void start();

    /**
     * Stops the current node, gracefully freeing the encapsulated resources.
     */
    void shutdown();
}
