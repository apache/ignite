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

import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * Class that represents the network-related resources of a node and provides entry points for working with the
 * network members of a cluster.
 */
public interface ClusterService extends IgniteComponent {
    /**
     * @return {@link TopologyService} for working with the cluster topology.
     */
    TopologyService topologyService();

    /**
     * @return {@link TopologyService} for sending messages to the cluster members.
     */
    MessagingService messagingService();

    /**
     * @return Context associated with the current node.
     */
    ClusterLocalConfiguration localConfiguration();

    /** {@inheritDoc} */
    @Override default void stop() {
        // TODO: IGNITE-15161 Implement component's stop.
    }

    /**
     * Checks whether cluster service was stopped.
     *
     * @return {@code true} if cluster service is stopped, {@code false} otherwise.
     */
    public boolean isStopped();
}
