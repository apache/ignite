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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.JoiningNodeDiscoveryData;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for all main internal Ignite components (managers and processors).
 */
public interface GridComponent {
    /**
     * Unique component type for discovery data exchange.
     */
    enum DiscoveryDataExchangeType {
        /** */
        CONTINUOUS_PROC,

        /** */
        CACHE_PROC,

        /** State process. */
        STATE_PROC,

        /** */
        PLUGIN,

        /** */
        CLUSTER_PROC,

        /** */
        DISCOVERY_PROC,

        /** */
        MARSHALLER_PROC,

        /** */
        BINARY_PROC,

        /** Query processor. */
        QUERY_PROC,

        /** Authentication processor. */
        AUTH_PROC,

        /** */
        CACHE_CRD_PROC,

        /** Encryption manager. */
        ENCRYPTION_MGR,

        /** Service processor. */
        SERVICE_PROC
    }

    /**
     * Starts grid component.
     *
     * @throws IgniteCheckedException Throws in case of any errors.
     */
    public void start() throws IgniteCheckedException;

    /**
     * Stops grid component.
     *
     * @param cancel If {@code true}, then all ongoing tasks or jobs for relevant
     *      components need to be cancelled.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void stop(boolean cancel) throws IgniteCheckedException;

    /**
     * Callback that notifies that kernal has successfully started,
     * including all managers and processors.
     *
     * @param active Cluster active flag (note: should be used carefully since state can
     *     change concurrently).
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void onKernalStart(boolean active) throws IgniteCheckedException;

    /**
     * Callback to notify that kernal is about to stop.
     *
     * @param cancel Flag indicating whether jobs should be canceled.
     */
    public void onKernalStop(boolean cancel);

    /**
     * Collects discovery data on joining node before sending
     * {@link TcpDiscoveryJoinRequestMessage} request.
     *
     * @param dataBag container object to store discovery data in.
     */
    public void collectJoiningNodeData(DiscoveryDataBag dataBag);

    /**
     * Collects discovery data on nodes already in grid on receiving
     * {@link TcpDiscoveryNodeAddedMessage}.
     *
     * @param dataBag container object to store discovery data in.
     */
    public void collectGridNodeData(DiscoveryDataBag dataBag);

    /**
     * Receives discovery data object from remote nodes (called
     * on new node during discovery process).
     *
     * @param data {@link GridDiscoveryData} interface to retrieve discovery data collected on remote nodes
     *                                      (data common for all nodes in grid and specific for each node).
     */
    public void onGridDataReceived(GridDiscoveryData data);

    /**
     * Method is called on nodes that are already in grid (not on joining node).
     * It receives discovery data from joining node.
     *
     * @param data {@link JoiningNodeDiscoveryData} interface to retrieve discovery data of joining node.
     */
    public void onJoiningNodeDataReceived(JoiningNodeDiscoveryData data);

    /**
     * Prints memory statistics (sizes of internal structures, etc.).
     *
     * NOTE: this method is for testing and profiling purposes only.
     */
    public void printMemoryStats();

    /**
     * Validates that new node can join grid topology, this method is called on coordinator
     * node before new node joins topology.
     *
     * @param node Joining node.
     * @return Validation result or {@code null} in case of success.
     */
    @Nullable public IgniteNodeValidationResult validateNode(ClusterNode node);

    /** */
    @Nullable public IgniteNodeValidationResult validateNode(ClusterNode node, JoiningNodeDiscoveryData discoData);

    /**
     * Gets unique component type to distinguish components providing discovery data. Must return non-null value
     * if component implements any of methods {@link #collectJoiningNodeData(DiscoveryDataBag)}
     * or {@link #collectGridNodeData(DiscoveryDataBag)}.
     *
     * @return Unique component type for discovery data exchange.
     */
    @Nullable public DiscoveryDataExchangeType discoveryDataType();

    /**
     * Client disconnected callback.
     *
     * @param reconnectFut Reconnect future.
     * @throws IgniteCheckedException If failed.
     */
    public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException;

    /**
     * Client reconnected callback.
     *
     * @param clusterRestarted Cluster restarted flag.
     * @throws IgniteCheckedException If failed.
     * @return Future to wait before completing reconnect future.
     */
    @Nullable public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException;
}
