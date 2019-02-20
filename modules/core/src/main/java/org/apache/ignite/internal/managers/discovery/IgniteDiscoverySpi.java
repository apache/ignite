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

package org.apache.ignite.internal.managers.discovery;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.spi.discovery.DiscoverySpi;

/**
 *
 */
public interface IgniteDiscoverySpi extends DiscoverySpi {
    /**
     * @param nodeId Node ID.
     * @return {@code True} if node joining or already joined topology.
     */
    public boolean knownNode(UUID nodeId);

    /**
     *
     * @return {@code True} if SPI supports client reconnect.
     */
    public boolean clientReconnectSupported();

    /**
     *
     */
    public void clientReconnect();

    /**
     * @param feature Feature to check.
     * @return {@code true} if all nodes support the given feature.
     */
    public boolean allNodesSupport(IgniteFeatures feature);

    /**
     * For TESTING only.
     */
    public void simulateNodeFailure();

    /**
     * For TESTING only.
     *
     * @param lsnr Listener.
     */
    public void setInternalListener(IgniteDiscoverySpiInternalListener lsnr);

    /**
     * @return {@code True} if supports communication error resolve.
     */
    public boolean supportsCommunicationFailureResolve();

    /**
     * @param node Problem node.
     * @param err Connection error.
     */
    public void resolveCommunicationFailure(ClusterNode node, Exception err);
}
