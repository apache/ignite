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

package org.apache.ignite.client;

import java.util.Collection;
import java.util.UUID;

/**
 * Thin client cluster group facade. Defines a cluster group which contains all or a subset of cluster nodes.
 */
public interface ClientClusterGroup {
    /**
     * Creates a cluster group over nodes with specified node IDs.
     *
     * @param ids Collection of node IDs.
     * @return Cluster group over nodes with the specified node IDs.
     */
    public ClientClusterGroup forNodeIds(Collection<UUID> ids);

    /**
     * Creates a cluster group for a node with the specified ID.
     *
     * @param id Node ID to get the cluster group for.
     * @param ids Optional additional node IDs to include into the cluster group.
     * @return Cluster group over the node with the specified node IDs.
     */
    public ClientClusterGroup forNodeId(UUID id, UUID... ids);
}