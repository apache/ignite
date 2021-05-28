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

import java.util.Collection;
import org.jetbrains.annotations.Nullable;

/**
 * Entry point for obtaining information about a cluster's topology.
 *
 * TODO: allow removing event handlers, see https://issues.apache.org/jira/browse/IGNITE-14519
 */
public interface TopologyService {
    /**
     * @return Information about the local network member.
     */
    ClusterNode localMember();

    /**
     * @return Information about all members which have discovered by the local member (including the local member
     * itself).
     */
    Collection<ClusterNode> allMembers();

    /**
     * Registers a handler for topology change events.
     *
     * @param handler Topology events handler.
     */
    void addEventHandler(TopologyEventHandler handler);

    /**
     * Returns a cluster node by it's network address in host:port format.
     *
     * @param addr The address.
     * @return The node or {@code null} if the node is not yet discovered or dead.
     */
    @Nullable ClusterNode getByAddress(String addr);
}
