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

package org.apache.ignite.spi.discovery.datacenter;

import org.apache.ignite.cluster.NetworkEnvironment;

/**
 * <p>
 * The {@code NetworkEnvironmentResolver} interface provides a mechanism for retrieving detailed
 * network-related information about the environment in which a node is running. This includes, but is not limited to,
 * the <b>Data Center ID</b>, and potentially other metadata such as rack ID or availability zone.
 *
 * <p>
 * This interface is intended for use in distributed systems where the Ignite cluster spans multiple data centers.
 * It allows Ignite to make intelligent, topology-aware decisions regarding node communication, data placement,
 * failover strategies, and resource allocation. Implementations can be tailored to extract this information from
 * various sources, including:
 * <ul>
 *     <li>Environment variables;</li>
 *     <li>System properties;</li>
 *     <li>Configuration files;</li>
 *     <li>Node's network interfaces.</li>
 * </ul>
 *
 * <p>
 * The resolver is used during node initialization to determine its network context, enabling Ignite to optimize
 * operations such as:
 * <ul>
 *     <li>Minimizing cross-data center communication to reduce latency;</li>
 *     <li>Improving fault tolerance by avoiding placing redundant copies across distant locations;</li>
 *     <li>Supporting affinity-based task execution within the same data center;</li>
 *     <li>Providing input to discovery SPIs for topology-aware node grouping.</li>
 * </ul>
 *
 * <p>
 * This resolver should be configured via Ignite configuration using:
 * {@link org.apache.ignite.configuration.IgniteConfiguration#setNetworkEnvironmentResolver(NetworkEnvironmentResolver)}.
 */
public interface NetworkEnvironmentResolver {
    /**
     * Returns the network environment information for the current node.
     * <p>
     * This method is called during node initialization and should return consistent values
     * for all nodes located in the same physical or logical location.
     *
     * @return A {@link NetworkEnvironment} object containing at least the Data Center ID.
     */
    public NetworkEnvironment resolveNetworkEnvironment();
}
