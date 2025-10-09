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

package org.apache.ignite.cluster;

/**
 * Provides network-related information for a node in the Ignite cluster.
 * <p>
 * This interface allows access to metadata about the network environment of a node,
 * such as its <b>Data Center ID</b>, which is crucial for topology-aware operations
 * in multi-data center deployments.
 *
 * <p>
 * The Data Center ID is used by Ignite components to optimize communication and data placement
 * based on the physical or logical location of nodes. For example, it can be used to:
 * <ul>
 *     <li>Prefer intra-data center communication for lower latency;</li>
 *     <li>Balance workloads across data centers;</li>
 *     <li>Implement failover strategies that respect data center boundaries.</li>
 * </ul>
 *
 * <p>
 * This interface is typically used internally by Ignite SPIs (Service Provider Interfaces)
 * to make decisions based on the network topology.
 */
public interface NetworkEnvironment {
    /**
     * Returns the Data Center ID of the node.
     * <p>
     * This ID is provided via the {@code org.apache.ignite.spi.discovery.datacenter.DataCenterResolver}
     * and must be consistent for all nodes in the same data center.
     *
     * @return Data Center ID as a string.
     */
    public String dataCenterId();
}
