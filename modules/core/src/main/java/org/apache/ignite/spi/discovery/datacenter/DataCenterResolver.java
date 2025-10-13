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

/**
 * <p>
 * The {@code DataCenterResolver} interface provides a mechanism for retrieving information about
 * the data center in which a node is running.
 * <p>
 * This interface is intended for use in distributed systems where the Ignite cluster spans multiple data centers.
 * It allows Ignite to make topology-aware decisions regarding node communication, data placement,
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
 * {@link org.apache.ignite.configuration.IgniteConfiguration#setDataCenterResolver(DataCenterResolver)}.
 */
public interface DataCenterResolver {
    /**
     * Returns the network environment information for the current node.
     * <p>
     * This method is called during node initialization and should return consistent values
     * for all nodes located in the same physical or logical location.
     *
     * @return A {@link String} representing id of a Data Center.
     */
    public String resolveDataCenterId();
}
