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

import org.apache.ignite.lang.IgniteExperimental;

/**
 *  Data center resolver enables to obtain a <b>Data Center ID</b> at node's startup.
 *  This interface is used in distributed environments where the cluster spans multiple data centers.
 *  It is <b>not required</b> for single data center deployments.
 *
 *  <p>
 *  The <b>Data Center ID</b> is a human-readable string that uniquely identifies the data center where a node is running.
 *  All nodes within the same physical or logical data center must return the same ID.
 *  This information is used by Ignite's internal components to optimize operations such as:
 *  <ul>
 *      <li>Node placement and affinity.</li>
 *      <li>Network communication optimization (e.g., preferring intra-data center communication).</li>
 *      <li>Failover and rebalancing strategies.</li>
 *      <li>Topology awareness in discovery SPIs.</li>
 *  </ul>
 *
 *  <p>
 *  Implementations of this interface can resolve the Data Center ID based on:
 *  <ul>
 *      <li>Environment variables.</li>
 *      <li>System properties.</li>
 *      <li>Configuration files.</li>
 *      <li>Node's network interfaces.</li>
 *      <li>Custom logic specific to your deployment.</li>
 *  </ul>
 *
 *  <p>
 *  Example usage:
 *  <pre>
 *  public class MyDataCenterResolver implements DataCenterResolver {
 *      {@code @Override}
 *      public String resolveDataCenterId() {
 *          return System.getenv("DATA_CENTER_ID");
 *      }
 *  }
 *  </pre>
 *
 *  <p>
 *  This resolver should be configured in the Ignite configuration file or programmatically via
 *  {@link org.apache.ignite.configuration.IgniteConfiguration#setDataCenterResolver(DataCenterResolver)}.
 */
@IgniteExperimental
public interface DataCenterResolver {
    /**
     * Returns the Data Center ID for the current node.
     * <p>
     * This method is called during node initialization and should return a consistent value
     * for all nodes located in the same data center.
     *
     * @return Data Center ID as a non-null, non-empty string.
     */
    public String resolveDataCenterId();
}
