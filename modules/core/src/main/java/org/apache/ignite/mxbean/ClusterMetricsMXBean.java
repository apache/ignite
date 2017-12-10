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

package org.apache.ignite.mxbean;

import java.util.Map;

/**
 * Cluster metrics MBean.
 */
@MXBeanDescription("MBean that provides access to aggregated cluster metrics.")
public interface ClusterMetricsMXBean extends ClusterLocalNodeMetricsMXBean {
    /**
     * Get count of server nodes.
     *
     * @return Count of server nodes.
     */
    @MXBeanDescription("Server nodes count.")
    public int getTotalServerNodes();

    /**
     * Get count of client nodes.
     *
     * @return Count of client nodes.
     */
    @MXBeanDescription("Client nodes count.")
    public int getTotalClientNodes();

    /**
     * Get current topology version.
     *
     * @return Current topology version.
     */
    @MXBeanDescription("Current topology version.")
    public long getTopologyVersion();

    /**
     * Get the number of nodes that have specified attribute.
     *
     * @param attrName Attribute name.
     * @param attrVal Attribute value.
     * @param srv Include server nodes.
     * @param client Include client nodes.
     * @return The number of nodes that have specified attribute.
     */
    @MXBeanDescription("Get the number of nodes that have specified attribute.")
    @MXBeanParametersNames(
        {"attrName", "attrValue", "server", "client"}
    )
    @MXBeanParametersDescriptions(
        {"Attribute name.", "Attribute value.", "Include server nodes.", "Include client nodes."}
    )
    public int countNodes(String attrName, String attrVal, boolean srv, boolean client);

    /**
     * Get the number of nodes grouped by the node attribute value.
     *
     * @param attrName Attribute name.
     * @param srv Include server nodes.
     * @param client Include client nodes.
     * @return The number of nodes grouped by the node attribute value.
     */
    @MXBeanDescription("Get the number of nodes grouped by the node attribute.")
    @MXBeanParametersNames(
        {"attrName", "server", "client"}
    )
    @MXBeanParametersDescriptions(
        {"Attribute name.", "Include server nodes.", "Include client nodes."}
    )
    public Map<Object, Integer> groupNodes(String attrName, boolean srv, boolean client);
}
