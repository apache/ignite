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

/**
 * Management bean for monitoring cluster state.
 */
@MXBeanDescription("MBean for monitoring cluster state.")
public interface IgniteClusterMXBean {
    /**
     * Get count of server nodes.
     *
     * @return Count of server nodes.
     */
    @MXBeanDescription("Server nodes count.")
    public int getTotalServerNodes();

    /**
     * Get current topology version.
     *
     * @return Current topology version.
     */
    @MXBeanDescription("Current topology version.")
    public long getTopologyVersion();
}

