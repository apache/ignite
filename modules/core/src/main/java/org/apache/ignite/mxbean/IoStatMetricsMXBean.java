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
 *
 */

package org.apache.ignite.mxbean;

import java.time.LocalDateTime;
import java.util.Map;
import org.apache.ignite.mxbean.MXBeanDescription;

/**
 * This interface defines JMX view for IO statistics.
 */
@MXBeanDescription("MBean that provides access to local IO statistics metrics.")
public interface IoStatMetricsMXBean {

    /**
     * @return Start time of gathering statistics.
     */
    @MXBeanDescription("Start gathering staistics.")
    public LocalDateTime getStartGatheringStatistics();

    /**
     * @return Number of physical reads splitted by type.
     */
    @MXBeanDescription("Physical reads IO statistics.")
    public Map<String, Long> getPhysicalReads();

    /**
     * @return Number of physical writes splitted by type.
     */
    @MXBeanDescription("Physical writes IO statistics.")
    public Map<String, Long> getPhysicalWrites();

    /**
     * @return Number of logical reads splitted by type.
     */
    @MXBeanDescription("Logical reads IO statistics.")
    public Map<String, Long> getLogicalReads();

    /**
     * @return Number of physical reads splitted by aggregated types.
     */
    @MXBeanDescription("Aggregated physical reads IO statistics.")
    public Map<String, Long> getAggregatedPhysicalReads();

    /**
     * @return Number of physical writes splitted by aggregated types.
     */
    @MXBeanDescription("Aggregated physical writes IO statistics.")
    public Map<String, Long> getAggregatedPhysicalWrites();

    /**
     * @return Number of logical reads splitted by aggregated types.
     */
    @MXBeanDescription("Aggregated logical reads IO statistics.")
    public Map<String, Long> getAggregatedLogicalReads();

    /**
     * Reset all IO statistics.
     */
    @MXBeanDescription("Reset gathered statistics.")
    public void resetStatistics();
}
