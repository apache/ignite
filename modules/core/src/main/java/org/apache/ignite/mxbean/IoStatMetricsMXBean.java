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
import java.util.Set;
import org.apache.ignite.internal.stat.StatType;

/**
 * This interface defines JMX view for IO statistics.
 */
@MXBeanDescription("MBean that provides access to local IO statistics metrics.")
public interface IoStatMetricsMXBean {

    /**
     * @return Start time of gathering statistics.
     */
    @MXBeanDescription("Start time of gathering staistics.")
    public LocalDateTime getStartGatheringStatistics();

    /**
     * Reset all IO statistics.
     */
    @MXBeanDescription("Reset gathered statistics.")
    public void resetStatistics();

    /**
     * Universal method to get statistics for IO logical reads
     *
     * @param statTypeName Type of statistic {@link StatType}
     * @param subTypeFilter Subtype of statistics. In case value can be parsed as int will be used as Integer.
     * @param aggregate True in case statistics should be aggregated.
     * @return Logical reads statistics for given params.
     */
    @MXBeanDescription("Logical read statistics.")
    @MXBeanParametersNames(
        {
            "statTypeName",
            "subTypeFilter",
            "aggregate"
        }
    )
    @MXBeanParametersDescriptions(
        {
            "Type of statistic.",
            "Subtype of statistics. In case of null value will be returned GLOBAL statistics.",
            "True in case statistics should be aggregated."
        }
    )
    public Map<String, Long> getLogicalReadStatistics(String statTypeName, String subTypeFilter, boolean aggregate);

    /**
     * Universal method to get statistics for IO physical reads
     *
     * @param statTypeName Type of statistic {@link StatType}
     * @param subTypeFilter Subtype of statistics. In case value can be parsed as int will be used as Integer.
     * @param aggregate True in case statistics should be aggregated.
     * @return Physical reads statistics for given params.
     */
    @MXBeanDescription("Physical read statistics.")
    @MXBeanParametersNames(
        {
            "statTypeName",
            "subTypeFilter",
            "aggregate"
        }
    )
    @MXBeanParametersDescriptions(
        {
            "Type of statistic.",
            "Subtype of statistics. In case of null value will be returned GLOBAL statistics.",
            "True in case statistics should be aggregated."
        }
    )
    public Map<String, Long> getPhysicalReadStatistics(String statTypeName, String subTypeFilter, boolean aggregate);

    /**
     * @return Number of physical reads splitted by type.
     */
    @MXBeanDescription("Physical reads GLOBAL IO statistics.")
    public Map<String, Long> getPhysicalReadsGlobal();

    /**
     * @return Number of physical writes splitted by type.
     */
    @MXBeanDescription("Physical writes GLOBAL IO statistics.")
    public Map<String, Long> getPhysicalWritesGlobal();

    /**
     * @return Number of logical reads splitted by type.
     */
    @MXBeanDescription("Logical reads GLOBAL IO statistics.")
    public Map<String, Long> getLogicalReadsGlobal();

    /**
     * @return Number of physical reads splitted by aggregated types.
     */
    @MXBeanDescription("Aggregated physical reads GLOBAL IO statistics.")
    public Map<String, Long> getAggregatedPhysicalReadsGlobal();

    /**
     * @return Number of physical writes splitted by aggregated types.
     */
    @MXBeanDescription("Aggregated physical writes GLOBAL IO statistics.")
    public Map<String, Long> getAggregatedPhysicalWritesGlobal();

    /**
     * @return Number of logical reads splitted by aggregated types.
     */
    @MXBeanDescription("Aggregated logical reads GLOBAL IO statistics.")
    public Map<String, Long> getAggregatedLogicalReadsGlobal();

    /**
     * @return Names of indexes which have logical reads IO statistics.
     */
    @MXBeanDescription("Name of indexes which have logical reads IO statistics.")
    public Set<String> getLogicalReadsStatIndexesNames();

    /**
     * @return Names of indexes which have physical reads IO statistics.
     */
    @MXBeanDescription("Name of indexes which have physical reads IO statistics.")
    public Set<String> getPhysicalReadsStatIndexesNames();

    /**
     * @param idxName Name of index.
     * @return Number of physical reads for given index splitted by type.
     */
    @MXBeanDescription("Physical reads statistics for concrete index.")
    @MXBeanParametersNames(
        "idxName"
    )
    @MXBeanParametersDescriptions(
        "Name of index."
    )
    public Map<String, Long> getPhysicalReadsIndex(String idxName);

    /**
     * @param idxName Name of index.
     * @return Number of logical reads for given index splitted by type.
     */
    @MXBeanDescription("Logical reads statistics for concrete index.")
    @MXBeanParametersNames(
        "idxName"
    )
    @MXBeanParametersDescriptions(
        "Name of index."
    )
    public Map<String, Long> getLogicalReadsIndex(String idxName);

}
