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
import java.util.Set;

/**
 * This interface defines JMX view for IO statistics.
 */
@MXBeanDescription("MBean that provides access IO statistics metrics.")
public interface IoStatisticsMetricsMXBean {

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
     * Gets string presentation of cache group IO statistics for given cache group.
     *
     * @param cacheGrpName Name of cache group.
     * @return Formatted representation of cache group IO statistics.
     */
    @MXBeanDescription("String presentation of cache group IO statistics.")
    @MXBeanParametersNames("cacheGrpName")
    @MXBeanParametersDescriptions("Cache group name.")
    public String getCacheStatisticsFormatted(String cacheGrpName);

    /**
     * Gets number of physical page reads for given cache group.
     *
     * @param cacheGrpName Name of cache group.
     * @return Number of physical page reads for given cache group. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of physical page reads for given cache group." +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames("cacheGrpName")
    @MXBeanParametersDescriptions("Cache group name.")
    public Long getCachePhysicalReadsStatistics(String cacheGrpName);

    /**
     * Gets number of logical page reads for given cache group.
     *
     * @param cacheGrpName Name of cache group.
     * @return Number of logical page reads for given cache group. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of logical page reads for given cache group. " +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames("cacheGrpName")
    @MXBeanParametersDescriptions("Cache group name.")
    public Long getCacheLogicalReadsStatistics(String cacheGrpName);

    /**
     * Gets string presentation of index IO statistics for given cache group and index.
     *
     * @param cacheGrpName Name of cache group.
     * @param idxName Name of index.
     * @return Formatted representation of index IO statistics for given cache group and index.
     */
    @MXBeanDescription("String presentation of index IO statistics.")
    @MXBeanParametersNames({"cacheGrpName", "idxName"})
    @MXBeanParametersDescriptions({"Cache group name.", "Index name."})
    public String getIndexStatisticsFormatted(String cacheGrpName, String idxName);


    /**
     * Gets number of physical index page reads for given cache group and index.
     *
     * @param cacheGrpName Name of cache group.
     * @param idxName Name of index.
     * @return Number of physical page reads for given cache group and index. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of physical page reads for given cache group." +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheGrpName", "idxName"})
    @MXBeanParametersDescriptions({"Cache group name.", "Index name."})
    public Long getIndexPhysicalReadsStatistics(String cacheGrpName, String idxName);

    /**
     * Gets number of logical index page reads for given cache group and index.
     *
     * @param cacheGrpName Name of cache group.
     * @param idxName Name of index.
     * @return Number of logical page reads for given cache group and index. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of logical page reads for given cache group." +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheGrpName", "idxName"})
    @MXBeanParametersDescriptions({"Cache group name.", "Index name."})
    public Long getIndexLogicalReadsStatistics(String cacheGrpName, String idxName);


    /**
     * Gets number of logical leaf index's page reads for given cache group and index.
     *
     * @param cacheGrpName Name of cache group.
     * @param idxName Name of index.
     * @return Number of logical leaf index's page reads for given cache group and index. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of logical leaf index's page reads for given cache group and index." +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheGrpName", "idxName"})
    @MXBeanParametersDescriptions({"Cache group name.", "Index name."})
    public Long getIndexLeafLogicalReadsStatistics(String cacheGrpName, String idxName);

    /**
     * Gets number of physical leaf index's page reads for given cache group and index.
     *
     * @param cacheGrpName Name of cache group.
     * @param idxName Name of index.
     * @return Number of physical leaf index's page reads for given cache group and index. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of physical leaf index's page reads for given cache group and index." +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheGrpName", "idxName"})
    @MXBeanParametersDescriptions({"Cache group name.", "Index name."})
    public Long getIndexLeafPhysicalReadsStatistics(String cacheGrpName, String idxName);

    /**
     * Gets number of logical inner index's page reads for given cache group and index.
     *
     * @param cacheGrpName Name of cache group.
     * @param idxName Name of index.
     * @return Number of logical inner index's page reads for given cache group and index. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of logical inner index's page reads for given cache group and index." +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheGrpName", "idxName"})
    @MXBeanParametersDescriptions({"Cache group name.", "Index name."})
    public Long getIndexInnerLogicalReadsStatistics(String cacheGrpName, String idxName);

    /**
     * Gets number of physical inner index's page reads for given cache group and index.
     *
     * @param cacheGrpName Name of cache group.
     * @param idxName Name of index.
     * @return Number of physical inner index's page reads for given cache group and index. {@code null} in case such
     * statistics doesn't exists.
     */
    @MXBeanDescription("Number of physical inner index's page reads for given cache group and index." +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheGrpName", "idxName"})
    @MXBeanParametersDescriptions({"Cache group name.", "Index name."})
    public Long getIndexInnerPhysicalReadsStatistics(String cacheGrpName, String idxName);

    /**
     * @param cacheGrpName Name of cache group.
     * @return Names of hash indexes registered to gather IO statistics.
     */
    @MXBeanDescription("Name of hash indexes registered to gather IO statisitcs for given cache group.")
    @MXBeanParametersNames("cacheGrpName")
    @MXBeanParametersDescriptions("Cache group name.")
    public Set<String> getStatHashIndexesNames(String cacheGrpName);

    /**
     * @param cacheGrpName Name of cache group.
     * @return Names of sorted indexes registered to gather IO statistics.
     */
    @MXBeanDescription("Name of sorted indexes registered to gather IO statisitcs for given cache group.")
    @MXBeanParametersNames("cacheGrpName")
    @MXBeanParametersDescriptions("Cache group name.")
    public Set<String> getStatSortedIndexesNames(String cacheGrpName);

    /**
     * @return Names of caches registered to gather IO statistics.
     */
    @MXBeanDescription("Name of cache groups registered to gather IO statistics.")
    public Set<String> getStatCachesNames();

}
