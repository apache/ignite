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
     * Gets string presentation of cache IO statistics for given cache.
     *
     * @param cacheName Name of cache.
     * @return Formatted representation of cache IO statistics.
     */
    @MXBeanDescription("String presentation of cache IO statistics.")
    @MXBeanParametersNames("cacheName")
    @MXBeanParametersDescriptions("Cache name.")
    public String getCacheStatisticsFormatted(String cacheName);

    /**
     * Gets number of physical page reads for given cache.
     *
     * @param cacheName Name of cache.
     * @return Number of physical page reads for given cache. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of physical page reads for given cache. " +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames("cacheName")
    @MXBeanParametersDescriptions("Cache name.")
    public Long getCachePhysicalReadsStatistics(String cacheName);

    /**
     * Gets number of logical page reads for given cache.
     *
     * @param cacheName Name of cache.
     * @return Number of logical page reads for given cache. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of logical page reads for given cache. " +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames("cacheName")
    @MXBeanParametersDescriptions("Cache name.")
    public Long getCacheLogicalReadsStatistics(String cacheName);

    /**
     * Gets string presentation of index IO statistics for given cache and index.
     *
     * @param cacheName Name of cache.
     * @param idxName Name of index.
     * @return Formatted representation of index IO statistics for given cache and index.
     */
    @MXBeanDescription("String presentation of index IO statistics.")
    @MXBeanParametersNames({"cacheName", "idxName"})
    @MXBeanParametersDescriptions({"Cache name.", "Index name."})
    public String getIndexStatisticsFormatted(String cacheName, String idxName);


    /**
     * Gets number of physical index page reads for given cache and index.
     *
     * @param cacheName Name of cache.
     * @param idxName Name of index.
     * @return Number of physical page reads for given cache and index. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of physical page reads for given cache. " +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheName", "idxName"})
    @MXBeanParametersDescriptions({"Cache name.", "Index name."})
    public Long getIndexPhysicalReadsStatistics(String cacheName, String idxName);

    /**
     * Gets number of logical index page reads for given cache and index.
     *
     * @param cacheName Name of cache.
     * @param idxName Name of index.
     * @return Number of logical page reads for given cache and index. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of logical page reads for given cache. " +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheName", "idxName"})
    @MXBeanParametersDescriptions({"Cache name.", "Index name."})
    public Long getIndexLogicalReadsStatistics(String cacheName, String idxName);


    /**
     * Gets number of logical leaf index's page reads for given cache and index.
     *
     * @param cacheName Name of cache.
     * @param idxName Name of index.
     * @return Number of logical leaf index's page reads for given cache and index. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of logical leaf index's page reads for given cache and index." +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheName", "idxName"})
    @MXBeanParametersDescriptions({"Cache name.", "Index name."})
    public Long getIndexLeafLogicalReadsStatistics(String cacheName, String idxName);

    /**
     * Gets number of physical leaf index's page reads for given cache and index.
     *
     * @param cacheName Name of cache.
     * @param idxName Name of index.
     * @return Number of physical leaf index's page reads for given cache and index. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of physical leaf index's page reads for given cache and index." +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheName", "idxName"})
    @MXBeanParametersDescriptions({"Cache name.", "Index name."})
    public Long getIndexLeafPhysicalReadsStatistics(String cacheName, String idxName);

    /**
     * Gets number of logical inner index's page reads for given cache and index.
     *
     * @param cacheName Name of cache.
     * @param idxName Name of index.
     * @return Number of logical inner index's page reads for given cache and index. {@code null} in case such statistics doesn't exists.
     */
    @MXBeanDescription("Number of logical inner index's page reads for given cache and index." +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheName", "idxName"})
    @MXBeanParametersDescriptions({"Cache name.", "Index name."})
    public Long getIndexInnerLogicalReadsStatistics(String cacheName, String idxName);

    /**
     * Gets number of physical inner index's page reads for given cache and index.
     *
     * @param cacheName Name of cache.
     * @param idxName Name of index.
     * @return Number of physical inner index's page reads for given cache and index. {@code null} in case such
     * statistics doesn't exists.
     */
    @MXBeanDescription("Number of physical inner index's page reads for given cache and index." +
        "Can return null in case such statistics doesn't exists.")
    @MXBeanParametersNames({"cacheName", "idxName"})
    @MXBeanParametersDescriptions({"Cache name.", "Index name."})
    public Long getIndexInnerPhysicalReadsStatistics(String cacheName, String idxName);

    /**
     * @param cacheName Name of cache.
     * @return Names of indexes registered to gather IO statistics.
     */
    @MXBeanDescription("Name of indexes registered to gather IO statisitcs for given cache.")
    @MXBeanParametersNames("cacheName")
    @MXBeanParametersDescriptions("Cache name.")
    public Set<String> getStatIndexesNames(String cacheName);

    /**
     * @return Names of caches registered to gather IO statistics.
     */
    @MXBeanDescription("Name of caches registered to gather IO statistics.")
    public Set<String> getStatCachesNames();

}
