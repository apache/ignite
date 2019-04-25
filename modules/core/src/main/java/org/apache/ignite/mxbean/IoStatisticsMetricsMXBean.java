/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.mxbean;

/**
 * This interface defines JMX view for IO statistics.
 */
@MXBeanDescription("MBean that provides access IO statistics metrics.")
public interface IoStatisticsMetricsMXBean {

    /**
     * @return Start time of gathering statistics as UTC milliseconds.
     */
    @MXBeanDescription("Start time of gathering staistics.")
    long getStartTime();

    /**
     * @return Start time of gathering statistics in ISO-8601 format.
     */
    @MXBeanDescription("Start time of gathering staistics.")
    String getStartTimeLocal();

    /**
     * Reset all IO statistics.
     */
    @MXBeanDescription("Reset gathered statistics.")
    public void reset();

    /**
     * Gets string presentation of cache group IO statistics for given cache group.
     *
     * @param cacheGrpName Name of cache group.
     * @return Formatted representation of cache group IO statistics.
     */
    @MXBeanDescription("String presentation of cache group IO statistics.")
    @MXBeanParametersNames("cacheGrpName")
    @MXBeanParametersDescriptions("Cache group name.")
    public String getCacheGroupStatistics(String cacheGrpName);

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
    public Long getCacheGroupPhysicalReads(String cacheGrpName);

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
    public Long getCacheGroupLogicalReads(String cacheGrpName);

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
    public String getIndexStatistics(String cacheGrpName, String idxName);

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
    public Long getIndexPhysicalReads(String cacheGrpName, String idxName);

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
    public Long getIndexLogicalReads(String cacheGrpName, String idxName);


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
    public Long getIndexLeafLogicalReads(String cacheGrpName, String idxName);

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
    public Long getIndexLeafPhysicalReads(String cacheGrpName, String idxName);

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
    public Long getIndexInnerLogicalReads(String cacheGrpName, String idxName);

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
    public Long getIndexInnerPhysicalReads(String cacheGrpName, String idxName);

}
