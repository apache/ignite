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
 * An MX bean allowing to monitor and tune SQL queries.
 *
 * @deprecated Temporary monitoring solution.
 */
@Deprecated
public interface QueryMXBean {
    /**
     * @return Timeout in milliseconds after which long query warning will be printed.
     */
    @MXBeanDescription("Timeout in milliseconds after which long query warning will be printed.")
    long getLongQueryWarningTimeout();

    /**
     * Sets timeout in milliseconds after which long query warning will be printed.
     *
     * @param longQueryWarningTimeout Timeout in milliseconds after which long query warning will be printed.
     */
    @MXBeanDescription("Sets timeout in milliseconds after which long query warning will be printed.")
    @MXBeanParametersNames("longQueryWarningTimeout")
    @MXBeanParametersDescriptions("Timeout in milliseconds after which long query warning will be printed.")
    void setLongQueryWarningTimeout(long longQueryWarningTimeout);

    /**
     * @return Threshold result's row count, when count of fetched rows is bigger than the threshold
     *      warning will be printed.
     */
    @MXBeanDescription("Threshold result's row count, when count of fetched rows is bigger than the threshold " +
        "warning will be printed.")
    long getResultSetSizeThreshold();

    /**
     * Sets threshold result's row count, when count of fetched rows is bigger than the threshold
     *      warning will be printed.
     *
     * @param resultSetSizeThreshold Threshold result's row count, when count of fetched rows is bigger
     *      than the threshold warning will be printed.
     */
    @MXBeanDescription("Sets threshold result's row count, when count of fetched rows is bigger than the threshold " +
        "warning will be printed.")
    @MXBeanParametersNames("resultSetSizeThreshold")
    @MXBeanParametersDescriptions("Threshold result's row count, when count of fetched rows is bigger than the " +
        "threshold warning will be printed.")
    void setResultSetSizeThreshold(long resultSetSizeThreshold);
}
