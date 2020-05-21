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

package org.apache.ignite.internal.mxbean;

import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.mxbean.MXBeanParameter;
import org.apache.ignite.mxbean.MXBeanParametersDescriptions;
import org.apache.ignite.mxbean.MXBeanParametersNames;

/**
 * An MX bean allowing to monitor and tune SQL queries.
 *
 * @deprecated Temporary monitoring solution.
 */
@Deprecated
public interface SqlQueryMXBean {
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
    void setLongQueryWarningTimeout(
        @MXBeanParameter(name = "longQueryWarningTimeout",
            description = "Timeout in milliseconds after which long query warning will be printed.")
            long longQueryWarningTimeout
    );

    /**
     * @return Long query timeout multiplier.
     */
    @MXBeanDescription("Long query timeout multiplier. The warning will be printed after: timeout, " +
        "timeout * multiplier, timeout * multiplier * multiplier, etc. " +
        "If the multiplier <= 1, the warning message is printed once.")
    int getLongQueryTimeoutMultiplier();

    /**
     * Sets long query timeout multiplier. The warning will be printed after:
     *      - timeout;
     *      - timeout * multiplier;
     *      - timeout * multiplier * multiplier;
     *      - etc.
     * If the multiplier <= 1, the warning message is printed once.
     *
     * @param longQueryTimeoutMultiplier Long query timeout multiplier.
     */
    @MXBeanDescription("Sets long query timeout multiplier. The warning will be printed after: timeout, " +
        "timeout * multiplier, timeout * multiplier * multiplier, etc. " +
        "If the multiplier <= 1, the warning message is printed once.")
    @MXBeanParametersNames("longQueryTimeoutMultiplier")
    @MXBeanParametersDescriptions("Long query timeout multiplier.")
    void setLongQueryTimeoutMultiplier(int longQueryTimeoutMultiplier);

    /**
     * @return Threshold for the number of rows of the result, when count of fetched rows is bigger than the threshold
     *      warning will be printed.
     */
    @MXBeanDescription("Threshold for the number of rows of the result, when count of fetched rows is bigger than the threshold" +
        "warning will be printed.")
    long getResultSetSizeThreshold();

    /**
     * Sets threshold result's row count, when count of fetched rows is bigger than the threshold
     *      warning will be printed.
     *
     * @param rsSizeThreshold Threshold result's row count, when count of fetched rows is bigger than the threshold
     *      warning will be printed.
     */
    @MXBeanDescription("Sets threshold for the number of rows of the result, when count of fetched rows is bigger than the threshold " +
        "warning will be printed")
    @MXBeanParametersNames("rsSizeThreshold")
    @MXBeanParametersDescriptions("Threshold for the number of rows of the result, when count of fetched rows is bigger than the " +
        "threshold warning will be printed.")
    void setResultSetSizeThreshold(long rsSizeThreshold);

    /**
     * Gets result set size threshold multiplier. The warning will be printed after:
     *  - size of result set > threshold;
     *  - size of result set > threshold * multiplier;
     *  - size of result set > threshold * multiplier * multiplier;
     *  - etc.
     * If the multiplier <= 1, the warning message is printed once during query execution and the next one on the query end.
     *
     * @return Result set size threshold multiplier.
     */
    @MXBeanDescription("Gets result set size threshold multiplier. The warning will be printed when size " +
        "of result set is bugger than: threshold, threshold * multiplier, threshold * multiplier * multiplier, " +
        "etc. If the multiplier <= 1, the warning message is printed once during query execution " +
        "and the next one on the query end.")
    int getResultSetSizeThresholdMultiplier();

    /**
     * Sets result set size threshold multiplier.
     *
     * @param rsSizeThresholdMultiplier Result set size threshold multiplier.
     */
    @MXBeanDescription("Sets result set size threshold multiplier. The warning will be printed when size " +
        "of result set is bugger than: threshold, threshold * multiplier, threshold * multiplier * multiplier," +
        "etc. If the multiplier <= 1, the warning message is printed once.")
    @MXBeanParametersNames("rsSizeThresholdMultiplier")
    @MXBeanParametersDescriptions("TResult set size threshold multiplier.")
    void setResultSetSizeThresholdMultiplier(int rsSizeThresholdMultiplier);
}
