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
    void setLongQueryTimeoutMultiplier(
        @MXBeanParameter(name = "longQueryTimeoutMultiplier", description = "Long query timeout multiplier.")
            int longQueryTimeoutMultiplier
    );
}
