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

import org.apache.ignite.internal.cluster.DistributedBaselineConfiguration;

/**
 * This interface defines JMX view on {@link DistributedBaselineConfiguration}.
 */
public interface BaselineAutoAdjustMXBean {
    /**
     * @return Whether baseline autoadjustment is enabled ot not.
     * @see org.apache.ignite.internal.management.api.CommandMBean
     * @deprecated Use managements API beans, instead.
     */
    @Deprecated
    @MXBeanDescription("Whether baseline autoadjustment is enabled ot not.")
    boolean isAutoAdjustmentEnabled();

    /** @return Baseline autoadjustment timeout value. */
    @MXBeanDescription("Baseline autoadjustment timeout value.")
    long getAutoAdjustmentTimeout();

    /**
     * @return Time until baseline will be adjusted automatically.
     * @see org.apache.ignite.internal.management.api.CommandMBean
     * @deprecated Use managements API beans, instead.
     */
    @MXBeanDescription("Time until baseline will be adjusted automatically.")
    long getTimeUntilAutoAdjust();

    /** @return State of task of auto-adjust. */
    @MXBeanDescription("State of task of auto-adjust(IN_PROGRESS, SCHEDULED, NOT_SCHEDULED).")
    String getTaskState();

    /**
     * @param enabled Enable/disable baseline autoadjustment flag.
     * @see org.apache.ignite.internal.management.api.CommandMBean
     * @deprecated Use managements API beans, instead.
     */
    @Deprecated
    @MXBeanDescription("Enable/disable baseline autoadjustment feature.")
    public void setAutoAdjustmentEnabled(
        @MXBeanParameter(name = "enabled", description = "Enable/disable flag.") boolean enabled
    );

    /**
     * @param timeout Timeout value.
     * @see org.apache.ignite.internal.management.api.CommandMBean
     * @deprecated Use managements API beans, instead.
     */
    @Deprecated
    @MXBeanDescription("Set baseline autoadjustment timeout value.")
    public void setAutoAdjustmentTimeout(
        @MXBeanParameter(name = "timeout", description = "Timeout value.") long timeout
    );
}
