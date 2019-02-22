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
public interface BaselineConfigurationXMBean {
    /** */
    @MXBeanDescription("")
    boolean isAutoAdjustmentEnabled();

    /** */
    @MXBeanDescription("")
    long getAutoAdjustmentTimeout();

    /** */
    @MXBeanDescription("")
    void enableAutoAdjustment();

    /** */
    @MXBeanDescription("")
    void disableAutoAdjustment();

    /** */
    @MXBeanDescription("")
    public void setAutoAdjustmentEnabled(boolean enabled);

    /** */
    @MXBeanDescription("")
    public void setAutoAdjustmentTimeout(long timeout);
}
