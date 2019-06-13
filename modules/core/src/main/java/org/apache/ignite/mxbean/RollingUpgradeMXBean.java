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

import java.util.List;
import org.apache.ignite.internal.processors.ru.RollingUpgradeProcessor;

/**
 * This interface defines JMX view on {@link RollingUpgradeProcessor}.
 */
@MXBeanDescription("MBean that provides access to rolling upgrade.")
public interface RollingUpgradeMXBean {
    @MXBeanDescription("State of rolling upgrade (enabled/disabled).")
    boolean isEnabled();

    /** */
    @MXBeanDescription("The initial cluster version.")
    String getInitialVersion();

    /** */
    @MXBeanDescription("The target cluster version.")
    String getUpdateVersion();

    /** */
    @MXBeanDescription("Feature set that is supported by nodes.")
    List<String> getSupportedFeatures();

    /** */
    @MXBeanDescription("Set rolling upgrade mode value.")
    @MXBeanParametersNames("enable")
    @MXBeanParametersDescriptions("Rolling upgrade mode.")
    void changeMode(boolean enable);
}
