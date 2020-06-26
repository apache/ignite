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

package org.apache.ignite.internal.processors.performancestatistics;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.mxbean.MXBeanDescription;

/**
 * MBean that provides access to performance statistics management.
 */
@IgniteExperimental
@MXBeanDescription("MBean provide access to performance statistics management.")
public interface IgnitePerformanceStatisticsMBean {
    /**
     * Start collecting performance statistics in the cluster with default settings.
     *
     * @see FilePerformanceStatisticsWriter#DFLT_FILE_MAX_SIZE
     * @see FilePerformanceStatisticsWriter#DFLT_BUFFER_SIZE
     * @see FilePerformanceStatisticsWriter#DFLT_FLUSH_SIZE
     */
    @MXBeanDescription("Start collecting performance statistics in the cluster.")
    public void start() throws IgniteCheckedException;

    /** Stop collecting performance statistics in the cluster. */
    @MXBeanDescription("Stop collecting performance statistics in the cluster.")
    public void stop() throws IgniteCheckedException;

    /** @return {@code True} if collecting performance statistics is enabled. */
    @MXBeanDescription("True if collecting performance statistics is enabled.")
    public boolean enabled();
}
