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

package org.apache.ignite.streamer;

import org.apache.ignite.mxbean.*;

/**
 * Streamer window MBean.
 */
@MXBeanDescription("MBean that provides access to streamer window description.")
public interface StreamerWindowMBean {
    /**
     * Gets window name.
     *
     * @return Window name.
     */
    @MXBeanDescription("Window name.")
    public String getName();

    /**
     * Gets window class name.
     *
     * @return Window class name.
     */
    @MXBeanDescription("Window class name.")
    public String getClassName();

    /**
     * Gets current window size.
     *
     * @return Current window size.
     */
    @MXBeanDescription("Window size.")
    public int getSize();

    /**
     * Gets estimate for window eviction queue size.
     *
     * @return Eviction queue size estimate.
     */
    @MXBeanDescription("Eviction queue size estimate.")
    public int getEvictionQueueSize();
}
