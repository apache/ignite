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
 * MBean that provides access to information about striped threadpool executor service.
 */
@MXBeanDescription("MBean that provides access to information about striped executor service.")
public interface IgniteStripedThreadPoolExecutorMXBean extends ThreadPoolMXBean {

    /**
     * @return Stripes count.
     */
    @MXBeanDescription("Stripes count.")
    public int getStripesCount();
    
    
    @MXBeanDescription("Number of completed tasks per stripe.")
    public long[] getStripesCompletedTasksCounts();

    /**
     * @return Number of active tasks per stripe.
     */
    @MXBeanDescription("Number of active tasks per stripe.")
    public boolean[] getStripesActiveStatuses();

    /**
     * @return Size of queue per stripe.
     */
    @MXBeanDescription("Size of queue per stripe.")
    public int[] getStripesQueueSizes();

}
