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
 * MBean that provides access to information about striped executor service.
 */
@MXBeanDescription("MBean that provides access to information about striped executor service.")
public interface StripedExecutorMXBean {
    /**
     * Checks starvation in striped pool. Maybe too verbose
     * but this is needed to faster debug possible issues.
     */
    @MXBeanDescription("Starvation in striped pool.")
    public void checkStarvation();
    /**
     * @return Stripes count.
     */
    @MXBeanDescription("Stripes count.")
    public int getStripesCount();
    /**
     *
     * @return {@code True} if this executor has been shut down.
     */
    @MXBeanDescription("True if this executor has been shut down.")
    public boolean isShutdown();
    /**
     * Note that
     * {@code isTerminated()} is never {@code true} unless either {@code shutdown()} or
     * {@code shutdownNow()} was called first.
     *
     * @return {@code True} if all tasks have completed following shut down.
     */
    @MXBeanDescription("True if all tasks have completed following shut down.")
    public boolean isTerminated();
    /**
     * @return Return total queue size of all stripes.
     */
    @MXBeanDescription("Total queue size of all stripes.")
    public int getTotalQueueSize();
    /**
     * @return Completed tasks count.
     */
    @MXBeanDescription("Completed tasks count.")
    public long getTotalCompletedTasksCount();
    /**
     * @return Number of completed tasks per stripe.
     */
    @MXBeanDescription("Number of completed tasks per stripe.")
    public long[] getStripesCompletedTasksCount();
    /**
     * @return Number of active tasks.
     */
    @MXBeanDescription("Number of active tasks.")
    public int getActiveCount();
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
    /**
     * {@inheritDoc}
     */
    public String toString();
}
