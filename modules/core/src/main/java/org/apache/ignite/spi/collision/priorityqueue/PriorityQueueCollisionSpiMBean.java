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

package org.apache.ignite.spi.collision.priorityqueue;

import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;

/**
 * Management bean that provides access to the priority queue collision SPI configuration.
 */
@MXBeanDescription("MBean provides access to the priority queue collision SPI.")
public interface PriorityQueueCollisionSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets current number of jobs that wait for the execution.
     *
     * @return Number of jobs that wait for execution.
     */
    @MXBeanDescription("Number of jobs that wait for execution.")
    public int getCurrentWaitJobsNumber();

    /**
     * Gets current number of jobs that are active, i.e. {@code 'running + held'} jobs.
     *
     * @return Number of active jobs.
     */
    @MXBeanDescription("Number of active jobs.")
    public int getCurrentActiveJobsNumber();

    /*
     * Gets number of currently running (not {@code 'held}) jobs.
     *
     * @return Number of currently running (not {@code 'held}) jobs.
     */
    @MXBeanDescription("Number of running jobs.")
    public int getCurrentRunningJobsNumber();

    /**
     * Gets number of currently {@code 'held'} jobs.
     *
     * @return Number of currently {@code 'held'} jobs.
     */
    @MXBeanDescription("Number of held jobs.")
    public int getCurrentHeldJobsNumber();

    /**
     * Gets number of jobs that can be executed in parallel.
     *
     * @return Number of jobs that can be executed in parallel.
     */
    @MXBeanDescription("Number of jobs that can be executed in parallel.")
    public int getParallelJobsNumber();

    /**
     * Sets number of jobs that can be executed in parallel.
     *
     * @param num Parallel jobs number.
     */
    @MXBeanDescription("Number of jobs that can be executed in parallel.")
    public void setParallelJobsNumber(int num);

    /**
     * Maximum number of jobs that are allowed to wait in waiting queue. If number
     * of waiting jobs ever exceeds this number, excessive jobs will be rejected.
     *
     * @return Maximum allowed number of waiting jobs.
     */
    @MXBeanDescription("Maximum allowed number of waiting jobs.")
    public int getWaitingJobsNumber();

    /**
     * Maximum number of jobs that are allowed to wait in waiting queue. If number
     * of waiting jobs ever exceeds this number, excessive jobs will be rejected.
     *
     * @param num Maximium jobs number.
     */
    @MXBeanDescription("Maximum allowed number of waiting jobs.")
    public void setWaitingJobsNumber(int num);

    /**
     * Gets key name of task priority attribute.
     *
     * @return Key name of task priority attribute.
     */
    @MXBeanDescription("Key name of task priority attribute.")
    public String getPriorityAttributeKey();

    /**
     * Gets key name of job priority attribute.
     *
     * @return Key name of job priority attribute.
     */
    @MXBeanDescription("Key name of job priority attribute.")
    public String getJobPriorityAttributeKey();

    /**
     * Gets default priority to use if a job does not have priority attribute
     * set.
     *
     * @return Default priority to use if a task does not have priority
     *      attribute set.
     */
    @MXBeanDescription("Default priority to use if a task does not have priority attribute set.")
    public int getDefaultPriority();

    /**
     * Sets default priority to use if a job does not have priority attribute set.
     *
     * @param priority default priority.
     */
    @MXBeanDescription("Default priority to use if a task does not have priority attribute set.")
    public void setDefaultPriority(int priority);

    /**
     * Gets value to increment job priority by every time a lower priority job gets
     * behind a higher priority job.
     *
     * @return Value to increment job priority by every time a lower priority job gets
     *      behind a higher priority job.
     */
    @MXBeanDescription("Value to increment job priority by every time a lower priority job gets behind a higher priority job.")
    public int getStarvationIncrement();

    /**
     * Sets value to increment job priority by every time a lower priority job gets
     * behind a higher priority job.
     *
     * @param increment Increment value.
     */
    @MXBeanDescription("Value to increment job priority by every time a lower priority job gets behind a higher priority job.")
    public void setStarvationIncrement(int increment);

    /**
     * Gets flag indicating whether job starvation prevention is enabled.
     *
     * @return Flag indicating whether job starvation prevention is enabled.
     */
    @MXBeanDescription("Flag indicating whether job starvation prevention is enabled.")
    public boolean isStarvationPreventionEnabled();

    /**
     * Sets flag indicating whether job starvation prevention is enabled.
     *
     * @param preventStarvation Flag indicating whether job starvation prevention is enabled.
     */
    @MXBeanDescription("Flag indicating whether job starvation prevention is enabled.")
    public void setStarvationPreventionEnabled(boolean preventStarvation);
}