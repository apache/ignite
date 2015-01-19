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

package org.apache.ignite.spi.collision.jobstealing;

import org.apache.ignite.mbean.*;
import org.apache.ignite.spi.*;

import java.io.*;
import java.util.*;

/**
 * Management MBean for job stealing based collision SPI.
 */
@IgniteMBeanDescription("MBean for job stealing based collision SPI.")
public interface JobStealingCollisionSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets current number of jobs to be stolen. This is outstanding
     * requests number.
     *
     * @return Number of jobs to be stolen.
     */
    @IgniteMBeanDescription("Number of jobs to be stolen.")
    public int getCurrentJobsToStealNumber();

    /**
     * Gets current number of jobs that wait for the execution.
     *
     * @return Number of jobs that wait for execution.
     */
    @IgniteMBeanDescription("Number of jobs that wait for execution.")
    public int getCurrentWaitJobsNumber();

    /**
     * Gets current number of jobs that are being executed.
     *
     * @return Number of active jobs.
     */
    @IgniteMBeanDescription("Number of active jobs.")
    public int getCurrentActiveJobsNumber();

    /*
     * Gets number of currently running (not {@code 'held}) jobs.
     *
     * @return Number of currently running (not {@code 'held}) jobs.
     */
    @IgniteMBeanDescription("Number of running jobs.")
    public int getCurrentRunningJobsNumber();

    /**
     * Gets number of currently {@code 'held'} jobs.
     *
     * @return Number of currently {@code 'held'} jobs.
     */
    @IgniteMBeanDescription("Number of held jobs.")
    public int getCurrentHeldJobsNumber();

    /**
     * Gets total number of stolen jobs.
     *
     * @return Number of stolen jobs.
     */
    @IgniteMBeanDescription("Number of stolen jobs.")
    public int getTotalStolenJobsNumber();

    /**
     * Gets number of jobs that can be executed in parallel.
     *
     * @return Number of jobs that can be executed in parallel.
     */
    @IgniteMBeanDescription("Number of jobs that can be executed in parallel.")
    public int getActiveJobsThreshold();

    /**
     * Sets number of jobs that can be executed in parallel.
     *
     * @param activeJobsTreshold Number of jobs that can be executed in parallel.
     */
    @IgniteMBeanDescription("Number of jobs that can be executed in parallel.")
    public void setActiveJobsThreshold(int activeJobsTreshold);

    /**
     * Gets job count threshold at which this node will
     * start stealing jobs from other nodes.
     *
     * @return Job count threshold.
     */
    @IgniteMBeanDescription("Job count threshold.")
    public int getWaitJobsThreshold();

    /**
     * Sets job count threshold at which this node will
     * start stealing jobs from other nodes.
     *
     * @param waitJobsThreshold Job count threshold.
     */
    @IgniteMBeanDescription("Job count threshold.")
    public void setWaitJobsThreshold(int waitJobsThreshold);

    /**
     * Message expire time configuration parameter. If no response is received
     * from a busy node to a job stealing message, then implementation will
     * assume that message never got there, or that remote node does not have
     * this node included into topology of any of the jobs it has.
     *
     * @return Message expire time.
     */
    @IgniteMBeanDescription("Message expire time.")
    public long getMessageExpireTime();

    /**
     * Message expire time configuration parameter. If no response is received
     * from a busy node to a job stealing message, then implementation will
     * assume that message never got there, or that remote node does not have
     * this node included into topology of any of the jobs it has.
     *
     * @param msgExpireTime Message expire time.
     */
    @IgniteMBeanDescription("Message expire time.")
    public void setMessageExpireTime(long msgExpireTime);

    /**
     * Gets flag indicating whether this node should attempt to steal jobs
     * from other nodes. If {@code false}, then this node will steal allow
     * jobs to be stolen from it, but won't attempt to steal any jobs from
     * other nodes.
     * <p>
     * Default value is {@code true}.
     *
     * @return Flag indicating whether this node should attempt to steal jobs
     *      from other nodes.
     */
    @IgniteMBeanDescription("Flag indicating whether this node should attempt to steal jobs from other nodes.")
    public boolean isStealingEnabled();

    /**
     * Gets flag indicating whether this node should attempt to steal jobs
     * from other nodes. If {@code false}, then this node will steal allow
     * jobs to be stolen from it, but won't attempt to steal any jobs from
     * other nodes.
     * <p>
     * Default value is {@code true}.
     *
     * @param stealingEnabled Flag indicating whether this node should attempt to steal jobs
     *      from other nodes.
     */
    @IgniteMBeanDescription("Flag indicating whether this node should attempt to steal jobs from other nodes.")
    public void setStealingEnabled(boolean stealingEnabled);

    /**
     * Gets maximum number of attempts to steal job by another node.
     * If not specified, {@link JobStealingCollisionSpi#DFLT_MAX_STEALING_ATTEMPTS}
     * value will be used.
     *
     * @return Maximum number of attempts to steal job by another node.
     */
    @IgniteMBeanDescription("Maximum number of attempts to steal job by another node.")
    public int getMaximumStealingAttempts();

    /**
     * Gets maximum number of attempts to steal job by another node.
     * If not specified, {@link JobStealingCollisionSpi#DFLT_MAX_STEALING_ATTEMPTS}
     * value will be used.
     *
     * @param maximumStealingAttempts Maximum number of attempts to steal job by another node.
     */
    @IgniteMBeanDescription("Maximum number of attempts to steal job by another node.")
    public void setMaximumStealingAttempts(int maximumStealingAttempts);

    /**
     * Configuration parameter to enable stealing to/from only nodes that
     * have these attributes set (see {@link org.apache.ignite.cluster.ClusterNode#attribute(String)} and
     * {@link org.apache.ignite.configuration.IgniteConfiguration#getUserAttributes()} methods).
     *
     * @return Node attributes to enable job stealing for.
     */
    @IgniteMBeanDescription("Node attributes to enable job stealing for.")
    public Map<String, ? extends Serializable> getStealingAttributes();
}
