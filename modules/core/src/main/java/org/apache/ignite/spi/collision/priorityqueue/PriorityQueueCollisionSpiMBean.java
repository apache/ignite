/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.collision.priorityqueue;

import org.apache.ignite.mbean.*;
import org.apache.ignite.spi.*;

/**
 * Management bean that provides access to the priority queue collision SPI configuration.
 */
@IgniteMBeanDescription("MBean provides access to the priority queue collision SPI.")
public interface PriorityQueueCollisionSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Gets current number of jobs that wait for the execution.
     *
     * @return Number of jobs that wait for execution.
     */
    @IgniteMBeanDescription("Number of jobs that wait for execution.")
    public int getCurrentWaitJobsNumber();

    /**
     * Gets current number of jobs that are active, i.e. {@code 'running + held'} jobs.
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
     * Gets number of jobs that can be executed in parallel.
     *
     * @return Number of jobs that can be executed in parallel.
     */
    @IgniteMBeanDescription("Number of jobs that can be executed in parallel.")
    public int getParallelJobsNumber();

    /**
     * Sets number of jobs that can be executed in parallel.
     *
     * @param num Parallel jobs number.
     */
    @IgniteMBeanDescription("Number of jobs that can be executed in parallel.")
    public void setParallelJobsNumber(int num);

    /**
     * Maximum number of jobs that are allowed to wait in waiting queue. If number
     * of waiting jobs ever exceeds this number, excessive jobs will be rejected.
     *
     * @return Maximum allowed number of waiting jobs.
     */
    @IgniteMBeanDescription("Maximum allowed number of waiting jobs.")
    public int getWaitingJobsNumber();

    /**
     * Maximum number of jobs that are allowed to wait in waiting queue. If number
     * of waiting jobs ever exceeds this number, excessive jobs will be rejected.
     *
     * @param num Maximium jobs number.
     */
    @IgniteMBeanDescription("Maximum allowed number of waiting jobs.")
    public void setWaitingJobsNumber(int num);

    /**
     * Gets key name of task priority attribute.
     *
     * @return Key name of task priority attribute.
     */
    @IgniteMBeanDescription("Key name of task priority attribute.")
    public String getPriorityAttributeKey();

    /**
     * Gets key name of job priority attribute.
     *
     * @return Key name of job priority attribute.
     */
    @IgniteMBeanDescription("Key name of job priority attribute.")
    public String getJobPriorityAttributeKey();

    /**
     * Gets default priority to use if a job does not have priority attribute
     * set.
     *
     * @return Default priority to use if a task does not have priority
     *      attribute set.
     */
    @IgniteMBeanDescription("Default priority to use if a task does not have priority attribute set.")
    public int getDefaultPriority();

    /**
     * Sets default priority to use if a job does not have priority attribute set.
     *
     * @param priority default priority.
     */
    @IgniteMBeanDescription("Default priority to use if a task does not have priority attribute set.")
    public void setDefaultPriority(int priority);

    /**
     * Gets value to increment job priority by every time a lower priority job gets
     * behind a higher priority job.
     *
     * @return Value to increment job priority by every time a lower priority job gets
     *      behind a higher priority job.
     */
    @IgniteMBeanDescription("Value to increment job priority by every time a lower priority job gets behind a higher priority job.")
    public int getStarvationIncrement();

    /**
     * Sets value to increment job priority by every time a lower priority job gets
     * behind a higher priority job.
     *
     * @param increment Increment value.
     */
    @IgniteMBeanDescription("Value to increment job priority by every time a lower priority job gets behind a higher priority job.")
    public void setStarvationIncrement(int increment);

    /**
     * Gets flag indicating whether job starvation prevention is enabled.
     *
     * @return Flag indicating whether job starvation prevention is enabled.
     */
    @IgniteMBeanDescription("Flag indicating whether job starvation prevention is enabled.")
    public boolean isStarvationPreventionEnabled();

    /**
     * Sets flag indicating whether job starvation prevention is enabled.
     *
     * @param preventStarvation Flag indicating whether job starvation prevention is enabled.
     */
    @IgniteMBeanDescription("Flag indicating whether job starvation prevention is enabled.")
    public void setStarvationPreventionEnabled(boolean preventStarvation);
}
