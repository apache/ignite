// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.collision.fifoqueue;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.mbean.*;

/**
 * Management bean that provides access to the FIFO queue collision SPI configuration.
 *
 * @author @java.author
 * @version @java.version
 */
@GridMBeanDescription("MBean provides information about FIFO queue based collision SPI configuration.")
public interface GridFifoQueueCollisionSpiMBean extends GridSpiManagementMBean {
    /**
     * Gets number of jobs that can be executed in parallel.
     *
     * @return Number of jobs that can be executed in parallel.
     */
    @GridMBeanDescription("Number of jobs that can be executed in parallel.")
    public int getParallelJobsNumber();

    /**
     * Sets number of jobs that can be executed in parallel.
     *
     * @param num Parallel jobs number.
     */
    @GridMBeanDescription("Number of jobs that can be executed in parallel.")
    public void setParallelJobsNumber(int num);

    /**
     * Maximum number of jobs that are allowed to wait in waiting queue. If number
     * of waiting jobs ever exceeds this number, excessive jobs will be rejected.
     *
     * @return Maximum allowed number of waiting jobs.
     */
    @GridMBeanDescription("Maximum allowed number of waiting jobs.")
    public int getWaitingJobsNumber();

    /**
     * Sets maximum number of jobs that are allowed to wait in waiting queue. If number
     * of waiting jobs ever exceeds this number, excessive jobs will be rejected.
     *
     * @param num Waiting jobs number.
     */
    @GridMBeanDescription("Maximum allowed number of waiting jobs.")
    public void setWaitingJobsNumber(int num);

    /**
     * Gets current number of jobs that wait for the execution.
     *
     * @return Number of jobs that wait for execution.
     */
    @GridMBeanDescription("Number of jobs that wait for execution.")
    public int getCurrentWaitJobsNumber();

    /**
     * Gets current number of jobs that are active, i.e. {@code 'running + held'} jobs.
     *
     * @return Number of active jobs.
     */
    @GridMBeanDescription("Number of active jobs.")
    public int getCurrentActiveJobsNumber();

    /*
     * Gets number of currently running (not {@code 'held}) jobs.
     *
     * @return Number of currently running (not {@code 'held}) jobs.
     */
    @GridMBeanDescription("Number of running jobs.")
    public int getCurrentRunningJobsNumber();

    /**
     * Gets number of currently {@code 'held'} jobs.
     *
     * @return Number of currently {@code 'held'} jobs.
     */
    @GridMBeanDescription("Number of held jobs.")
    public int getCurrentHeldJobsNumber();
}
