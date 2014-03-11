/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer;

import org.gridgain.grid.util.mbean.*;

/**
 * Streamer stage MBean.
 */
@GridMBeanDescription("MBean that provides access to streamer stage description and metrics.")
public interface GridStreamerStageMBean {
    /**
     * Gets stage name.
     *
     * @return Stage name.
     */
    @GridMBeanDescription("Stage name.")
    public String getName();

    /**
     * Gets stage class name.
     *
     * @return Stage class name.
     */
    @GridMBeanDescription("Stage class name.")
    public String getStageClassName();

    /**
     * Gets stage minimum execution time.
     *
     * @return Stage minimum execution time.
     */
    @GridMBeanDescription("Stage minimum execution time.")
    public long getMinimumExecutionTime();

    /**
     * Gets stage maximum execution time.
     *
     * @return Stage maximum execution time.
     */
    @GridMBeanDescription("Stage maximum execution time.")
    public long getMaximumExecutionTime();

    /**
     * Gets stage average execution time.
     *
     * @return Stage average execution time.
     */
    @GridMBeanDescription("Stage average execution time.")
    public long getAverageExecutionTime();

    /**
     * Gets stage minimum waiting time.
     *
     * @return Stage minimum waiting time.
     */
    @GridMBeanDescription("Stage minimum waiting time.")
    public long getMinimumWaitingTime();

    /**
     * Gets stage maximum waiting time.
     *
     * @return Stage maximum waiting time.
     */
    @GridMBeanDescription("Stage maximum waiting time.")
    public long getMaximumWaitingTime();

    /**
     * Stage average waiting time.
     *
     * @return Stage average waiting time.
     */
    @GridMBeanDescription("Stage average waiting time.")
    public long getAverageWaitingTime();

    /**
     * Gets total stage execution count since last reset.
     *
     * @return Number of times this stage was executed.
     */
    @GridMBeanDescription("Number of times this stage was executed.")
    public long getTotalExecutionCount();

    /**
     * Gets stage failure count.
     *
     * @return Stage failure count.
     */
    @GridMBeanDescription("Stage failure count.")
    public int getFailuresCount();

    /**
     * Gets flag indicating if stage is being currently executed by at least one thread on current node.
     *
     * @return {@code True} if stage is executing now.
     */
    @GridMBeanDescription("Whether stage is currently being executed.")
    public boolean isExecuting();
}
