/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.apache.ignite.*;

import java.util.*;

/**
 * Hadoop job.
 */
public interface GridHadoopJob {
    /**
     * Gets job ID.
     *
     * @return Job ID.
     */
    public GridHadoopJobId id();

    /**
     * Gets job information.
     *
     * @return Job information.
     */
    public GridHadoopJobInfo info();

    /**
     * Gets collection of input splits for this job.
     *
     * @return Input splits.
     */
    public Collection<GridHadoopInputSplit> input() throws IgniteCheckedException;

    /**
     * Returns context for task execution.
     *
     * @param info Task info.
     * @return Task Context.
     * @throws IgniteCheckedException If failed.
     */
    public GridHadoopTaskContext getTaskContext(GridHadoopTaskInfo info) throws IgniteCheckedException;

    /**
     * Does all the needed initialization for the job. Will be called on each node where tasks for this job must
     * be executed.
     * <p>
     * If job is running in external mode this method will be called on instance in GridGain node with parameter
     * {@code false} and on instance in external process with parameter {@code true}.
     *
     * @param external If {@code true} then this job instance resides in external process.
     * @param locNodeId Local node ID.
     * @throws IgniteCheckedException If failed.
     */
    public void initialize(boolean external, UUID locNodeId) throws IgniteCheckedException;

    /**
     * Release all the resources.
     * <p>
     * If job is running in external mode this method will be called on instance in GridGain node with parameter
     * {@code false} and on instance in external process with parameter {@code true}.
     *
     * @param external If {@code true} then this job instance resides in external process.
     * @throws IgniteCheckedException If failed.
     */
    public void dispose(boolean external) throws IgniteCheckedException;

    /**
     * Prepare local environment for the task.
     *
     * @param info Task info.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareTaskEnvironment(GridHadoopTaskInfo info) throws IgniteCheckedException;

    /**
     * Cleans up local environment of the task.
     *
     * @param info Task info.
     * @throws IgniteCheckedException If failed.
     */
    public void cleanupTaskEnvironment(GridHadoopTaskInfo info) throws IgniteCheckedException;

    /**
     * Cleans up the job staging directory.
     */
    void cleanupStagingDirectory();
}
