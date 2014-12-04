/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

/**
 * Hadoop facade providing access to GridGain Hadoop features.
 */
public interface GridHadoop {
    /**
     * Gets Hadoop module configuration.
     *
     * @return Hadoop module configuration.
     */
    public GridHadoopConfiguration configuration();

    /**
     * Generate next job ID.
     *
     * @return Next job ID.
     */
    public GridHadoopJobId nextJobId();

    /**
     * Submits job to job tracker.
     *
     * @param jobId Job ID to submit.
     * @param jobInfo Job info to submit.
     * @return Execution future.
     */
    public IgniteFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo);

    /**
     * Gets Hadoop job execution status.
     *
     * @param jobId Job ID to get status for.
     * @return Job execution status or {@code null} in case job with the given ID is not found.
     * @throws GridException If failed.
     */
    @Nullable public GridHadoopJobStatus status(GridHadoopJobId jobId) throws GridException;

    /**
     * Returns job counters.
     *
     * @param jobId Job ID to get counters for.
     * @return Job counters object.
     * @throws GridException If failed.
     */
    public GridHadoopCounters counters(GridHadoopJobId jobId) throws GridException;

    /**
     * Gets Hadoop finish future for particular job.
     *
     * @param jobId Job ID.
     * @return Job finish future or {@code null} in case job with the given ID is not found.
     * @throws GridException If failed.
     */
    @Nullable public IgniteFuture<?> finishFuture(GridHadoopJobId jobId) throws GridException;

    /**
     * Kills job.
     *
     * @param jobId Job ID.
     * @return {@code True} if job was killed.
     * @throws GridException If failed.
     */
    public boolean kill(GridHadoopJobId jobId) throws GridException;
}
