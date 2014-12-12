/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;

/**
 * Hadoop processor.
 */
public abstract class GridHadoopProcessorAdapter extends GridProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    protected GridHadoopProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @return Hadoop facade.
     */
    public abstract GridHadoop hadoop();

    /**
     * @return Hadoop configuration.
     */
    public abstract GridHadoopConfiguration config();

    /**
     * @return Collection of generated IDs.
     */
    public abstract GridHadoopJobId nextJobId();

    /**
     * Submits job to job tracker.
     *
     * @param jobId Job ID to submit.
     * @param jobInfo Job info to submit.
     * @return Execution future.
     */
    public abstract IgniteFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo);

    /**
     * Gets Hadoop job execution status.
     *
     * @param jobId Job ID to get status for.
     * @return Job execution status.
     * @throws IgniteCheckedException If failed.
     */
    public abstract GridHadoopJobStatus status(GridHadoopJobId jobId) throws IgniteCheckedException;

    /**
     * Returns Hadoop job counters.
     *
     * @param jobId Job ID to get counters for.
     * @return Job counters.
     * @throws IgniteCheckedException If failed.
     */
    public abstract GridHadoopCounters counters(GridHadoopJobId jobId) throws IgniteCheckedException;

    /**
     * Gets Hadoop job finish future.
     *
     * @param jobId Job ID.
     * @return Job finish future or {@code null}.
     * @throws IgniteCheckedException If failed.
     */
    public abstract IgniteFuture<?> finishFuture(GridHadoopJobId jobId) throws IgniteCheckedException;

    /**
     * Kills job.
     *
     * @param jobId Job ID.
     * @return {@code True} if job was killed.
     * @throws IgniteCheckedException If failed.
     */
    public abstract boolean kill(GridHadoopJobId jobId) throws IgniteCheckedException;
}
