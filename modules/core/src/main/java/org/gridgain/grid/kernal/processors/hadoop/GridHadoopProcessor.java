/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;

/**
 * Hadoop processor.
 */
public abstract class GridHadoopProcessor extends GridProcessorAdapter {
    /** Implementation class name. */
    private static final String CLS_NAME = "org.gridgain.grid.kernal.processors.hadoop.GridHadoopOpProcessor";

    /**
     * @param ctx Kernal context.
     */
    protected GridHadoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

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
    public abstract GridFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo);

    /**
     * Gets hadoop job execution status.
     *
     * @param jobId Job ID to get status for.
     * @return Job execution status.
     */
    public abstract GridHadoopJobStatus status(GridHadoopJobId jobId) throws GridException;
}
