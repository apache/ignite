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
import org.gridgain.grid.util.future.*;

/**
 * Hadoop processor.
 */
public class GridHadoopNopProcessor extends GridHadoopProcessor {
    /**
     * @param ctx Kernal context.
     */
    public GridHadoopNopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @return Collection of generated IDs.
     */
    public GridHadoopJobId nextJobId() {
        return null;
    }

    /**
     * Submits job to job tracker.
     *
     * @param jobId Job ID to submit.
     * @param jobInfo Job info to submit.
     * @return Execution future.
     */
    @Override public GridFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo) {
        return new GridFinishedFutureEx<>(new GridException("Hadoop is not available."));
    }

    /**
     * Gets hadoop job execution status.
     *
     * @param jobId Job ID to get status for.
     * @return Job execution status.
     */
    @Override public GridHadoopJobStatus status(GridHadoopJobId jobId) throws GridException {
        return new GridHadoopJobStatus(new GridFinishedFutureEx<>(new GridException("Hadoop is not available")), null);
    }

    @Override public GridHadoopJobStatus status(GridHadoopJobId jobId, long pollTimeout) throws GridException {
        return new GridHadoopJobStatus(new GridFinishedFutureEx<>(new GridException("Hadoop is not available")), null);
    }
}
