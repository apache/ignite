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

import java.util.*;

/**
 * Hadoop processor.
 */
public class GridHadoopNoopProcessor extends GridHadoopProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    public GridHadoopNoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param cnt Number of IDs to generate.
     * @return Collection of generated IDs.
     */
    @Override public Collection<GridHadoopJobId> getNextJobIds(int cnt) {
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
}
