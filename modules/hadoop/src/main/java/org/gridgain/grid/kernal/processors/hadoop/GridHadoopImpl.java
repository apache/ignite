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
import org.jetbrains.annotations.*;

/**
 * Hadoop facade implementation.
 */
public class GridHadoopImpl implements GridHadoop {
    /** Hadoop processor. */
    private final GridHadoopProcessor proc;

    /**
     * Constructor.
     *
     * @param proc Hadoop processor.
     */
    GridHadoopImpl(GridHadoopProcessor proc) {
        this.proc = proc;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo jobInfo) {
        return proc.submit(jobId, jobInfo);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridHadoopJobStatus status(GridHadoopJobId jobId) throws GridException {
        return proc.status(jobId);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridFuture<?> finishFuture(GridHadoopJobId jobId) throws GridException {
        return proc.finishFuture(jobId);
    }
}
