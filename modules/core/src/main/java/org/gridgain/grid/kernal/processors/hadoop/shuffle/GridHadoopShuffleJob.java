/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.hadoop.*;

/**
 * Shuffle job.
 */
public class GridHadoopShuffleJob implements AutoCloseable {
    /** */
    private final GridHadoopJob job;

    /**
     * @param job Job.
     */
    public GridHadoopShuffleJob(GridHadoopJob job) {
        this.job = job;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // TODO
    }

    public GridHadoopTaskOutput output(GridHadoopTaskInfo taskInfo) {
        return null;
    }

    public GridHadoopTaskInput input(GridHadoopTaskInfo taskInfo) {
        return null;
    }
}
