/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;

/**
 * Shuffle component.
 */
public class GridHadoopShuffler extends GridHadoopComponent {
    /** Embedded shuffle implementation to delegate to. */
    private GridHadoopEmbeddedShuffle shuffle;

    /** {@inheritDoc} */
    @Override public void start(GridHadoopContext ctx) throws GridException {
        shuffle = new GridHadoopEmbeddedShuffle(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        shuffle.start();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        shuffle.stop(cancel);
    }

    /**
     * Gets task output for task info.
     *
     * @param taskInfo Task info.
     * @return Task output.
     * @throws GridException If failed.
     */
    public GridHadoopTaskOutput output(GridHadoopTaskInfo taskInfo) throws GridException {
        return shuffle.output(taskInfo);
    }

    /**
     * Gets task input for task info.
     * @param taskInfo Task info.
     * @return Task input.
     * @throws GridException If failed.
     */
    public GridHadoopTaskInput input(GridHadoopTaskInfo taskInfo) throws GridException {
        return shuffle.input(taskInfo);
    }

    /**
     * Job finished callback.
     *
     * @param jobId Finished job ID.
     */
    public void jobFinished(GridHadoopJobId jobId) {
        shuffle.jobFinished(jobId);
    }

    /**
     * Flushes all pending messages.
     *
     * @param jobId Job ID to flush.
     * @return Flush future.
     */
    public GridFuture<?> flush(GridHadoopJobId jobId) {
        return shuffle.flush(jobId);
    }
}
