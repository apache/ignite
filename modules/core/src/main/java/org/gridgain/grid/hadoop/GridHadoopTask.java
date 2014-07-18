/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;

import java.io.*;

/**
 * Hadoop task.
 */
public abstract class GridHadoopTask {
    /** */
    private GridHadoopTaskInfo taskInfo;

    /**
     * Creates task.
     *
     * @param taskInfo Task info.
     */
    public GridHadoopTask(GridHadoopTaskInfo taskInfo) {
        assert taskInfo != null;

        this.taskInfo = taskInfo;
    }

    /**
     * For {@link Externalizable}.
     */
    public GridHadoopTask() {
        // No-op.
    }

    /**
     * Gets task info.
     *
     * @return Task info.
     */
    public GridHadoopTaskInfo info() {
        return taskInfo;
    }

    /**
     * Runs task.
     *
     * @param taskCtx Context.
     * @param log Logger.
     * @throws GridInterruptedException If interrupted.
     * @throws GridException If failed.
     */
    public abstract void run(GridHadoopTaskContext taskCtx, GridLogger log) throws GridException;

    /**
     * Interrupts task execution.
     */
    public abstract void cancel();
}
