/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v1;

import org.apache.hadoop.mapred.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;

import java.io.*;

/**
 * Hadoop cleanup task implementation for v1 API.
 */
public class GridHadoopV1CleanupTask extends GridHadoopV1Task {
    /** Abort flag. */
    private final boolean abort;

    /**
     * @param taskInfo Task info.
     * @param abort Abort flag.
     */
    public GridHadoopV1CleanupTask(GridHadoopTaskInfo taskInfo, boolean abort) {
        super(taskInfo);

        this.abort = abort;
    }

    /** {@inheritDoc} */
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridException {
        GridHadoopV2TaskContext ctx = (GridHadoopV2TaskContext)taskCtx;

        JobContext jobCtx = ctx.jobContext();

        try {
            OutputCommitter committer = jobCtx.getJobConf().getOutputCommitter();

            if (abort)
                committer.abortJob(jobCtx, JobStatus.State.FAILED);
            else
                committer.commitJob(jobCtx);
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}
