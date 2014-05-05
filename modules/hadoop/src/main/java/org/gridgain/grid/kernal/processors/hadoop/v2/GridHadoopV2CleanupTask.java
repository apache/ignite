/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Hadoop cleanup task (commits or aborts job).
 */
public class GridHadoopV2CleanupTask extends GridHadoopTask {
    /** Abort flag. */
    private boolean abort;

    /**
     * @param taskInfo Task info.
     * @param abort Abort flag.
     */
    public GridHadoopV2CleanupTask(GridHadoopTaskInfo taskInfo, boolean abort) {
        super(taskInfo);

        this.abort = abort;
    }

    /** {@inheritDoc} */
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridInterruptedException, GridException {
        GridHadoopV2Job jobImpl = (GridHadoopV2Job)taskCtx.job();

        JobContext jobCtx = jobImpl.hadoopJobContext();

        OutputFormat outputFormat;

        try {
            outputFormat = U.newInstance(jobCtx.getOutputFormatClass());
        }
        catch (ClassNotFoundException e) {
            throw new GridException(e);
        }

        TaskAttemptContext hCtx = new GridHadoopV2Context(jobCtx.getConfiguration(), taskCtx,
            jobImpl.attemptId(info()));

        try {
            OutputCommitter committer = outputFormat.getOutputCommitter(hCtx);

            if (abort)
                // TODO how to get getUseNewMapper?
                committer.abortJob(jobCtx, JobStatus.State.FAILED);
            else
                committer.commitJob(jobCtx);
        }
        catch (IOException e) {
            throw new GridException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException(e);
        }
    }
}
