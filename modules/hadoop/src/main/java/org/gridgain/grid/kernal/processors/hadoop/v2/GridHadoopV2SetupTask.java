/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapreduce.*;
import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;

import java.io.*;

/**
 * Hadoop setup task (prepares job).
 */
public class GridHadoopV2SetupTask extends GridHadoopV2Task {
    /**
     * Constructor.
     *
     * @param taskInfo task info.
     */
    public GridHadoopV2SetupTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override protected void run0(GridHadoopV2TaskContext taskCtx) throws IgniteCheckedException {
        try {
            JobContextImpl jobCtx = taskCtx.jobContext();

            OutputFormat outputFormat = getOutputFormat(jobCtx);

            outputFormat.checkOutputSpecs(jobCtx);

            OutputCommitter committer = outputFormat.getOutputCommitter(hadoopContext());

            if (committer != null)
                committer.setupJob(jobCtx);
        }
        catch (ClassNotFoundException | IOException e) {
            throw new IgniteCheckedException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException(e);
        }
    }
}
