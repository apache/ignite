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
    @Override protected void run0(GridHadoopV2Job job, JobContext jobCtx, GridHadoopTaskContext taskCtx)
        throws GridException {
        try {
            OutputFormat outputFormat = U.newInstance(jobCtx.getOutputFormatClass());

            outputFormat.checkOutputSpecs(jobCtx);

            OutputCommitter committer = outputFormat.getOutputCommitter(hadoopContext());

            committer.setupJob(jobCtx);
        }
        catch (ClassNotFoundException | IOException e) {
            throw new GridException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException(e);
        }
    }
}
