/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Hadoop reduce task implementation for v2 API.
 */
public class GridHadoopV2ReduceTask extends GridHadoopV2Task {
    /**
     * @param taskInfo Task info.
     */
    public GridHadoopV2ReduceTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridException {
        GridHadoopV2Job jobImpl = (GridHadoopV2Job)taskCtx.job();

        JobContext jobCtx = jobImpl.hadoopJobContext();

        Reducer reducer;

        try {
            reducer = U.newInstance(jobCtx.getReducerClass());
        }
        catch (ClassNotFoundException e) {
            throw new GridException(e);
        }

        GridHadoopV2Context hadoopCtx = new GridHadoopV2Context(jobCtx.getConfiguration(), taskCtx,
            jobImpl.attemptId(info()));

        try {
            OutputFormat outputFormat = putWriter(hadoopCtx, jobCtx);

            try {
                reducer.run(new WrappedReducer().getReducerContext(hadoopCtx));
            }
            finally {
                commit(hadoopCtx, outputFormat);
            }
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
