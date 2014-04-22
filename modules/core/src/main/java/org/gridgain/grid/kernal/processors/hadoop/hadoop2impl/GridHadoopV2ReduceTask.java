/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop2impl;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.IOException;

/**
 * Hadoop reduce task implementation for v2 API.
 */
public class GridHadoopV2ReduceTask extends GridHadoopV2TaskImpl {
    /**
     * @param taskInfo Task info.
     */
    public GridHadoopV2ReduceTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridInterruptedException, GridException {
        GridHadoopV2JobImpl jobImpl = (GridHadoopV2JobImpl)taskCtx.job();

        JobContext jobCtx = jobImpl.ctx;

        Reducer reducer;
        OutputFormat outputFormat;

        try {
            reducer = U.newInstance(jobCtx.getReducerClass());
            outputFormat = U.newInstance(jobCtx.getOutputFormatClass());
        }
        catch (ClassNotFoundException e) {
            throw new GridException(e);
        }

        GridHadoopV2Context hadoopCtx = new GridHadoopV2Context(jobCtx.getConfiguration(), taskCtx, jobImpl.attemptId(info()));

        try {
            RecordWriter writer = outputFormat.getRecordWriter(hadoopCtx);
            hadoopCtx.writer(writer);

            reducer.run(new WrappedReducer().getReducerContext(hadoopCtx));

            writer.close(hadoopCtx);

            OutputCommitter outputCommitter = outputFormat.getOutputCommitter(hadoopCtx);

            outputCommitter.commitTask(hadoopCtx);
            outputCommitter.commitJob(hadoopCtx);
        }
        catch (IOException e) {
            throw new GridException(e);
        }
        catch (InterruptedException e) {
            throw new GridInterruptedException(e);
        }
    }
}
