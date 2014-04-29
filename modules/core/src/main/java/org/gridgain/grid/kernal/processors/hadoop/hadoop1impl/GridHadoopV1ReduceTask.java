/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop1impl;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.IOException;
import java.text.NumberFormat;

/**
 * Hadoop reduce task implementation for v1 API.
 */
public class GridHadoopV1ReduceTask extends GridHadoopTask {
    /** {@inheritDoc} */
    public GridHadoopV1ReduceTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridInterruptedException, GridException {
        GridHadoopV1JobImpl jobImpl = (GridHadoopV1JobImpl)taskCtx.job();

        JobContext jobCtx = jobImpl.hadoopJobContext();

        Reducer reducer = U.newInstance(jobCtx.getJobConf().getReducerClass());

        OutputFormat outFormat = jobCtx.getJobConf().getOutputFormat();

        Reporter reporter = Reporter.NULL;

        NumberFormat numberFormat = NumberFormat.getInstance();

        numberFormat.setMinimumIntegerDigits(5);
        numberFormat.setGroupingUsed(false);

        String fileName = "part-" + numberFormat.format(info().taskNumber());

        reducer.configure(jobCtx.getJobConf());

        GridHadoopTaskInput input = taskCtx.input();

        TaskAttemptID attempt = jobImpl.attemptId(info());

        jobCtx.getJobConf().set("mapreduce.task.attempt.id", attempt.toString());

        try {
            final RecordWriter writer = outFormat.getRecordWriter(null, jobCtx.getJobConf(), fileName, reporter);

            OutputCollector collector = new OutputCollector() {
                @Override public void collect(Object key, Object val) throws IOException {
                    writer.write(key, val);
                }
            };

            try {
                while (input.next()) {
                    reducer.reduce(input.key(), input.values(), collector, reporter);
                }

                reducer.close();
            }
            finally {
                writer.close(reporter);
            }

            OutputCommitter commiter = jobCtx.getJobConf().getOutputCommitter();

            commiter.commitTask(new TaskAttemptContextImpl(jobCtx.getJobConf(), attempt));
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}
