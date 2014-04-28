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
    /** Format of number in output file name. */
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    static {
        NUMBER_FORMAT.setMinimumIntegerDigits(5);
        NUMBER_FORMAT.setGroupingUsed(false);
    }

    /** {@inheritDoc} */
    public GridHadoopV1ReduceTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridInterruptedException, GridException {
        GridHadoopV1JobImpl jobImpl = (GridHadoopV1JobImpl)taskCtx.job();

        JobContext jobCtx = jobImpl.hadoopJobContext();

        Reducer reducer = U.newInstance(jobCtx.getJobConf().getReducerClass());

        //TODO: Is there a difference how to create instance?
        //OutputFormat outFormat = U.newInstance(jobCtx.getJobConf().getClass("mapred.output.format.class", TextOutputFormat.class,
        //        OutputFormat.class));
        OutputFormat outFormat = jobCtx.getJobConf().getOutputFormat();

        Reporter reporter = Reporter.NULL;

        String fileName = "part-" + NUMBER_FORMAT.format(info().taskNumber());

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
            commiter.commitJob(jobCtx);
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}
