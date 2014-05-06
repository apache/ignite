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
import org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2Job;
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
        GridHadoopV2Job jobImpl = (GridHadoopV2Job) taskCtx.job();

        JobConf jobConf = new JobConf(jobImpl.hadoopJobContext().getJobConf());

        Reducer reducer = U.newInstance(jobConf.getReducerClass());

        OutputFormat outFormat = jobConf.getOutputFormat();

        Reporter reporter = Reporter.NULL;

        NumberFormat numFormat = NumberFormat.getInstance();

        numFormat.setMinimumIntegerDigits(5);
        numFormat.setGroupingUsed(false);

        String fileName = "part-" + numFormat.format(info().taskNumber());

        reducer.configure(jobConf);

        GridHadoopTaskInput input = taskCtx.input();

        TaskAttemptID attempt = jobImpl.attemptId(info());

        jobConf.set("mapreduce.task.attempt.id", attempt.toString());

        try {
            final RecordWriter writer = outFormat.getRecordWriter(null, jobConf, fileName, reporter);

            OutputCollector collector = new OutputCollector() {
                @Override public void collect(Object key, Object val) throws IOException {
                    writer.write(key, val);
                }
            };

            try {
                while (input.next())
                    reducer.reduce(input.key(), input.values(), collector, reporter);

                reducer.close();
            }
            finally {
                writer.close(reporter);
            }

            OutputCommitter commiter = jobConf.getOutputCommitter();

            commiter.commitTask(new TaskAttemptContextImpl(jobConf, attempt));
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}
