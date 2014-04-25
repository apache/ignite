/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop1impl;

import org.apache.hadoop.mapred.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.IOException;

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
        //TODO: Is there a difference how to create instance?
        //OutputFormat outFormat = U.newInstance(jobCtx.getJobConf().getClass("mapred.output.format.class", TextOutputFormat.class,
        //        OutputFormat.class));

        Reporter reporter = Reporter.NULL;

        OutputFormat outFormat = jobCtx.getJobConf().getOutputFormat();

        String name = "";

        reducer.configure(jobCtx.getJobConf());
        GridHadoopTaskInput input = taskCtx.input();
        try {
            final RecordWriter writer = outFormat.getRecordWriter(null, jobCtx.getJobConf(), name, reporter);

            OutputCollector collector = new OutputCollector() {
                @Override public void collect(Object key, Object val) throws IOException {
                    writer.write(key, val);
                }
            };

            while (input.next()) {
                reducer.reduce(input.key(), input.values(), collector, reporter);
            }
            reducer.close();
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}
