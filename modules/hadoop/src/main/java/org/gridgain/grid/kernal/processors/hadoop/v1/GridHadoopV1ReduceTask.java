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
    @SuppressWarnings("unchecked")
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridException {
        GridHadoopV2Job jobImpl = (GridHadoopV2Job) taskCtx.job();

        JobConf jobConf = new JobConf(jobImpl.hadoopJobContext().getJobConf());

        Reducer reducer = U.newInstance(jobConf.getReducerClass());

        Reporter reporter = Reporter.NULL;

        assert reducer != null;

        reducer.configure(jobConf);

        GridHadoopTaskInput input = taskCtx.input();

        try {
            GridHadoopOutputCollector collector = new GridHadoopOutputCollector(jobConf, taskCtx, true, fileName(),
                jobImpl.attemptId(info()));

            try {
                while (input.next())
                    reducer.reduce(input.key(), input.values(), collector, reporter);

                reducer.close();
            }
            finally {
                collector.close();
            }
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}
