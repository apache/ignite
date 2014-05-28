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
 * Hadoop combine task implementation for v1 API.
 */
public class GridHadoopV1CombineTask extends GridHadoopV1Task {
    /**
     * @param taskInfo Task info.
     */
    public GridHadoopV1CombineTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void run(final GridHadoopTaskContext taskCtx) throws GridException {
        GridHadoopV2Job jobImpl = (GridHadoopV2Job) taskCtx.job();

        JobConf jobConf = new JobConf(jobImpl.hadoopJobContext().getJobConf());

        Reducer combiner = U.newInstance(jobConf.getCombinerClass());

        assert combiner != null;

        combiner.configure(jobConf);

        GridHadoopTaskInput input = taskCtx.input();

        Reporter reporter = Reporter.NULL;

        try {
            GridHadoopOutputCollector collector = new GridHadoopOutputCollector(jobConf, taskCtx,
                !jobImpl.hasReducer(), fileName(), jobImpl.attemptId(info()));

            try {
                while (input.next())
                    combiner.reduce(input.key(), input.values(), collector, reporter);
            }
            finally {
                U.closeQuiet(combiner);

                collector.closeWriter();
            }

            collector.commit();
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}
