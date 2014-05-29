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
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.IOException;

/**
 * Hadoop reduce task implementation for v1 API.
 */
public class GridHadoopV1ReduceTask extends GridHadoopV1Task {
    /** {@code True} if reduce, {@code false} if combine. */
    private final boolean reduce;

    /**
     * Constructor.
     *
     * @param taskInfo Task info.
     * @param reduce {@code True} if reduce, {@code false} if combine.
     */
    public GridHadoopV1ReduceTask(GridHadoopTaskInfo taskInfo, boolean reduce) {
        super(taskInfo);

        this.reduce = reduce;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void run(GridHadoopTaskContext taskCtx) throws GridException {
        GridHadoopV2Job jobImpl = (GridHadoopV2Job) taskCtx.job();

        JobConf jobConf = new JobConf(jobImpl.hadoopJobContext().getJobConf());

        Reducer reducer = U.newInstance(reduce ? jobConf.getReducerClass() : jobConf.getCombinerClass());

        assert reducer != null;

        reducer.configure(jobConf);

        GridHadoopTaskInput input = taskCtx.input();

        try {
            GridHadoopOutputCollector collector = new GridHadoopOutputCollector(jobConf, taskCtx,
                    reduce || !jobImpl.hasReducer(), fileName(), jobImpl.attemptId(info()));

            try {
                while (input.next()) {
                    if (isCancelled())
                        throw new GridHadoopTaskCancelledException(null);

                    reducer.reduce(input.key(), input.values(), collector, Reporter.NULL);
                }
            } finally {
                U.closeQuiet(reducer);

                collector.closeWriter();
            }

            collector.commit();
        } catch (IOException e) {
            throw new GridException(e);
        }
    }
}
