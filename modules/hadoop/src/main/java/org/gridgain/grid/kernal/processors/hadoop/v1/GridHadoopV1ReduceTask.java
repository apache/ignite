/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v1;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;

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

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridException {
        GridHadoopJob job = taskCtx.job();

        GridHadoopV2TaskContext ctx = (GridHadoopV2TaskContext)taskCtx;

        JobConf jobConf = ctx.jobConf();

        GridHadoopTaskInput input = taskCtx.input();

        GridHadoopV1OutputCollector collector = null;

        try {
            collector = collector(jobConf, ctx, reduce || !job.info().hasReducer(), fileName(), ctx.attemptId());

            Reducer reducer = ReflectionUtils.newInstance(reduce ? jobConf.getReducerClass() : jobConf.getCombinerClass(),
                jobConf);

            assert reducer != null;

            try {
                try {
                    while (input.next()) {
                        if (isCancelled())
                            throw new GridHadoopTaskCancelledException("Reduce task cancelled.");

                        reducer.reduce(input.key(), input.values(), collector, Reporter.NULL);
                    }
                }
                finally {
                    reducer.close();
                }
            }
            finally {
                collector.closeWriter();
            }

            collector.commit();
        }
        catch (Exception e) {
            if (collector != null)
                collector.abort();

            throw new GridException(e);
        }
    }
}
