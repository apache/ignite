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
    /** {@code True} if reduce, {@code false} if combine. */
    private final boolean reduce;

    /**
     * Constructor.
     *
     * @param taskInfo Task info.
     * @param reduce {@code True} if reduce, {@code false} if combine.
     */
    public GridHadoopV2ReduceTask(GridHadoopTaskInfo taskInfo, boolean reduce) {
        super(taskInfo);

        this.reduce = reduce;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    @Override public void run0(GridHadoopV2Job jobImpl, JobContext jobCtx, GridHadoopTaskContext taskCtx)
        throws GridException {
        try {
            Reducer reducer = U.newInstance(reduce ? jobCtx.getReducerClass() : jobCtx.getCombinerClass());

            OutputFormat outputFormat = reduce || !jobImpl.hasReducer() ? prepareWriter(jobCtx) : null;

            try {
                reducer.run(new WrappedReducer().getReducerContext(hadoopContext()));
            }
            finally {
                closeWriter();
            }

            commit(outputFormat);
        }
        catch (ClassNotFoundException | IOException e) {
            throw new GridException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException(e);
        }
    }
}
