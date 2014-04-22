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

/**
 * Hadoop combine task implementation for v2 API.
 */
public class GridHadoopV2CombineTask extends GridHadoopV2TaskImpl {
    /**
     * @param taskInfo Task info.
     */
    public GridHadoopV2CombineTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @Override public void run(GridHadoopTaskContext taskCtx) throws GridInterruptedException, GridException {
        GridHadoopV2JobImpl jobImpl = (GridHadoopV2JobImpl)taskCtx.job();

        JobContext jobCtx = jobImpl.ctx;

        try {
            Reducer combiner = U.newInstance(jobCtx.getCombinerClass());
            ReduceContext hadoopCtx = new GridHadoopV2Context(jobCtx.getConfiguration(), taskCtx, jobImpl.attemptId(info()));
            combiner.run(new WrappedReducer().getReducerContext(hadoopCtx));
        }
        catch (InterruptedException e) {
            throw new GridInterruptedException(e);
        }
        catch (Exception e) {
            throw new GridException(e);
        }
    }
}
