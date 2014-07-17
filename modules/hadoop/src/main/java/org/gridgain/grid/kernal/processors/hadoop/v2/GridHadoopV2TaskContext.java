package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.mapred.*;
import org.gridgain.grid.GridException;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v1.GridHadoopV1Partitioner;

public class GridHadoopV2TaskContext extends GridHadoopTaskContext {
    /** */
    private JobContextImpl jobCtx;

    /**
     * @param taskInfo Task info.
     * @param job      Job.
     */
    public GridHadoopV2TaskContext(GridHadoopTaskInfo taskInfo, GridHadoopJob job,
                                   JobContextImpl jobCtx) {
        super(taskInfo, job);
        this.jobCtx = jobCtx;
    }

    public JobConf jobConf() {
        return jobCtx.getJobConf();
    }

    public JobContextImpl jobContext() {
        return jobCtx;
    }

    @Override public GridHadoopPartitioner partitioner() throws GridException {
        Class<?> partClsOld = jobConf().getClass("mapred.partitioner.class", null);

        if (partClsOld != null)
            return new GridHadoopV1Partitioner(jobConf().getPartitionerClass(), jobConf());

        try {
            return new GridHadoopV2Partitioner(jobCtx.getPartitionerClass(), jobConf());
        }
        catch (ClassNotFoundException e) {
            throw new GridException(e);
        }

    }
}
