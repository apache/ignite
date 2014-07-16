package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.mapred.*;
import org.gridgain.grid.hadoop.*;

public class GridHadoopV2TaskContext extends GridHadoopTaskContext {
    /** */
    private JobContextImpl jobContext;

    /**
     * @param taskInfo Task info.
     * @param job      Job.
     * @param input    Input.
     * @param output   Output.
     */
    public GridHadoopV2TaskContext(GridHadoopTaskInfo taskInfo, GridHadoopJob job, GridHadoopTaskInput input,
        GridHadoopTaskOutput output, JobContextImpl jobContext) {
        super(taskInfo, job, input, output);
        this.jobContext = jobContext;
    }

    public JobConf jobConf() {
        return jobContext.getJobConf();
    }

    public JobContextImpl jobContext() {
        return jobContext;
    }
}
