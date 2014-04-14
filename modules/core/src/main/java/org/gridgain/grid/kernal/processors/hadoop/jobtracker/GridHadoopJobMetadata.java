/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.hadoop.*;

import java.io.*;

/**
 * Hadoop job metadata. Internal object used for distributed job state tracking.
 */
public class GridHadoopJobMetadata<T> implements Serializable {
    /** Job ID. */
    private GridHadoopJobId jobId;

    /** Job info. */
    private GridHadoopJobInfo<T> jobInfo;

    /** Map-reduce plan. */
    private GridHadoopMapReducePlan mrPlan;

    /**
     * @param jobId Job ID.
     * @param jobInfo Job info.
     */
    public GridHadoopJobMetadata(GridHadoopJobId jobId, GridHadoopJobInfo<T> jobInfo) {
        this.jobId = jobId;
        this.jobInfo = jobInfo;
    }

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @param mrPlan Map-reduce plan.
     */
    public void mapReducePlan(GridHadoopMapReducePlan mrPlan) {
        this.mrPlan = mrPlan;
    }

    /**
     * @return Map-reduce plan.
     */
    public GridHadoopMapReducePlan mapReducePlan() {
        return mrPlan;
    }

    /**
     * @return Job info.
     */
    public GridHadoopJobInfo<T> jobInfo() {
        return jobInfo;
    }
}
