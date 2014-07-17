/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.GridException;

import java.util.Comparator;

/**
 * Task context.
 */
public abstract class GridHadoopTaskContext {
    /** */
    private final GridHadoopJob job;

    /** */
    private GridHadoopTaskInput input;

    /** */
    private GridHadoopTaskOutput output;

    /** */
    private final GridHadoopTaskInfo taskInfo;

    /**
     * @param taskInfo Task info.
     * @param job Job.
     */
    public GridHadoopTaskContext(GridHadoopTaskInfo taskInfo, GridHadoopJob job) {
        this.taskInfo = taskInfo;
        this.job = job;
    }

    /**
     * Gets task info.
     *
     * @return Task info.
     */
    public GridHadoopTaskInfo taskInfo() {
        return taskInfo;
    }

    /**
     * Gets task output.
     *
     * @return Task output.
     */
    public GridHadoopTaskOutput output() {
        return output;
    }

    /**
     * Gets task input.
     *
     * @return Task input.
     */
    public GridHadoopTaskInput input() {
        return input;
    }

    /**
     * @return Job.
     */
    public GridHadoopJob job() {
        return job;
    }

    public void input(GridHadoopTaskInput in) {
        input = in;
    }

    public void output(GridHadoopTaskOutput out) {
        output = out;
    }

    public abstract GridHadoopPartitioner partitioner() throws GridException;

    public abstract Comparator<?> combineGroupComparator();

    public abstract Comparator<?> reduceGroupComparator();

    public abstract GridHadoopSerialization keySerialization() throws GridException;

    public abstract GridHadoopSerialization valueSerialization() throws GridException;

    public abstract Comparator<?> sortComparator();
}
