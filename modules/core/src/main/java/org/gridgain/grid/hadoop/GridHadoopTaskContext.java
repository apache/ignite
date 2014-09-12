/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.*;

import java.util.*;

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
    private GridHadoopTaskInfo taskInfo;

    /** */
    private GridHadoopCounters counters;

    /**
     * @param taskInfo Task info.
     * @param job Job.
     */
    protected GridHadoopTaskContext(GridHadoopTaskInfo taskInfo, GridHadoopJob job) {
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
     * Set a new task info.
     *
     * @param info Task info.
     */
    public void taskInfo(GridHadoopTaskInfo info) {
        taskInfo = info;
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

    /**
     * Gets counter for the given name.
     *
     * @param grp Counter group's name.
     * @param name Counter name.
     * @return Counter.
     */
    public GridHadoopCounter counter(String grp, String name) {
        return counters.counter(grp, name, true);
    }

    /**
     * Gets all known counters.
     *
     * @return Unmodifiable collection of counters.
     */
    public Collection<GridHadoopCounter> counters() {
        return counters.all();
    }

    /**
     * Sets input of the task.
     *
     * @param in Input.
     */
    public void input(GridHadoopTaskInput in) {
        input = in;
    }

    /**
     * Sets output of the task.
     *
     * @param out Output.
     */
    public void output(GridHadoopTaskOutput out) {
        output = out;
    }

    /**
     * Sets container for counters of the task.
     *
     * @param counters Counters.
     */
    public void counters(GridHadoopCounters counters) {
        this.counters = counters;
    }

    /**
     * Gets partitioner.
     *
     * @return Partitioner.
     * @throws GridException If failed.
     */
    public abstract GridHadoopPartitioner partitioner() throws GridException;

    /**
     * Gets serializer for values.
     *
     * @return Serializer for keys.
     * @throws GridException If failed.
     */
    public abstract GridHadoopSerialization keySerialization() throws GridException;

    /**
     * Gets serializer for values.
     *
     * @return Serializer for values.
     * @throws GridException If failed.
     */
    public abstract GridHadoopSerialization valueSerialization() throws GridException;

    /**
     * Gets sorting comparator.
     *
     * @return Comparator for sorting.
     */
    public abstract Comparator<Object> sortComparator();

    /**
     * Gets comparator for grouping on combine or reduce operation.
     *
     * @return Comparator.
     */
    public abstract Comparator<Object> groupComparator();

    /**
     * Execute current task.
     *
     * @throws GridException If failed.
     */
    public abstract void run() throws GridException;

    /**
     * Cancel current task execution.
     */
    public abstract void cancel();
}
