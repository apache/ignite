/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop1impl;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.TaskType;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * Hadoop job implementation for v1 API.
 */
public class GridHadoopV1JobImpl implements GridHadoopJob {
    /** Job ID. */
    private GridHadoopJobId jobId;

    /** Job info. */
    protected GridHadoopDefaultJobInfo jobInfo;

    /** Hadoop job context. */
    private JobContext ctx;

    /**
     * @param jobId Job ID.
     * @param jobInfo Job info.
     */
    public GridHadoopV1JobImpl(GridHadoopJobId jobId, GridHadoopDefaultJobInfo jobInfo) {
        this.jobId = jobId;
        this.jobInfo = jobInfo;

        ctx = new JobContextImpl((JobConf)jobInfo.configuration(), new JobID(jobId.globalId().toString(), jobId.localId()));
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobId id() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopJobInfo info() {
        return jobInfo;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridHadoopFileBlock> input() throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public int reducers() {
        return ctx.getNumReduceTasks();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopPartitioner partitioner() throws GridException {
        return null;
    }

    @Override
    public GridHadoopSerialization keySerialization() throws GridException {
        return null;
    }

    @Override public GridHadoopSerialization valueSerialization() throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTask createTask(GridHadoopTaskInfo taskInfo) {
        switch (taskInfo.type()) {
            case MAP:
                return new GridHadoopV1MapTask(taskInfo);

            case REDUCE:
                return new GridHadoopV1ReduceTask(taskInfo);

            case COMBINE:
                return new GridHadoopV1CombineTask(taskInfo);

            case COMMIT:
                return new GridHadoopV1CleanupTask(taskInfo, false);

            case ABORT:
                return new GridHadoopV1CleanupTask(taskInfo, true);
            default:
                return null;
        }
    }

    @Nullable @Override public String property(String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasCombiner() {
        return combinerClass() != null;
    }

    /**
     * Gets combiner class.
     *
     * @return Combiner class or {@code null} if combiner is not specified.
     */
    private Class<? extends Reducer> combinerClass() {
        return ctx.getJobConf().getCombinerClass();
    }

    /**
     * @param type Task type.
     * @return Hadoop task type.
     */
    private TaskType taskType(GridHadoopTaskType type) {
        switch (type) {
            case MAP:
                return TaskType.MAP;

            case REDUCE:
                return TaskType.REDUCE;

            case COMMIT:
            case ABORT:
                return TaskType.JOB_CLEANUP;

            default:
                return null;
        }
    }

    /**
     * Creates Hadoop attempt ID.
     *
     * @param taskInfo Task info.
     * @return Attempt ID.
     */
    public TaskAttemptID attemptId(GridHadoopTaskInfo taskInfo) {
        TaskID tid = new TaskID(ctx.getJobID(), taskType(taskInfo.type()), taskInfo.taskNumber());

        return new TaskAttemptID(tid, taskInfo.attempt());
    }

    /** Hadoop native job context. */
    public JobContext hadoopJobContext() {
        return ctx;
    }
}
