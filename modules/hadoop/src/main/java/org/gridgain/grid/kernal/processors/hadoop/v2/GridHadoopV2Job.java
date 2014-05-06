/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.TaskType;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v1.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Hadoop job implementation for v2 API.
 */
public class GridHadoopV2Job implements GridHadoopJob {
    /** Flag is set if new context-object code is used for running the mapper. */
    private final boolean useNewMapper;

    /** Flag is set if new context-object code is used for running the reducer. */
    private final boolean useNewReducer;

    /** Flag is set if new context-object code is used for running the combiner. */
    private final boolean useNewCombiner;

    /** Hadoop job ID. */
    private GridHadoopJobId jobId;

    /** Job info. */
    protected GridHadoopDefaultJobInfo jobInfo;

    /** Hadoop native job context. */
    protected JobContextImpl ctx;

    /** Key class. */
    private Class<?> keyCls;

    /** Value class. */
    private Class<?> valCls;

    /**
     * @param jobId Job ID.
     * @param jobInfo Job info.
     */
    public GridHadoopV2Job(GridHadoopJobId jobId, GridHadoopDefaultJobInfo jobInfo) {
        this.jobId = jobId;
        this.jobInfo = jobInfo;

        JobID hadoopJobID = new JobID(jobId.globalId().toString(), jobId.localId());

        JobConf cfg = jobInfo.configuration();

        ctx = new JobContextImpl(cfg, hadoopJobID);

        useNewMapper = cfg.getUseNewMapper();
        useNewReducer = cfg.getUseNewReducer();
        useNewCombiner = cfg.getCombinerClass() == null;

        keyCls = ctx.getMapOutputKeyClass();
        valCls = ctx.getMapOutputValueClass();
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
        return GridHadoopV2Splitter.splitJob(ctx);
    }

    /** {@inheritDoc} */
    @Override public int reducers() {
        return ctx.getNumReduceTasks();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopPartitioner partitioner() throws GridException {
        Class<?> partClsOld = ctx.getConfiguration().getClass("mapred.partitioner.class", null);

        if (partClsOld != null)
            return new GridHadoopV1Partitioner(ctx.getJobConf().getPartitionerClass());

        try {
            return new GridHadoopV2Partitioner(ctx.getPartitionerClass());
        }
        catch (ClassNotFoundException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTask createTask(GridHadoopTaskInfo taskInfo) {
        boolean isAbort = taskInfo.type() == GridHadoopTaskType.ABORT;

        switch (taskInfo.type()) {
            case MAP:
                return useNewMapper ? new GridHadoopV2MapTask(taskInfo) : new GridHadoopV1MapTask(taskInfo);

            case REDUCE:
                return useNewReducer ? new GridHadoopV2ReduceTask(taskInfo) : new GridHadoopV1ReduceTask(taskInfo);

            case COMBINE:
                return useNewCombiner ? new GridHadoopV2CombineTask(taskInfo) : new GridHadoopV1CombineTask(taskInfo);

            case COMMIT:
            case ABORT:
                return useNewReducer ? new GridHadoopV2CleanupTask(taskInfo, isAbort) : new GridHadoopV1CleanupTask(taskInfo, isAbort);

            default:
                return null;
        }
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
    private Class<?> combinerClass() {
        Class<?> res = ctx.getJobConf().getCombinerClass();

        try {
            if (res == null)
                res = ctx.getCombinerClass();

            return res;
        }
        catch (ClassNotFoundException e) {
            // TODO check combiner class at initialization and throw meaningful exception.
            throw new GridRuntimeException(e);
        }
    }

    /**
     * Gets serializer for specified class.
     * @param cls Class.
     * @return Appropriate serializer.
     */
    @SuppressWarnings("unchecked")
    private GridHadoopSerialization getSerialization(Class<?> cls) {
        SerializationFactory factory = new SerializationFactory(ctx.getJobConf());

        Serialization<?> serialization = factory.getSerialization(cls);

        if (serialization.getClass() == WritableSerialization.class)
            return new GridHadoopWritableSerialization((Class<? extends Writable>)cls);

        return new GridHadoopSerializationWrapper(serialization, cls);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopSerialization keySerialization() throws GridException {
        return getSerialization(keyCls);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopSerialization valueSerialization() throws GridException {
        return getSerialization(valCls);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String property(String name) {
        return jobInfo.configuration().get(name);
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
