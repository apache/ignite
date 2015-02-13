/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.v2;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.*;
import org.apache.ignite.*;
import org.apache.ignite.internal.processors.hadoop.*;
import org.apache.ignite.internal.processors.hadoop.counter.*;
import org.apache.ignite.internal.processors.hadoop.fs.*;
import org.apache.ignite.internal.processors.hadoop.v1.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.ignitefs.hadoop.IgfsHadoopParameters.*;
import static org.apache.ignite.internal.processors.hadoop.GridHadoopUtils.*;

/**
 * Context for task execution.
 */
public class GridHadoopV2TaskContext extends GridHadoopTaskContext {
    /** */
    private static final boolean COMBINE_KEY_GROUPING_SUPPORTED;

    /**
     * Check for combiner grouping support (available since Hadoop 2.3).
     */
    static {
        boolean ok;

        try {
            JobContext.class.getDeclaredMethod("getCombinerKeyGroupingComparator");

            ok = true;
        }
        catch (NoSuchMethodException ignore) {
            ok = false;
        }

        COMBINE_KEY_GROUPING_SUPPORTED = ok;
    }

    /** Flag is set if new context-object code is used for running the mapper. */
    private final boolean useNewMapper;

    /** Flag is set if new context-object code is used for running the reducer. */
    private final boolean useNewReducer;

    /** Flag is set if new context-object code is used for running the combiner. */
    private final boolean useNewCombiner;

    /** */
    private final JobContextImpl jobCtx;

    /** Set if task is to cancelling. */
    private volatile boolean cancelled;

    /** Current task. */
    private volatile GridHadoopTask task;

    /** Local node ID */
    private UUID locNodeId;

    /** Counters for task. */
    private final GridHadoopCounters cntrs = new GridHadoopCountersImpl();

    /**
     * @param taskInfo Task info.
     * @param job Job.
     * @param jobId Job ID.
     * @param locNodeId Local node ID.
     * @param jobConfDataInput DataInput for read JobConf.
     */
    public GridHadoopV2TaskContext(GridHadoopTaskInfo taskInfo, GridHadoopJob job, GridHadoopJobId jobId,
        @Nullable UUID locNodeId, DataInput jobConfDataInput) throws IgniteCheckedException {
        super(taskInfo, job);
        this.locNodeId = locNodeId;

        // Before create JobConf instance we should set new context class loader.
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        try {
            JobConf jobConf = new JobConf();

            try {
                jobConf.readFields(jobConfDataInput);
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }

            // For map-reduce jobs prefer local writes.
            jobConf.setBooleanIfUnset(PARAM_GGFS_PREFER_LOCAL_WRITES, true);

            jobCtx = new JobContextImpl(jobConf, new JobID(jobId.globalId().toString(), jobId.localId()));

            useNewMapper = jobConf.getUseNewMapper();
            useNewReducer = jobConf.getUseNewReducer();
            useNewCombiner = jobConf.getCombinerClass() == null;
        }
        finally {
            Thread.currentThread().setContextClassLoader(null);
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends GridHadoopCounter> T counter(String grp, String name, Class<T> cls) {
        return cntrs.counter(grp, name, cls);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopCounters counters() {
        return cntrs;
    }

    /**
     * Creates appropriate task from current task info.
     *
     * @return Task.
     */
    private GridHadoopTask createTask() {
        boolean isAbort = taskInfo().type() == GridHadoopTaskType.ABORT;

        switch (taskInfo().type()) {
            case SETUP:
                return useNewMapper ? new GridHadoopV2SetupTask(taskInfo()) : new GridHadoopV1SetupTask(taskInfo());

            case MAP:
                return useNewMapper ? new GridHadoopV2MapTask(taskInfo()) : new GridHadoopV1MapTask(taskInfo());

            case REDUCE:
                return useNewReducer ? new GridHadoopV2ReduceTask(taskInfo(), true) :
                    new GridHadoopV1ReduceTask(taskInfo(), true);

            case COMBINE:
                return useNewCombiner ? new GridHadoopV2ReduceTask(taskInfo(), false) :
                    new GridHadoopV1ReduceTask(taskInfo(), false);

            case COMMIT:
            case ABORT:
                return useNewReducer ? new GridHadoopV2CleanupTask(taskInfo(), isAbort) :
                    new GridHadoopV1CleanupTask(taskInfo(), isAbort);

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void run() throws IgniteCheckedException {
        try {
            Thread.currentThread().setContextClassLoader(jobConf().getClassLoader());

            try {
                task = createTask();
            }
            catch (Throwable e) {
                throw transformException(e);
            }

            if (cancelled)
                throw new GridHadoopTaskCancelledException("Task cancelled.");

            try {
                task.run(this);
            }
            catch (Throwable e) {
                throw transformException(e);
            }
        }
        finally {
            task = null;

            Thread.currentThread().setContextClassLoader(null);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        cancelled = true;

        GridHadoopTask t = task;

        if (t != null)
            t.cancel();
    }

    /** {@inheritDoc} */
    @Override public void prepareTaskEnvironment() throws IgniteCheckedException {
        File locDir;

        switch(taskInfo().type()) {
            case MAP:
            case REDUCE:
                job().prepareTaskEnvironment(taskInfo());

                locDir = taskLocalDir(locNodeId, taskInfo());

                break;

            default:
                locDir = jobLocalDir(locNodeId, taskInfo().jobId());
        }

        Thread.currentThread().setContextClassLoader(jobConf().getClassLoader());

        try {
            FileSystem fs = FileSystem.get(jobConf());

            GridHadoopFileSystemsUtils.setUser(fs, jobConf().getUser());

            LocalFileSystem locFs = FileSystem.getLocal(jobConf());

            locFs.setWorkingDirectory(new Path(locDir.getAbsolutePath()));
        }
        catch (Throwable e) {
            throw transformException(e);
        }
        finally {
            Thread.currentThread().setContextClassLoader(null);
        }
    }

    /** {@inheritDoc} */
    @Override public void cleanupTaskEnvironment() throws IgniteCheckedException {
        job().cleanupTaskEnvironment(taskInfo());
    }

    /**
     * Creates Hadoop attempt ID.
     *
     * @return Attempt ID.
     */
    public TaskAttemptID attemptId() {
        TaskID tid = new TaskID(jobCtx.getJobID(), taskType(taskInfo().type()), taskInfo().taskNumber());

        return new TaskAttemptID(tid, taskInfo().attempt());
    }

    /**
     * @param type Task type.
     * @return Hadoop task type.
     */
    private TaskType taskType(GridHadoopTaskType type) {
        switch (type) {
            case SETUP:
                return TaskType.JOB_SETUP;
            case MAP:
            case COMBINE:
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
     * Gets job configuration of the task.
     *
     * @return Job configuration.
     */
    public JobConf jobConf() {
        return jobCtx.getJobConf();
    }

    /**
     * Gets job context of the task.
     *
     * @return Job context.
     */
    public JobContextImpl jobContext() {
        return jobCtx;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopPartitioner partitioner() throws IgniteCheckedException {
        Class<?> partClsOld = jobConf().getClass("mapred.partitioner.class", null);

        if (partClsOld != null)
            return new GridHadoopV1Partitioner(jobConf().getPartitionerClass(), jobConf());

        try {
            return new GridHadoopV2Partitioner(jobCtx.getPartitionerClass(), jobConf());
        }
        catch (ClassNotFoundException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Gets serializer for specified class.
     *
     * @param cls Class.
     * @param jobConf Job configuration.
     * @return Appropriate serializer.
     */
    @SuppressWarnings("unchecked")
    private GridHadoopSerialization getSerialization(Class<?> cls, Configuration jobConf) throws IgniteCheckedException {
        A.notNull(cls, "cls");

        SerializationFactory factory = new SerializationFactory(jobConf);

        Serialization<?> serialization = factory.getSerialization(cls);

        if (serialization == null)
            throw new IgniteCheckedException("Failed to find serialization for: " + cls.getName());

        if (serialization.getClass() == WritableSerialization.class)
            return new GridHadoopWritableSerialization((Class<? extends Writable>)cls);

        return new GridHadoopSerializationWrapper(serialization, cls);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopSerialization keySerialization() throws IgniteCheckedException {
        return getSerialization(jobCtx.getMapOutputKeyClass(), jobConf());
    }

    /** {@inheritDoc} */
    @Override public GridHadoopSerialization valueSerialization() throws IgniteCheckedException {
        return getSerialization(jobCtx.getMapOutputValueClass(), jobConf());
    }

    /** {@inheritDoc} */
    @Override public Comparator<Object> sortComparator() {
        return (Comparator<Object>)jobCtx.getSortComparator();
    }

    /** {@inheritDoc} */
    @Override public Comparator<Object> groupComparator() {
        Comparator<?> res;

        switch (taskInfo().type()) {
            case COMBINE:
                res = COMBINE_KEY_GROUPING_SUPPORTED ?
                    jobContext().getCombinerKeyGroupingComparator() : jobContext().getGroupingComparator();

                break;

            case REDUCE:
                res = jobContext().getGroupingComparator();

                break;

            default:
                return null;
        }

        if (res != null && res.getClass() != sortComparator().getClass())
            return (Comparator<Object>)res;

        return null;
    }

    /**
     * @param split Split.
     * @return Native Hadoop split.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("unchecked")
    public Object getNativeSplit(GridHadoopInputSplit split) throws IgniteCheckedException {
        if (split instanceof GridHadoopExternalSplit)
            return readExternalSplit((GridHadoopExternalSplit)split);

        if (split instanceof GridHadoopSplitWrapper)
            return unwrapSplit((GridHadoopSplitWrapper)split);

        throw new IllegalStateException("Unknown split: " + split);
    }

    /**
     * @param split External split.
     * @return Native input split.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private Object readExternalSplit(GridHadoopExternalSplit split) throws IgniteCheckedException {
        Path jobDir = new Path(jobConf().get(MRJobConfig.MAPREDUCE_JOB_DIR));

        try (FileSystem fs = FileSystem.get(jobDir.toUri(), jobConf());
            FSDataInputStream in = fs.open(JobSubmissionFiles.getJobSplitFile(jobDir))) {

            in.seek(split.offset());

            String clsName = Text.readString(in);

            Class<?> cls = jobConf().getClassByName(clsName);

            assert cls != null;

            Serialization serialization = new SerializationFactory(jobConf()).getSerialization(cls);

            Deserializer deserializer = serialization.getDeserializer(cls);

            deserializer.open(in);

            Object res = deserializer.deserialize(null);

            deserializer.close();

            assert res != null;

            return res;
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IgniteCheckedException(e);
        }
    }
}
