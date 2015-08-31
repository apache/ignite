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

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopClassLoader;
import org.apache.ignite.internal.processors.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopPartitioner;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.processors.hadoop.HadoopTask;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskCancelledException;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskType;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounter;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCountersImpl;
import org.apache.ignite.internal.processors.hadoop.fs.HadoopLazyConcurrentMap;
import org.apache.ignite.internal.processors.hadoop.v1.HadoopV1CleanupTask;
import org.apache.ignite.internal.processors.hadoop.v1.HadoopV1MapTask;
import org.apache.ignite.internal.processors.hadoop.v1.HadoopV1Partitioner;
import org.apache.ignite.internal.processors.hadoop.v1.HadoopV1ReduceTask;
import org.apache.ignite.internal.processors.hadoop.v1.HadoopV1SetupTask;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.jobLocalDir;
import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.taskLocalDir;
import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.transformException;
import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.unwrapSplit;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopFileSystemCacheUtils.FsCacheKey;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopFileSystemCacheUtils.createHadoopLazyConcurrentMap;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopFileSystemCacheUtils.fileSystemForMrUserWithCaching;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopParameters.PARAM_IGFS_PREFER_LOCAL_WRITES;

/**
 * Context for task execution.
 */
public class HadoopV2TaskContext extends HadoopTaskContext {
    /** */
    private static final boolean COMBINE_KEY_GROUPING_SUPPORTED;

    /** Lazy per-user file system cache used by the Hadoop task. */
    private static final HadoopLazyConcurrentMap<FsCacheKey, FileSystem> fsMap
        = createHadoopLazyConcurrentMap();

    /**
     * This method is called with reflection upon Job finish with class loader of each task.
     * This will clean up all the Fs created for specific task.
     * Each class loader sees uses its own instance of <code>fsMap<code/> since the class loaders
     * are different.
     *
     * @throws IgniteCheckedException On error.
     */
    public static void close() throws IgniteCheckedException {
        fsMap.close();
    }

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
    private volatile HadoopTask task;

    /** Local node ID */
    private final UUID locNodeId;

    /** Counters for task. */
    private final HadoopCounters cntrs = new HadoopCountersImpl();

    /**
     * @param taskInfo Task info.
     * @param job Job.
     * @param jobId Job ID.
     * @param locNodeId Local node ID.
     * @param jobConfDataInput DataInput for read JobConf.
     */
    public HadoopV2TaskContext(HadoopTaskInfo taskInfo, HadoopJob job, HadoopJobId jobId,
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
            jobConf.setBooleanIfUnset(PARAM_IGFS_PREFER_LOCAL_WRITES, true);

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
    @Override public <T extends HadoopCounter> T counter(String grp, String name, Class<T> cls) {
        return cntrs.counter(grp, name, cls);
    }

    /** {@inheritDoc} */
    @Override public HadoopCounters counters() {
        return cntrs;
    }

    /**
     * Creates appropriate task from current task info.
     *
     * @return Task.
     */
    private HadoopTask createTask() {
        boolean isAbort = taskInfo().type() == HadoopTaskType.ABORT;

        switch (taskInfo().type()) {
            case SETUP:
                return useNewMapper ? new HadoopV2SetupTask(taskInfo()) : new HadoopV1SetupTask(taskInfo());

            case MAP:
                return useNewMapper ? new HadoopV2MapTask(taskInfo()) : new HadoopV1MapTask(taskInfo());

            case REDUCE:
                return useNewReducer ? new HadoopV2ReduceTask(taskInfo(), true) :
                    new HadoopV1ReduceTask(taskInfo(), true);

            case COMBINE:
                return useNewCombiner ? new HadoopV2ReduceTask(taskInfo(), false) :
                    new HadoopV1ReduceTask(taskInfo(), false);

            case COMMIT:
            case ABORT:
                return useNewReducer ? new HadoopV2CleanupTask(taskInfo(), isAbort) :
                    new HadoopV1CleanupTask(taskInfo(), isAbort);

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
                if (e instanceof Error)
                    throw e;

                throw transformException(e);
            }

            if (cancelled)
                throw new HadoopTaskCancelledException("Task cancelled.");

            try {
                task.run(this);
            }
            catch (Throwable e) {
                if (e instanceof Error)
                    throw e;

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

        HadoopTask t = task;

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
            FileSystem.get(jobConf());

            LocalFileSystem locFs = FileSystem.getLocal(jobConf());

            locFs.setWorkingDirectory(new Path(locDir.getAbsolutePath()));
        }
        catch (Throwable e) {
            if (e instanceof Error)
                throw (Error)e;

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
    private TaskType taskType(HadoopTaskType type) {
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
    @Override public HadoopPartitioner partitioner() throws IgniteCheckedException {
        Class<?> partClsOld = jobConf().getClass("mapred.partitioner.class", null);

        if (partClsOld != null)
            return new HadoopV1Partitioner(jobConf().getPartitionerClass(), jobConf());

        try {
            return new HadoopV2Partitioner(jobCtx.getPartitionerClass(), jobConf());
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
    private HadoopSerialization getSerialization(Class<?> cls, Configuration jobConf) throws IgniteCheckedException {
        A.notNull(cls, "cls");

        SerializationFactory factory = new SerializationFactory(jobConf);

        Serialization<?> serialization = factory.getSerialization(cls);

        if (serialization == null)
            throw new IgniteCheckedException("Failed to find serialization for: " + cls.getName());

        if (serialization.getClass() == WritableSerialization.class)
            return new HadoopWritableSerialization((Class<? extends Writable>)cls);

        return new HadoopSerializationWrapper(serialization, cls);
    }

    /** {@inheritDoc} */
    @Override public HadoopSerialization keySerialization() throws IgniteCheckedException {
        return getSerialization(jobCtx.getMapOutputKeyClass(), jobConf());
    }

    /** {@inheritDoc} */
    @Override public HadoopSerialization valueSerialization() throws IgniteCheckedException {
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
    public Object getNativeSplit(HadoopInputSplit split) throws IgniteCheckedException {
        if (split instanceof HadoopExternalSplit)
            return readExternalSplit((HadoopExternalSplit)split);

        if (split instanceof HadoopSplitWrapper)
            return unwrapSplit((HadoopSplitWrapper)split);

        throw new IllegalStateException("Unknown split: " + split);
    }

    /**
     * @param split External split.
     * @return Native input split.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private Object readExternalSplit(HadoopExternalSplit split) throws IgniteCheckedException {
        Path jobDir = new Path(jobConf().get(MRJobConfig.MAPREDUCE_JOB_DIR));

        FileSystem fs;

        try {
            // This assertion uses .startsWith() instead of .equals() because task class loaders may
            // be reused between tasks of the same job.
            assert ((HadoopClassLoader)getClass().getClassLoader()).name()
                .startsWith(HadoopClassLoader.nameForTask(taskInfo(), true));

            // We also cache Fs there, all them will be cleared explicitly upon the Job end.
            fs = fileSystemForMrUserWithCaching(jobDir.toUri(), jobConf(), fsMap);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        try (
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

    /** {@inheritDoc} */
    @Override public <T> T runAsJobOwner(final Callable<T> c) throws IgniteCheckedException {
        String user = job.info().user();

        user = IgfsUtils.fixUserName(user);

        assert user != null;

        String ugiUser;

        try {
            UserGroupInformation currUser = UserGroupInformation.getCurrentUser();

            assert currUser != null;

            ugiUser = currUser.getShortUserName();
        }
        catch (IOException ioe) {
            throw new IgniteCheckedException(ioe);
        }

        try {
            if (F.eq(user, ugiUser))
                // if current UGI context user is the same, do direct call:
                return c.call();
            else {
                UserGroupInformation ugi = UserGroupInformation.getBestUGI(null, user);

                return ugi.doAs(new PrivilegedExceptionAction<T>() {
                    @Override public T run() throws Exception {
                        return c.call();
                    }
                });
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }
}