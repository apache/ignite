/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.split.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v1.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
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

    /** */
    private JobID hadoopJobID;

    /**
     * @param jobId Job ID.
     * @param jobInfo Job info.
     */
    public GridHadoopV2Job(GridHadoopJobId jobId, GridHadoopDefaultJobInfo jobInfo) {
        assert jobId != null;
        assert jobInfo != null;

        this.jobId = jobId;
        this.jobInfo = jobInfo;

        hadoopJobID = new JobID(jobId.globalId().toString(), jobId.localId());

        JobConf cfg = jobInfo.configuration();

        ctx = new JobContextImpl(cfg, hadoopJobID);
        useNewMapper = cfg.getUseNewMapper();
        useNewReducer = cfg.getUseNewReducer();
        useNewCombiner = cfg.getCombinerClass() == null;
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
    @Override public Collection<GridHadoopInputSplit> input() throws GridException {
        String jobDirPath = ctx.getConfiguration().get(MRJobConfig.MAPREDUCE_JOB_DIR);

        if (jobDirPath == null) { // Probably job was submitted not by hadoop client.
            // Assume that we have needed classes and try to generate input splits ourself.
            if (useNewMapper)
                return GridHadoopV2Splitter.splitJob(ctx);
            else
                return GridHadoopV1Splitter.splitJob(ctx.getJobConf());
        }

        Path jobDir = new Path(jobDirPath);

        try (FileSystem fs = FileSystem.get(jobDir.toUri(), ctx.getConfiguration())) {
            JobSplit.TaskSplitMetaInfo[] metaInfos = SplitMetaInfoReader.readSplitMetaInfo(hadoopJobID, fs,
                ctx.getConfiguration(), jobDir);

            if (F.isEmpty(metaInfos))
                throw new GridException("No input splits found.");

            Path splitsFile = JobSubmissionFiles.getJobSplitFile(jobDir);

            try (FSDataInputStream in = fs.open(splitsFile)) {
                Collection<GridHadoopInputSplit> res = new ArrayList<>(metaInfos.length);

                for (JobSplit.TaskSplitMetaInfo metaInfo : metaInfos) {
                    long off = metaInfo.getStartOffset();

                    String[] hosts = metaInfo.getLocations();

                    Class<?> cls = readSplitClass(in, off);

                    GridHadoopFileBlock block = null;

                    if (cls != null) {
                        block = GridHadoopV1Splitter.readFileBlock(cls, in, hosts);

                        if (block == null)
                            block = GridHadoopV2Splitter.readFileBlock(cls, in, hosts);
                    }

                    res.add(block != null ? block : new GridHadoopExternalSplit(hosts, off));
                }

                return res;
            }
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /**
     * @param in Input stream.
     * @param off Offset in stream.
     * @return Class or {@code null} if not found.
     * @throws IOException If failed.
     */
    @Nullable private Class<?> readSplitClass(FSDataInputStream in, long off)
        throws IOException {
        in.seek(off);

        String clsName = Text.readString(in);

        try {
            return ctx.getConfiguration().getClassByName(clsName);
        }
        catch (ClassNotFoundException e) {
            // No-op.
        }

        return null;
    }

    /**
     * @param split External split.
     * @return Native input split.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    public <T> T readExternalSplit(GridHadoopExternalSplit split) throws GridException {
        Path jobDir = new Path(ctx.getConfiguration().get(MRJobConfig.MAPREDUCE_JOB_DIR));

        Class<?> cls;

        try (FileSystem fs = FileSystem.get(jobDir.toUri(), ctx.getConfiguration());
            FSDataInputStream in = fs.open(JobSubmissionFiles.getJobSplitFile(jobDir))) {
            cls = readSplitClass(in, split.offset());

            assert cls != null;

            Serialization serialization = new SerializationFactory(ctx.getJobConf()).getSerialization(cls);

            Deserializer deserializer = serialization.getDeserializer(cls);

            deserializer.open(in);

            Object res = deserializer.deserialize(null);

            deserializer.close();

            assert res != null;

            return (T)res;
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int reducers() {
        return ctx.getNumReduceTasks();
    }

    /**
     * @return {@code True} in case reducer exists.
     */
    public boolean hasReducer() {
        return reducers() != 0;
    }

    /**
     * @return {@code True} in case either combiner or reducer exists.
     */
    public boolean hasCombinerOrReducer() {
        return hasCombiner() || hasReducer();
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
            case SETUP:
                return useNewMapper ? new GridHadoopV2SetupTask(taskInfo) : new GridHadoopV1SetupTask(taskInfo);

            case MAP:
                return useNewMapper ? new GridHadoopV2MapTask(taskInfo) : new GridHadoopV1MapTask(taskInfo);

            case REDUCE:
                return useNewReducer ? new GridHadoopV2ReduceTask(taskInfo, true) :
                    new GridHadoopV1ReduceTask(taskInfo, true);

            case COMBINE:
                return useNewCombiner ? new GridHadoopV2ReduceTask(taskInfo, false) :
                    new GridHadoopV1ReduceTask(taskInfo, false);

            case COMMIT:
            case ABORT:
                return useNewReducer ? new GridHadoopV2CleanupTask(taskInfo, isAbort) :
                    new GridHadoopV1CleanupTask(taskInfo, isAbort);

            default:
                return null;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasCombiner() {
        return ctx.getJobConf().get("mapred.combiner.class") != null ||
            ctx.getJobConf().get(MRJobConfig.COMBINE_CLASS_ATTR) != null;
    }

    /**
     * Gets serializer for specified class.
     * @param cls Class.
     * @return Appropriate serializer.
     */
    @SuppressWarnings("unchecked")
    private GridHadoopSerialization getSerialization(Class<?> cls) throws GridException {
        SerializationFactory factory = new SerializationFactory(ctx.getJobConf());

        Serialization<?> serialization = factory.getSerialization(cls);

        if (serialization.getClass() == WritableSerialization.class)
            return new GridHadoopWritableSerialization((Class<? extends Writable>)cls);

        return new GridHadoopSerializationWrapper(serialization, cls);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopSerialization keySerialization() throws GridException {
        return getSerialization(ctx.getMapOutputKeyClass());
    }

    /** {@inheritDoc} */
    @Override public GridHadoopSerialization valueSerialization() throws GridException {
        return getSerialization(ctx.getMapOutputValueClass());
    }

    /** {@inheritDoc} */
    @Override public Comparator<?> sortComparator() {
        return ctx.getSortComparator();
    }

    /** {@inheritDoc} */
    @Override public Comparator<?> reduceGroupComparator() {
        return ctx.getGroupingComparator();
    }

    /** {@inheritDoc} */
    @Override public Comparator<Object> combineGroupComparator() {
        return null; // TODO implement for Hadoop 2.4
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
