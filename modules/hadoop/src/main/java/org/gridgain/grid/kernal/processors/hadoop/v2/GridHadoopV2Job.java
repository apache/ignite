/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.gridgain.grid.kernal.processors.hadoop.fs.*;
import org.gridgain.grid.kernal.processors.hadoop.v1.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * Hadoop job implementation for v2 API.
 */
public class GridHadoopV2Job implements GridHadoopJob {
    /** */
    private static final boolean COMBINE_KEY_GROUPING_SUPPORTED;

    /**
     * Check for combiner grouping support (available since Hadoop 2.3).
     */
    static {
        boolean ok;

        try {
            org.apache.hadoop.mapreduce.JobContext.class.getDeclaredMethod("getCombinerKeyGroupingComparator");

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
    private final JobConf jobConf;

    /** */
    private final JobContextImpl jobContext;

    /** Logger. */
    private GridLogger log;

    /** Hadoop job ID. */
    private GridHadoopJobId jobId;

    /** Job info. */
    protected GridHadoopDefaultJobInfo jobInfo;

    /** Hadoop native job context. */
    //protected JobContextImpl ctx;

    /** */
    private JobID hadoopJobID;

    /** */
    private GridHadoopV2JobResourceManager rsrcMgr;

    /** */
    private GridHadoopPartitioner part;

    /** */
    private Comparator<?> sortComp;

    /** */
    private Comparator<?> combineGrpComp;

    /** */
    private Comparator<?> reduceGrpComp;

    /**
     * @param jobId Job ID.
     * @param jobInfo Job info.
     * @param log Logger.
     */
    public GridHadoopV2Job(GridHadoopJobId jobId, GridHadoopDefaultJobInfo jobInfo, GridLogger log) {
        assert jobId != null;
        assert jobInfo != null;

        this.jobId = jobId;
        this.jobInfo = jobInfo;
        this.log = log.getLogger(GridHadoopV2Job.class);

        hadoopJobID = new JobID(jobId.globalId().toString(), jobId.localId());

        jobConf = jobInfo.configuration();
        jobContext = new JobContextImpl(jobConf, hadoopJobID);

        GridHadoopFileSystemsUtils.setupFileSystems(jobConf);

        useNewMapper = jobConf.getUseNewMapper();
        useNewReducer = jobConf.getUseNewReducer();
        useNewCombiner = jobConf.getCombinerClass() == null;
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
        String jobDirPath = jobConf.get(MRJobConfig.MAPREDUCE_JOB_DIR);

        if (jobDirPath == null) { // Probably job was submitted not by hadoop client.
            // Assume that we have needed classes and try to generate input splits ourself.
            if (useNewMapper)
                return GridHadoopV2Splitter.splitJob(jobContext);
            else
                return GridHadoopV1Splitter.splitJob(jobConf);
        }

        Path jobDir = new Path(jobDirPath);

        try (FileSystem fs = FileSystem.get(jobDir.toUri(), jobConf)) {
            JobSplit.TaskSplitMetaInfo[] metaInfos = SplitMetaInfoReader.readSplitMetaInfo(hadoopJobID, fs, jobConf, jobDir);

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
            return jobConf.getClassByName(clsName);
        }
        catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    /**
     * @param split External split.
     * @return Native input split.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private Object readExternalSplit(GridHadoopExternalSplit split) throws GridException {
        Path jobDir = new Path(jobConf.get(MRJobConfig.MAPREDUCE_JOB_DIR));

        try (FileSystem fs = FileSystem.get(jobDir.toUri(), jobConf);
            FSDataInputStream in = fs.open(JobSubmissionFiles.getJobSplitFile(jobDir))) {

            Class<?> cls = readSplitClass(in, split.offset());

            assert cls != null;

            Serialization serialization = new SerializationFactory(jobConf).getSerialization(cls);

            Deserializer deserializer = serialization.getDeserializer(cls);

            deserializer.open(in);

            Object res = deserializer.deserialize(null);

            deserializer.close();

            assert res != null;

            return res;
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridHadoopPartitioner partitioner() throws GridException {
        return part;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTaskContext createTaskContext(GridHadoopTaskInfo info, GridHadoopTaskInput in,
        GridHadoopTaskOutput out) throws GridException {

        JobConf taskJobConf = new JobConf(jobInfo.configuration());

        Collection<URL> clsPath = rsrcMgr.classPath();

        if (!clsPath.isEmpty()) {
            URL[] urls = new URL[clsPath.size()];

            clsPath.toArray(urls);

            taskJobConf.setClassLoader(new ClassLoaderWrapper(new URLClassLoader(urls), getClass().getClassLoader()));
        }

        JobContextImpl taskJobContext = new JobContextImpl(taskJobConf, hadoopJobID);

        part = createPartitioner(taskJobContext);

        sortComp = taskJobContext.getSortComparator();

        combineGrpComp = COMBINE_KEY_GROUPING_SUPPORTED ? taskJobContext.getCombinerKeyGroupingComparator() : null;

        reduceGrpComp = taskJobContext.getGroupingComparator();

        GridHadoopTaskContext taskCtx = new GridHadoopV2TaskContext(info, this, in, out, taskJobContext);

        return taskCtx;
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTask createTask(GridHadoopTaskInfo taskInfo) {
        boolean isAbort = taskInfo.type() == GridHadoopTaskType.ABORT;

        switch (taskInfo.type()) {
            case SETUP:
                return useNewMapper ? new GridHadoopV2SetupTask(taskInfo, log) : new GridHadoopV1SetupTask(taskInfo, log);

            case MAP:
                return useNewMapper ? new GridHadoopV2MapTask(taskInfo, log) : new GridHadoopV1MapTask(taskInfo, log);

            case REDUCE:
                return useNewReducer ? new GridHadoopV2ReduceTask(taskInfo, true, log) :
                    new GridHadoopV1ReduceTask(taskInfo, true, log);

            case COMBINE:
                return useNewCombiner ? new GridHadoopV2ReduceTask(taskInfo, false, log) :
                    new GridHadoopV1ReduceTask(taskInfo, false, log);

            case COMMIT:
            case ABORT:
                return useNewReducer ? new GridHadoopV2CleanupTask(taskInfo, isAbort, log) :
                    new GridHadoopV1CleanupTask(taskInfo, isAbort, log);

            default:
                return null;
        }
    }

    /**
     * Gets serializer for specified class.
     * @param cls Class.
     * @return Appropriate serializer.
     */
    @SuppressWarnings("unchecked")
    private GridHadoopSerialization getSerialization(Class<?> cls) throws GridException {
        A.notNull(cls, "cls");

        SerializationFactory factory = new SerializationFactory(jobConf);

        Serialization<?> serialization = factory.getSerialization(cls);

        if (serialization == null)
            throw new GridException("Failed to find serialization for: " + cls.getName());

        if (serialization.getClass() == WritableSerialization.class)
            return new GridHadoopWritableSerialization((Class<? extends Writable>)cls);

        return new GridHadoopSerializationWrapper(serialization, cls);
    }

    /** {@inheritDoc} */
    @Override public GridHadoopSerialization keySerialization() throws GridException {
        return getSerialization(jobContext.getMapOutputKeyClass());
    }

    /** {@inheritDoc} */
    @Override public GridHadoopSerialization valueSerialization() throws GridException {
        return getSerialization(jobContext.getMapOutputValueClass());
    }

    /** {@inheritDoc} */
    @Override public Comparator<?> sortComparator() {
        return sortComp;
    }

    /** {@inheritDoc} */
    @Override public Comparator<?> reduceGroupComparator() {
        return reduceGrpComp;
    }

    /** {@inheritDoc} */
    @Override public Comparator<?> combineGroupComparator() {
        return combineGrpComp;
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
        TaskID tid = new TaskID(hadoopJobID, taskType(taskInfo.type()), taskInfo.taskNumber());

        return new TaskAttemptID(tid, taskInfo.attempt());
    }

    /**
     * @param split Split.
     * @return Native Hadoop split.
     * @throws GridException if failed.
     */
    @SuppressWarnings("unchecked")
    public Object getNativeSplit(GridHadoopInputSplit split) throws GridException {
        if (split instanceof GridHadoopExternalSplit)
            return readExternalSplit((GridHadoopExternalSplit)split);

        if (split instanceof GridHadoopSplitWrapper)
            return ((GridHadoopSplitWrapper)split).innerSplit();

        throw new IllegalStateException("Unknown split: " + split);
    }

    /**
     * Creates and initializes partitioner instance.
     *
     * @param ctx Hadoop job context.
     * @return Partitioner.
     * @throws GridException If fails.
     */
    private GridHadoopPartitioner createPartitioner(JobContextImpl ctx) throws GridException {
        Class<?> partClsOld = ctx.getConfiguration().getClass("mapred.partitioner.class", null);

        if (partClsOld != null)
            return new GridHadoopV1Partitioner(ctx.getJobConf().getPartitionerClass(), ctx.getConfiguration());

        try {
            return new GridHadoopV2Partitioner(ctx.getPartitionerClass(), ctx.getConfiguration());
        }
        catch (ClassNotFoundException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void initialize(boolean external, UUID locNodeId) throws GridException {
        rsrcMgr = new GridHadoopV2JobResourceManager(jobId, jobContext, locNodeId, log);

        rsrcMgr.prepareJobEnvironment(!external);
    }

    /** {@inheritDoc} */
    @Override public void dispose(boolean external) throws GridException {
        if (rsrcMgr != null)
            rsrcMgr.cleanupJobEnvironment(!external);
    }

    /** {@inheritDoc} */
    @Override public void prepareTaskEnvironment(GridHadoopTaskInfo info) throws GridException {
        rsrcMgr.prepareTaskEnvironment(info, jobConf);
    }

    /** {@inheritDoc} */
    @Override public void cleanupTaskEnvironment(GridHadoopTaskInfo info) throws GridException {
        rsrcMgr.cleanupTaskEnvironment(info);
    }

    /** {@inheritDoc} */
    @Override public void cleanupStagingDirectory() {
        if (rsrcMgr != null)
            rsrcMgr.cleanupStagingDirectory();
    }

    /**
     * Class loader wrapper.
     */
    private static class ClassLoaderWrapper extends ClassLoader {
        /** */
        private URLClassLoader delegate;

        /**
         * Makes classes available for GC.
         */
        public void destroy() {
            delegate = null;
        }

        /**
         * @param delegate Delegate.
         */
        private ClassLoaderWrapper(URLClassLoader delegate, ClassLoader parent) {
            super(parent);

            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public Class<?> loadClass(String name) throws ClassNotFoundException {
            try {
                return delegate.loadClass(name);
            }
            catch (ClassNotFoundException ignore) {
                return super.loadClass(name);
            }
        }

        /** {@inheritDoc} */
        @Override public InputStream getResourceAsStream(String name) {
            return delegate.getResourceAsStream(name);
        }

        /** {@inheritDoc} */
        @Override public URL findResource(final String name) {
            return delegate.findResource(name);
        }

        /** {@inheritDoc} */
        @Override public Enumeration<URL> findResources(final String name) throws IOException {
            return delegate.findResources(name);
        }
    }

}
