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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
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
import org.jdk8.backport.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

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

    /** */
    private final JobConf jobConf;

    /** */
    private final JobContextImpl jobCtx;

    /** Hadoop job ID. */
    private final GridHadoopJobId jobId;

    /** Job info. */
    protected GridHadoopDefaultJobInfo jobInfo;

    /** */
    private final JobID hadoopJobID;

    /** */
    private final GridHadoopV2JobResourceManager rsrcMgr;

    /** */
    private final ConcurrentMap<T2<GridHadoopTaskType, Integer>, GridHadoopTaskContext> ctxs = new ConcurrentHashMap8<>();

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

        hadoopJobID = new JobID(jobId.globalId().toString(), jobId.localId());

        jobConf = new JobConf(jobInfo.configuration());
        jobCtx = new JobContextImpl(jobConf, hadoopJobID);

        GridHadoopFileSystemsUtils.setupFileSystems(jobConf);

        useNewMapper = jobConf.getUseNewMapper();
        useNewReducer = jobConf.getUseNewReducer();
        useNewCombiner = jobConf.getCombinerClass() == null;

        rsrcMgr = new GridHadoopV2JobResourceManager(jobId, jobCtx, log);
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
                return GridHadoopV2Splitter.splitJob(jobCtx);
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

                    in.seek(off);

                    String clsName = Text.readString(in);

                    GridHadoopFileBlock block = GridHadoopV1Splitter.readFileBlock(clsName, in, hosts);

                    if (block == null)
                        block = GridHadoopV2Splitter.readFileBlock(clsName, in, hosts);

                    res.add(block != null ? block : new GridHadoopExternalSplit(hosts, off));
                }

                return res;
            }
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTaskContext getTaskContext(GridHadoopTaskInfo info) throws GridException {
        T2<GridHadoopTaskType, Integer> locTaskId =  new T2<>(info.type(),  info.taskNumber());

        GridHadoopTaskContext res = ctxs.get(locTaskId);

        if (res != null)
            return res;

        JobConf taskJobConf = new JobConf(jobConf);

        configureClassLoader(taskJobConf);

        JobContextImpl taskJobCtx = new JobContextImpl(taskJobConf, hadoopJobID);

        res = new GridHadoopV2TaskContext(info, this, taskJobCtx);

        GridHadoopTaskContext old = ctxs.putIfAbsent(locTaskId, res);

        return old == null ? res : old;
    }

    /**
     * Creates and sets class loader into jobConf.
     *
     * @param jobConf Job conf.
     */
    private void configureClassLoader(JobConf jobConf) {
        URL[] clsPath = rsrcMgr.classPath();

        if (!F.isEmpty(clsPath))
            jobConf.setClassLoader(new ClassLoaderWrapper(new URLClassLoader(clsPath), getClass().getClassLoader()));
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

    /** {@inheritDoc} */
    @Override public void initialize(boolean external, UUID locNodeId) throws GridException {
        rsrcMgr.prepareJobEnvironment(!external, locNodeId);
    }

    /** {@inheritDoc} */
    @Override public void dispose(boolean external) throws GridException {
        if (rsrcMgr != null)
            rsrcMgr.cleanupJobEnvironment(!external);
    }

    /**
     * Prepare local environment for the task.
     *
     * @param info Task info.
     * @throws GridException If failed.
     */
    public void prepareTaskEnvironment(GridHadoopTaskInfo info) throws GridException {
        rsrcMgr.prepareTaskEnvironment(info);
    }

    /**
     * Cleans up local environment of the task.
     *
     * @param info Task info.
     * @throws GridException If failed.
     */
    public void cleanupTaskEnvironment(GridHadoopTaskInfo info) throws GridException {
        rsrcMgr.cleanupTaskEnvironment(info);
    }

    /** {@inheritDoc} */
    @Override public void cleanupStagingDirectory() {
        if (rsrcMgr != null)
            rsrcMgr.cleanupStagingDirectory();
    }

    /**
     * Returns hadoop job context.
     *
     * @return Job context.
     */
    public JobContextImpl jobContext() {
        return jobCtx;
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
