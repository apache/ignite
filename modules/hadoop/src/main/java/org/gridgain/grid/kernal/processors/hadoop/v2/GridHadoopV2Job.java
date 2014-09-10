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
import org.gridgain.grid.kernal.processors.hadoop.GridHadoopClassLoader;
import org.gridgain.grid.kernal.processors.hadoop.v1.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Hadoop job implementation for v2 API.
 */
public class GridHadoopV2Job implements GridHadoopJob {
    /** */
    private final JobConf jobConf;

    /** */
    private final JobContextImpl jobCtx;

    /** Hadoop job ID. */
    private final GridHadoopJobId jobId;

    /** Job info. */
    protected GridHadoopJobInfo jobInfo;

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
    public GridHadoopV2Job(GridHadoopJobId jobId, final GridHadoopDefaultJobInfo jobInfo, GridLogger log) {
        assert jobId != null;
        assert jobInfo != null;

        this.jobId = jobId;
        this.jobInfo = jobInfo;

        hadoopJobID = new JobID(jobId.globalId().toString(), jobId.localId());

        // Before create JobConf instance we should set new context class loader.
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        jobConf = new JobConf() {
            {
                getProps().putAll(jobInfo.properties());
            }
        };

        jobCtx = new JobContextImpl(jobConf, hadoopJobID);

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
            if (jobConf.getUseNewMapper())
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

        GridHadoopClassLoader ldr = new GridHadoopClassLoader(rsrcMgr.classPath());

        try {
            Class<?> cls = ldr.loadClassExplicitly(GridHadoopV2TaskContext.class.getName());

            Constructor<?> ctr = cls.getConstructor(GridHadoopTaskInfo.class, GridHadoopJob.class, GridHadoopJobId.class);

            res = (GridHadoopTaskContext)ctr.newInstance(info, this, jobId);
        }
        catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException |
            IllegalAccessException e) {

            throw new GridException(e);
        }

        GridHadoopTaskContext old = ctxs.putIfAbsent(locTaskId, res);

        return old == null ? res : old;
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

    /** {@inheritDoc} */
    @Override public void prepareTaskEnvironment(GridHadoopTaskInfo info) throws GridException {
        rsrcMgr.prepareTaskEnvironment(info);
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
     * Returns hadoop job context.
     *
     * @return Job context.
     */
    public JobContextImpl jobContext() {
        return jobCtx;
    }
}
