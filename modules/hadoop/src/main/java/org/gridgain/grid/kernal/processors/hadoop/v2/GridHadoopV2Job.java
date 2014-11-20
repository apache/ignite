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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.split.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.counter.GridHadoopCountersImpl;
import org.gridgain.grid.kernal.processors.hadoop.fs.*;
import org.gridgain.grid.kernal.processors.hadoop.v1.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils.*;

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
    private final ConcurrentMap<T2<GridHadoopTaskType, Integer>, GridFutureAdapter<GridHadoopTaskContext>> ctxs =
        new ConcurrentHashMap8<>();

    /** Pooling task context class and thus class loading environment. */
    private final Queue<Class<?>> taskCtxClsPool = new ConcurrentLinkedQueue<>();

    /** Local node ID */
    private UUID locNodeId;

    /** Serialized JobConf. */
    private volatile byte[] jobConfData;

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

        GridHadoopClassLoader clsLdr = (GridHadoopClassLoader)getClass().getClassLoader();

        // Before create JobConf instance we should set new context class loader.
        Thread.currentThread().setContextClassLoader(clsLdr);

        jobConf = new JobConf();

        GridHadoopFileSystemsUtils.setupFileSystems(jobConf);

        Thread.currentThread().setContextClassLoader(null);

        for (Map.Entry<String,String> e : jobInfo.properties().entrySet())
            jobConf.set(e.getKey(), e.getValue());

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
        Thread.currentThread().setContextClassLoader(jobConf.getClassLoader());

        try {
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
                JobSplit.TaskSplitMetaInfo[] metaInfos = SplitMetaInfoReader.readSplitMetaInfo(hadoopJobID, fs, jobConf,
                    jobDir);

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
            catch (Throwable e) {
                throw transformException(e);
            }
        }
        finally {
            Thread.currentThread().setContextClassLoader(null);
        }
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTaskContext getTaskContext(GridHadoopTaskInfo info) throws GridException {
        T2<GridHadoopTaskType, Integer> locTaskId = new T2<>(info.type(),  info.taskNumber());

        GridFutureAdapter<GridHadoopTaskContext> fut = ctxs.get(locTaskId);

        if (fut != null)
            return fut.get();

        GridFutureAdapter<GridHadoopTaskContext> old = ctxs.putIfAbsent(locTaskId, fut = new GridFutureAdapter<>());

        if (old != null)
            return old.get();

        Class<?> cls = taskCtxClsPool.poll();

        try {
            if (cls == null) {
                // If there is no pooled class, then load new one.
                GridHadoopClassLoader ldr = new GridHadoopClassLoader(rsrcMgr.classPath());

                cls = ldr.loadClass(GridHadoopV2TaskContext.class.getName());
            }

            Constructor<?> ctr = cls.getConstructor(GridHadoopTaskInfo.class, GridHadoopJob.class,
                GridHadoopJobId.class, UUID.class, DataInput.class);

            if (jobConfData == null)
                synchronized(jobConf) {
                    if (jobConfData == null) {
                        ByteArrayOutputStream buf = new ByteArrayOutputStream();

                        jobConf.write(new DataOutputStream(buf));

                        jobConfData = buf.toByteArray();
                    }
                }

            GridHadoopTaskContext res = (GridHadoopTaskContext)ctr.newInstance(info, this, jobId, locNodeId,
                new DataInputStream(new ByteArrayInputStream(jobConfData)));

            res.counters(new GridHadoopCountersImpl());

            fut.onDone(res);

            return res;
        }
        catch (Throwable e) {
            GridException te = transformException(e);

            fut.onDone(te);

            throw te;
        }
    }

    /** {@inheritDoc} */
    @Override public void initialize(boolean external, UUID locNodeId) throws GridException {
        this.locNodeId = locNodeId;

        Thread.currentThread().setContextClassLoader(jobConf.getClassLoader());

        try {
            rsrcMgr.prepareJobEnvironment(!external, jobLocalDir(locNodeId, jobId));
        }
        finally {
            Thread.currentThread().setContextClassLoader(null);
        }
    }

    /** {@inheritDoc} */
    @Override public void dispose(boolean external) throws GridException {
        if (rsrcMgr != null && !external) {
            File jobLocDir = jobLocalDir(locNodeId, jobId);

            if (jobLocDir.exists())
                U.delete(jobLocDir);
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareTaskEnvironment(GridHadoopTaskInfo info) throws GridException {
        rsrcMgr.prepareTaskWorkDir(taskLocalDir(locNodeId, info));
    }

    /** {@inheritDoc} */
    @Override public void cleanupTaskEnvironment(GridHadoopTaskInfo info) throws GridException {
        GridHadoopTaskContext ctx = ctxs.remove(new T2<>(info.type(), info.taskNumber())).get();

        taskCtxClsPool.offer(ctx.getClass());

        File locDir = taskLocalDir(locNodeId, info);

        if (locDir.exists())
            U.delete(locDir);
    }

    /** {@inheritDoc} */
    @Override public void cleanupStagingDirectory() {
        if (rsrcMgr != null)
            rsrcMgr.cleanupStagingDirectory();
    }
}
