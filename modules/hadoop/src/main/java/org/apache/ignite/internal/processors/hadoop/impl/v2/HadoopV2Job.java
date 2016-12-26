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

package org.apache.ignite.internal.processors.hadoop.impl.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.hadoop.HadoopClassLoader;
import org.apache.ignite.internal.processors.hadoop.HadoopCommonUtils;
import org.apache.ignite.internal.processors.hadoop.HadoopDefaultJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopExternalSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopFileBlock;
import org.apache.ignite.internal.processors.hadoop.HadoopHelper;
import org.apache.ignite.internal.processors.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopJobProperty;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskType;
import org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopFileSystemsUtils;
import org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopLazyConcurrentMap;
import org.apache.ignite.internal.processors.hadoop.impl.v1.HadoopV1Splitter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.JOB_SHARED_CLASSLOADER;
import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.jobLocalDir;
import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.taskLocalDir;
import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.transformException;
import static org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopFileSystemCacheUtils.FsCacheKey;
import static org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopFileSystemCacheUtils.createHadoopLazyConcurrentMap;
import static org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopFileSystemCacheUtils.fileSystemForMrUserWithCaching;

/**
 * Hadoop job implementation for v2 API.
 */
public class HadoopV2Job implements HadoopJob {
    /** */
    private final JobConf jobConf;

    /** */
    private final JobContextImpl jobCtx;

    /** */
    private final HadoopHelper helper;

    /** Hadoop job ID. */
    private final HadoopJobId jobId;

    /** Job info. */
    protected final HadoopJobInfo jobInfo;

    /** Native library names. */
    private final String[] libNames;

    /** */
    private final JobID hadoopJobID;

    /** */
    private final HadoopV2JobResourceManager rsrcMgr;

    /** */
    private final ConcurrentMap<T2<HadoopTaskType, Integer>, GridFutureAdapter<HadoopTaskContext>> ctxs =
        new ConcurrentHashMap8<>();

    /** Pooling task context class and thus class loading environment. */
    private final Queue<Class<? extends HadoopTaskContext>> taskCtxClsPool = new ConcurrentLinkedQueue<>();

    /** All created contexts. */
    private final Queue<Class<? extends HadoopTaskContext>> fullCtxClsQueue = new ConcurrentLinkedDeque<>();

    /** File system cache map. */
    private final HadoopLazyConcurrentMap<FsCacheKey, FileSystem> fsMap = createHadoopLazyConcurrentMap();

    /** Shared class loader. */
    private volatile HadoopClassLoader sharedClsLdr;

    /** Local node ID */
    private volatile UUID locNodeId;

    /** Serialized JobConf. */
    private volatile byte[] jobConfData;

    /**
     * Constructor.
     *
     * @param jobId Job ID.
     * @param jobInfo Job info.
     * @param log Logger.
     * @param libNames Optional additional native library names.
     */
    public HadoopV2Job(HadoopJobId jobId, final HadoopDefaultJobInfo jobInfo, IgniteLogger log,
        @Nullable String[] libNames, HadoopHelper helper) {
        assert jobId != null;
        assert jobInfo != null;

        this.jobId = jobId;
        this.jobInfo = jobInfo;
        this.libNames = libNames;
        this.helper = helper;

        ClassLoader oldLdr = HadoopCommonUtils.setContextClassLoader(getClass().getClassLoader());

        try {
            hadoopJobID = new JobID(jobId.globalId().toString(), jobId.localId());

            jobConf = new JobConf();

            HadoopFileSystemsUtils.setupFileSystems(jobConf);

            for (Map.Entry<String,String> e : jobInfo.properties().entrySet())
                jobConf.set(e.getKey(), e.getValue());

            jobCtx = new JobContextImpl(jobConf, hadoopJobID);

            rsrcMgr = new HadoopV2JobResourceManager(jobId, jobCtx, log, this);
        }
        finally {
            HadoopCommonUtils.restoreContextClassLoader(oldLdr);
        }
    }

    /** {@inheritDoc} */
    @Override public HadoopJobId id() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override public HadoopJobInfo info() {
        return jobInfo;
    }

    /** {@inheritDoc} */
    @Override public Collection<HadoopInputSplit> input() throws IgniteCheckedException {
        ClassLoader oldLdr = HadoopCommonUtils.setContextClassLoader(jobConf.getClassLoader());

        try {
            String jobDirPath = jobConf.get(MRJobConfig.MAPREDUCE_JOB_DIR);

            if (jobDirPath == null) { // Probably job was submitted not by hadoop client.
                // Assume that we have needed classes and try to generate input splits ourself.
                if (jobConf.getUseNewMapper())
                    return HadoopV2Splitter.splitJob(jobCtx);
                else
                    return HadoopV1Splitter.splitJob(jobConf);
            }

            Path jobDir = new Path(jobDirPath);

            try {
                FileSystem fs = fileSystem(jobDir.toUri(), jobConf);

                JobSplit.TaskSplitMetaInfo[] metaInfos = SplitMetaInfoReader.readSplitMetaInfo(hadoopJobID, fs, jobConf,
                    jobDir);

                if (F.isEmpty(metaInfos))
                    throw new IgniteCheckedException("No input splits found.");

                Path splitsFile = JobSubmissionFiles.getJobSplitFile(jobDir);

                try (FSDataInputStream in = fs.open(splitsFile)) {
                    Collection<HadoopInputSplit> res = new ArrayList<>(metaInfos.length);

                    for (JobSplit.TaskSplitMetaInfo metaInfo : metaInfos) {
                        long off = metaInfo.getStartOffset();

                        String[] hosts = metaInfo.getLocations();

                        in.seek(off);

                        String clsName = Text.readString(in);

                        HadoopFileBlock block = HadoopV1Splitter.readFileBlock(clsName, in, hosts);

                        if (block == null)
                            block = HadoopV2Splitter.readFileBlock(clsName, in, hosts);

                        res.add(block != null ? block : new HadoopExternalSplit(hosts, off));
                    }

                    return res;
                }
            }
            catch (Throwable e) {
                if (e instanceof Error)
                    throw (Error)e;
                else
                    throw transformException(e);
            }
        }
        finally {
            HadoopCommonUtils.restoreContextClassLoader(oldLdr);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "MismatchedQueryAndUpdateOfCollection" })
    @Override public HadoopTaskContext getTaskContext(HadoopTaskInfo info) throws IgniteCheckedException {
        T2<HadoopTaskType, Integer> locTaskId = new T2<>(info.type(),  info.taskNumber());

        GridFutureAdapter<HadoopTaskContext> fut = ctxs.get(locTaskId);

        if (fut != null)
            return fut.get();

        GridFutureAdapter<HadoopTaskContext> old = ctxs.putIfAbsent(locTaskId, fut = new GridFutureAdapter<>());

        if (old != null)
            return old.get();

        Class<? extends HadoopTaskContext> cls = taskCtxClsPool.poll();

        try {
            if (cls == null) {
                // If there is no pooled class, then load new one.
                // Note that the classloader identified by the task it was initially created for,
                // but later it may be reused for other tasks.
                HadoopClassLoader ldr = sharedClsLdr != null ?
                    sharedClsLdr : createClassLoader(HadoopClassLoader.nameForTask(info, false));

                cls = (Class<? extends HadoopTaskContext>)ldr.loadClass(HadoopV2TaskContext.class.getName());

                fullCtxClsQueue.add(cls);
            }

            Constructor<?> ctr = cls.getConstructor(HadoopTaskInfo.class, HadoopJob.class,
                HadoopJobId.class, UUID.class, DataInput.class);

            if (jobConfData == null)
                synchronized(jobConf) {
                    if (jobConfData == null) {
                        ByteArrayOutputStream buf = new ByteArrayOutputStream();

                        jobConf.write(new DataOutputStream(buf));

                        jobConfData = buf.toByteArray();
                    }
                }

            HadoopTaskContext res = (HadoopTaskContext)ctr.newInstance(info, this, jobId, locNodeId,
                new DataInputStream(new ByteArrayInputStream(jobConfData)));

            fut.onDone(res);

            return res;
        }
        catch (Throwable e) {
            IgniteCheckedException te = transformException(e);

            fut.onDone(te);

            if (e instanceof Error)
                throw (Error)e;

            throw te;
        }
    }

    /** {@inheritDoc} */
    @Override public void initialize(boolean external, UUID locNodeId) throws IgniteCheckedException {
        assert locNodeId != null;

        this.locNodeId = locNodeId;

        ClassLoader oldLdr = HadoopCommonUtils.setContextClassLoader(getClass().getClassLoader());

        try {
            rsrcMgr.prepareJobEnvironment(!external, jobLocalDir(igniteWorkDirectory(), locNodeId, jobId));

            if (HadoopJobProperty.get(jobInfo, JOB_SHARED_CLASSLOADER, true))
                sharedClsLdr = createClassLoader(HadoopClassLoader.nameForJob(jobId));
        }
        finally {
            HadoopCommonUtils.restoreContextClassLoader(oldLdr);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ThrowFromFinallyBlock")
    @Override public void dispose(boolean external) throws IgniteCheckedException {
        try {
            if (rsrcMgr != null && !external) {
                File jobLocDir = jobLocalDir(igniteWorkDirectory(), locNodeId, jobId);

                if (jobLocDir.exists())
                    U.delete(jobLocDir);
            }
        }
        finally {
            taskCtxClsPool.clear();

            Throwable err = null;

            // Stop the daemon threads that have been created
            // with the task class loaders:
            while (true) {
                Class<? extends HadoopTaskContext> cls = fullCtxClsQueue.poll();

                if (cls == null)
                    break;

                try {
                    final ClassLoader ldr = cls.getClassLoader();

                    try {
                        // Stop Hadoop daemons for this *task*:
                        stopHadoopFsDaemons(ldr);
                    }
                    catch (Exception e) {
                        if (err == null)
                            err = e;
                    }

                    // Also close all the FileSystems cached in
                    // HadoopLazyConcurrentMap for this *task* class loader:
                    closeCachedTaskFileSystems(ldr);
                }
                catch (Throwable e) {
                    if (err == null)
                        err = e;

                    if (e instanceof Error)
                        throw (Error)e;
                }
            }

            assert fullCtxClsQueue.isEmpty();

            try {
                // Close all cached file systems for this *Job*:
                fsMap.close();
            }
            catch (Exception e) {
                if (err == null)
                    err = e;
            }

            if (err != null)
                throw U.cast(err);
        }
    }

    /**
     * Stops Hadoop Fs daemon threads.
     * @param ldr The task ClassLoader to stop the daemons for.
     * @throws Exception On error.
     */
    private void stopHadoopFsDaemons(ClassLoader ldr) throws Exception {
        Class<?> daemonCls = ldr.loadClass(HadoopClassLoader.CLS_DAEMON);

        Method m = daemonCls.getMethod("dequeueAndStopAll");

        m.invoke(null);
    }

    /**
     * Closes all the file systems user by task
     * @param ldr The task class loader.
     * @throws Exception On error.
     */
    private void closeCachedTaskFileSystems(ClassLoader ldr) throws Exception {
        Class<?> clazz = ldr.loadClass(HadoopV2TaskContext.class.getName());

        Method m = clazz.getMethod("close");

        m.invoke(null);
    }

    /** {@inheritDoc} */
    @Override public void prepareTaskEnvironment(HadoopTaskInfo info) throws IgniteCheckedException {
        rsrcMgr.prepareTaskWorkDir(taskLocalDir(igniteWorkDirectory(), locNodeId, info));
    }

    /** {@inheritDoc} */
    @Override public void cleanupTaskEnvironment(HadoopTaskInfo info) throws IgniteCheckedException {
        HadoopTaskContext ctx = ctxs.remove(new T2<>(info.type(), info.taskNumber())).get();

        taskCtxClsPool.add(ctx.getClass());

        File locDir = taskLocalDir(igniteWorkDirectory(), locNodeId, info);

        if (locDir.exists())
            U.delete(locDir);
    }

    /** {@inheritDoc} */
    @Override public void cleanupStagingDirectory() {
        rsrcMgr.cleanupStagingDirectory();
    }

    /** {@inheritDoc} */
    @Override public String igniteWorkDirectory() {
        return helper.workDirectory();
    }

    /**
     * Getter for job configuration.
     * @return The job configuration.
     */
    public JobConf jobConf() {
        return jobConf;
    }

    /**
     * Gets file system for this job.
     * @param uri The uri.
     * @param cfg The configuration.
     * @return The file system.
     * @throws IOException On error.
     */
    public FileSystem fileSystem(@Nullable URI uri, Configuration cfg) throws IOException {
        return fileSystemForMrUserWithCaching(uri, cfg, fsMap);
    }

    /**
     * Create class loader with the given name.
     *
     * @param name Name.
     * @return Class loader.
     */
    private HadoopClassLoader createClassLoader(String name) {
        return new HadoopClassLoader(rsrcMgr.classPath(), name, libNames, helper);
    }
}