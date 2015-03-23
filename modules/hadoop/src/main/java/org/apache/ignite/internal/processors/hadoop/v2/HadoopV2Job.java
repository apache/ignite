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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.split.*;
import org.apache.ignite.*;
import org.apache.ignite.internal.processors.hadoop.*;
import org.apache.ignite.internal.processors.hadoop.fs.*;
import org.apache.ignite.internal.processors.hadoop.v1.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jsr166.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.Queue;
import java.util.concurrent.*;

import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.*;

/**
 * Hadoop job implementation for v2 API.
 */
public class HadoopV2Job implements HadoopJob {
    /** */
    private final JobConf jobConf;

    /** */
    private final JobContextImpl jobCtx;

    /** Hadoop job ID. */
    private final HadoopJobId jobId;

    /** Job info. */
    protected HadoopJobInfo jobInfo;

    /** */
    private final JobID hadoopJobID;

    /** */
    private final HadoopV2JobResourceManager rsrcMgr;

    /** */
    private final ConcurrentMap<T2<HadoopTaskType, Integer>, GridFutureAdapter<HadoopTaskContext>> ctxs =
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
    public HadoopV2Job(HadoopJobId jobId, final HadoopDefaultJobInfo jobInfo, IgniteLogger log) {
        assert jobId != null;
        assert jobInfo != null;

        this.jobId = jobId;
        this.jobInfo = jobInfo;

        hadoopJobID = new JobID(jobId.globalId().toString(), jobId.localId());

        HadoopClassLoader clsLdr = (HadoopClassLoader)getClass().getClassLoader();

        // Before create JobConf instance we should set new context class loader.
        Thread.currentThread().setContextClassLoader(clsLdr);

        jobConf = new JobConf();

        HadoopFileSystemsUtils.setupFileSystems(jobConf);

        Thread.currentThread().setContextClassLoader(null);

        for (Map.Entry<String,String> e : jobInfo.properties().entrySet())
            jobConf.set(e.getKey(), e.getValue());

        jobCtx = new JobContextImpl(jobConf, hadoopJobID);

        rsrcMgr = new HadoopV2JobResourceManager(jobId, jobCtx, log);
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
        Thread.currentThread().setContextClassLoader(jobConf.getClassLoader());

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

            try (FileSystem fs = FileSystem.get(jobDir.toUri(), jobConf)) {
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
                throw transformException(e);
            }
        }
        finally {
            Thread.currentThread().setContextClassLoader(null);
        }
    }

    /** {@inheritDoc} */
    @Override public HadoopTaskContext getTaskContext(HadoopTaskInfo info) throws IgniteCheckedException {
        T2<HadoopTaskType, Integer> locTaskId = new T2<>(info.type(),  info.taskNumber());

        GridFutureAdapter<HadoopTaskContext> fut = ctxs.get(locTaskId);

        if (fut != null)
            return fut.get();

        GridFutureAdapter<HadoopTaskContext> old = ctxs.putIfAbsent(locTaskId, fut = new GridFutureAdapter<>());

        if (old != null)
            return old.get();

        Class<?> cls = taskCtxClsPool.poll();

        try {
            if (cls == null) {
                // If there is no pooled class, then load new one.
                HadoopClassLoader ldr = new HadoopClassLoader(rsrcMgr.classPath());

                cls = ldr.loadClass(HadoopV2TaskContext.class.getName());
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

            throw te;
        }
    }

    /** {@inheritDoc} */
    @Override public void initialize(boolean external, UUID locNodeId) throws IgniteCheckedException {
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
    @Override public void dispose(boolean external) throws IgniteCheckedException {
        if (rsrcMgr != null && !external) {
            File jobLocDir = jobLocalDir(locNodeId, jobId);

            if (jobLocDir.exists())
                U.delete(jobLocDir);
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareTaskEnvironment(HadoopTaskInfo info) throws IgniteCheckedException {
        rsrcMgr.prepareTaskWorkDir(taskLocalDir(locNodeId, info));
    }

    /** {@inheritDoc} */
    @Override public void cleanupTaskEnvironment(HadoopTaskInfo info) throws IgniteCheckedException {
        HadoopTaskContext ctx = ctxs.remove(new T2<>(info.type(), info.taskNumber())).get();

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
