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

package org.apache.ignite.internal.processors.hadoop.taskexecutor.external;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.HadoopContext;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobPhase;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReducePlan;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskType;
import org.apache.ignite.internal.processors.hadoop.jobtracker.HadoopJobMetadata;
import org.apache.ignite.internal.processors.hadoop.jobtracker.HadoopJobTracker;
import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopTaskExecutorAdapter;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopTaskState;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopTaskStatus;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.child.HadoopExternalProcessStarter;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.communication.HadoopExternalCommunication;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.communication.HadoopMessageListener;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.IgnitePortProtocol;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedDeque8;

import static org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopTaskState.CRASHED;
import static org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopTaskState.FAILED;

/**
 * External process registry. Handles external process lifecycle.
 */
public class HadoopExternalTaskExecutor extends HadoopTaskExecutorAdapter {
    /** Hadoop context. */
    private HadoopContext ctx;

    /** */
    private String javaCmd;

    /** Logger. */
    private IgniteLogger log;

    /** Node process descriptor. */
    private HadoopProcessDescriptor nodeDesc;

    /** Output base. */
    private File outputBase;

    /** Path separator. */
    private String pathSep;

    /** Hadoop external communication. */
    private HadoopExternalCommunication comm;

    /** Starting processes. */
    private final ConcurrentMap<UUID, HadoopProcess> runningProcsByProcId = new ConcurrentHashMap8<>();

    /** Starting processes. */
    private final ConcurrentMap<HadoopJobId, HadoopProcess> runningProcsByJobId = new ConcurrentHashMap8<>();

    /** Busy lock. */
    private final GridSpinReadWriteLock busyLock = new GridSpinReadWriteLock();

    /** Job tracker. */
    private HadoopJobTracker jobTracker;

    /** {@inheritDoc} */
    @Override public void start(HadoopContext ctx) throws IgniteCheckedException {
        this.ctx = ctx;

        log = ctx.kernalContext().log(HadoopExternalTaskExecutor.class);

        outputBase = U.resolveWorkDirectory("hadoop", false);

        pathSep = System.getProperty("path.separator", U.isWindows() ? ";" : ":");

        initJavaCommand();

        comm = new HadoopExternalCommunication(
            ctx.localNodeId(),
            UUID.randomUUID(),
            ctx.kernalContext().config().getMarshaller(),
            log,
            ctx.kernalContext().getSystemExecutorService(),
            ctx.kernalContext().gridName());

        comm.setListener(new MessageListener());

        comm.start();

        nodeDesc = comm.localProcessDescriptor();

        ctx.kernalContext().ports().registerPort(nodeDesc.tcpPort(), IgnitePortProtocol.TCP,
            HadoopExternalTaskExecutor.class);

        if (nodeDesc.sharedMemoryPort() != -1)
            ctx.kernalContext().ports().registerPort(nodeDesc.sharedMemoryPort(), IgnitePortProtocol.TCP,
                HadoopExternalTaskExecutor.class);

        jobTracker = ctx.jobTracker();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        busyLock.writeLock();

        try {
            comm.stop();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to gracefully stop external hadoop communication server (will shutdown anyway)", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onJobStateChanged(final HadoopJobMetadata meta) {
        final HadoopProcess proc = runningProcsByJobId.get(meta.jobId());

        // If we have a local process for this job.
        if (proc != null) {
            if (log.isDebugEnabled())
                log.debug("Updating job information for remote task process [proc=" + proc + ", meta=" + meta + ']');

            if (meta.phase() == HadoopJobPhase.PHASE_COMPLETE) {
                if (log.isDebugEnabled())
                    log.debug("Completed job execution, will terminate child process [jobId=" + meta.jobId() +
                        ", proc=" + proc + ']');

                runningProcsByJobId.remove(meta.jobId());
                runningProcsByProcId.remove(proc.descriptor().processId());

                proc.terminate();

                return;
            }

            if (proc.initFut.isDone()) {
                if (!proc.initFut.isFailed())
                    sendJobInfoUpdate(proc, meta);
                else if (log.isDebugEnabled())
                    log.debug("Failed to initialize child process (will skip job state notification) " +
                        "[jobId=" + meta.jobId() + ", meta=" + meta + ']');
            }
            else {
                proc.initFut.listen(new CI1<IgniteInternalFuture<IgniteBiTuple<Process, HadoopProcessDescriptor>>>() {
                    @Override
                    public void apply(IgniteInternalFuture<IgniteBiTuple<Process, HadoopProcessDescriptor>> f) {
                        try {
                            f.get();

                            sendJobInfoUpdate(proc, meta);
                        }
                        catch (IgniteCheckedException e) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to initialize child process (will skip job state notification) " +
                                    "[jobId=" + meta.jobId() + ", meta=" + meta + ", err=" + e + ']');
                        }

                    }
                });
            }
        }
        else if (ctx.isParticipating(meta)) {
            HadoopJob job;

            try {
                job = jobTracker.job(meta.jobId(), meta.jobInfo());
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to get job: " + meta.jobId(), e);

                return;
            }

            startProcess(job, meta.mapReducePlan());
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public void run(final HadoopJob job, final Collection<HadoopTaskInfo> tasks) throws IgniteCheckedException {
        if (!busyLock.tryReadLock()) {
            if (log.isDebugEnabled())
                log.debug("Failed to start hadoop tasks (grid is stopping, will ignore).");

            return;
        }

        try {
            HadoopProcess proc = runningProcsByJobId.get(job.id());

            HadoopTaskType taskType = F.first(tasks).type();

            if (taskType == HadoopTaskType.SETUP || taskType == HadoopTaskType.ABORT ||
                taskType == HadoopTaskType.COMMIT) {
                if (proc == null || proc.terminated()) {
                    runningProcsByJobId.remove(job.id(), proc);

                    // Start new process for ABORT task since previous processes were killed.
                    proc = startProcess(job, jobTracker.plan(job.id()));

                    if (log.isDebugEnabled())
                        log.debug("Starting new process for maintenance task [jobId=" + job.id() +
                            ", proc=" + proc + ", taskType=" + taskType + ']');
                }
            }
            else
                assert proc != null : "Missing started process for task execution request: " + job.id() +
                    ", tasks=" + tasks;

            final HadoopProcess proc0 = proc;

            proc.initFut.listen(new CI1<IgniteInternalFuture<IgniteBiTuple<Process, HadoopProcessDescriptor>>>() {
                @Override public void apply(
                    IgniteInternalFuture<IgniteBiTuple<Process, HadoopProcessDescriptor>> f) {
                    if (!busyLock.tryReadLock())
                        return;

                    try {
                        f.get();

                        proc0.addTasks(tasks);

                        if (log.isDebugEnabled())
                            log.debug("Sending task execution request to child process [jobId=" + job.id() +
                                ", proc=" + proc0 + ", tasks=" + tasks + ']');

                        sendExecutionRequest(proc0, job, tasks);
                    }
                    catch (IgniteCheckedException e) {
                        notifyTasksFailed(tasks, FAILED, e);
                    }
                    finally {
                        busyLock.readUnlock();
                    }
                }
            });
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void cancelTasks(HadoopJobId jobId) {
        HadoopProcess proc = runningProcsByJobId.get(jobId);

        if (proc != null)
            proc.terminate();
    }

    /**
     * Sends execution request to remote node.
     *
     * @param proc Process to send request to.
     * @param job Job instance.
     * @param tasks Collection of tasks to execute in started process.
     */
    private void sendExecutionRequest(HadoopProcess proc, HadoopJob job, Collection<HadoopTaskInfo> tasks)
        throws IgniteCheckedException {
        // Must synchronize since concurrent process crash may happen and will receive onConnectionLost().
        proc.lock();

        try {
            if (proc.terminated()) {
                notifyTasksFailed(tasks, CRASHED, null);

                return;
            }

            HadoopTaskExecutionRequest req = new HadoopTaskExecutionRequest();

            req.jobId(job.id());
            req.jobInfo(job.info());
            req.tasks(tasks);

            comm.sendMessage(proc.descriptor(), req);
        }
        finally {
            proc.unlock();
        }
    }

    /**
     * @return External task metadata.
     */
    private HadoopExternalTaskMetadata buildTaskMeta() {
        HadoopExternalTaskMetadata meta = new HadoopExternalTaskMetadata();

        meta.classpath(Arrays.asList(System.getProperty("java.class.path").split(File.pathSeparator)));
        meta.jvmOptions(Arrays.asList("-Xmx1g", "-ea", "-XX:+UseConcMarkSweepGC", "-XX:+CMSClassUnloadingEnabled",
            "-DIGNITE_HOME=" + U.getIgniteHome()));

        return meta;
    }

    /**
     * @param tasks Tasks to notify about.
     * @param state Fail state.
     * @param e Optional error.
     */
    private void notifyTasksFailed(Iterable<HadoopTaskInfo> tasks, HadoopTaskState state, Throwable e) {
        HadoopTaskStatus fail = new HadoopTaskStatus(state, e);

        for (HadoopTaskInfo task : tasks)
            jobTracker.onTaskFinished(task, fail);
    }

    /**
     * Starts process template that will be ready to execute Hadoop tasks.
     *
     * @param job Job instance.
     * @param plan Map reduce plan.
     */
    private HadoopProcess startProcess(final HadoopJob job, final HadoopMapReducePlan plan) {
        final UUID childProcId = UUID.randomUUID();

        HadoopJobId jobId = job.id();

        final HadoopProcessFuture fut = new HadoopProcessFuture(childProcId, jobId);

        final HadoopProcess proc = new HadoopProcess(jobId, fut, plan.reducers(ctx.localNodeId()));

        HadoopProcess old = runningProcsByJobId.put(jobId, proc);

        assert old == null;

        old = runningProcsByProcId.put(childProcId, proc);

        assert old == null;

        ctx.kernalContext().closure().runLocalSafe(new Runnable() {
            @Override public void run() {
                if (!busyLock.tryReadLock()) {
                    fut.onDone(new IgniteCheckedException("Failed to start external process (grid is stopping)."));

                    return;
                }

                try {
                    HadoopExternalTaskMetadata startMeta = buildTaskMeta();

                    if (log.isDebugEnabled())
                        log.debug("Created hadoop child process metadata for job [job=" + job +
                            ", childProcId=" + childProcId + ", taskMeta=" + startMeta + ']');

                    Process proc = startJavaProcess(childProcId, startMeta, job);

                    BufferedReader rdr = new BufferedReader(new InputStreamReader(proc.getInputStream()));

                    String line;

                    // Read up all the process output.
                    while ((line = rdr.readLine()) != null) {
                        if (log.isDebugEnabled())
                            log.debug("Tracing process output: " + line);

                        if ("Started".equals(line)) {
                            // Process started successfully, it should not write anything more to the output stream.
                            if (log.isDebugEnabled())
                                log.debug("Successfully started child process [childProcId=" + childProcId +
                                    ", meta=" + job + ']');

                            fut.onProcessStarted(proc);

                            break;
                        }
                        else if ("Failed".equals(line)) {
                            StringBuilder sb = new StringBuilder("Failed to start child process: " + job + "\n");

                            while ((line = rdr.readLine()) != null)
                                sb.append("    ").append(line).append("\n");

                            // Cut last character.
                            sb.setLength(sb.length() - 1);

                            log.warning(sb.toString());

                            fut.onDone(new IgniteCheckedException(sb.toString()));

                            break;
                        }
                    }
                }
                catch (Throwable e) {
                    fut.onDone(new IgniteCheckedException("Failed to initialize child process: " + job, e));

                    if (e instanceof Error)
                        throw (Error)e;
                }
                finally {
                    busyLock.readUnlock();
                }
            }
        }, true);

        fut.listen(new CI1<IgniteInternalFuture<IgniteBiTuple<Process, HadoopProcessDescriptor>>>() {
            @Override public void apply(IgniteInternalFuture<IgniteBiTuple<Process, HadoopProcessDescriptor>> f) {
                try {
                    // Make sure there were no exceptions.
                    f.get();

                    prepareForJob(proc, job, plan);
                }
                catch (IgniteCheckedException ignore) {
                    // Exception is printed in future's onDone() method.
                }
            }
        });

        return proc;
    }

    /**
     * Checks that java local command is available.
     *
     * @throws IgniteCheckedException If initialization failed.
     */
    private void initJavaCommand() throws IgniteCheckedException {
        String javaHome = System.getProperty("java.home");

        if (javaHome == null)
            javaHome = System.getenv("JAVA_HOME");

        if (javaHome == null)
            throw new IgniteCheckedException("Failed to locate JAVA_HOME.");

        javaCmd = javaHome + File.separator + "bin" + File.separator + (U.isWindows() ? "java.exe" : "java");

        try {
            Process proc = new ProcessBuilder(javaCmd, "-version").redirectErrorStream(true).start();

            Collection<String> out = readProcessOutput(proc);

            int res = proc.waitFor();

            if (res != 0)
                throw new IgniteCheckedException("Failed to execute 'java -version' command (process finished with nonzero " +
                    "code) [exitCode=" + res + ", javaCmd='" + javaCmd + "', msg=" + F.first(out) + ']');

            if (log.isInfoEnabled()) {
                log.info("Will use java for external task execution: ");

                for (String s : out)
                    log.info("    " + s);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to check java for external task execution.", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteCheckedException("Failed to wait for process completion (thread got interrupted).", e);
        }
    }

    /**
     * Reads process output line-by-line.
     *
     * @param proc Process to read output.
     * @return Read lines.
     * @throws IOException If read failed.
     */
    private Collection<String> readProcessOutput(Process proc) throws IOException {
        BufferedReader rdr = new BufferedReader(new InputStreamReader(proc.getInputStream()));

        Collection<String> res = new ArrayList<>();

        String s;

        while ((s = rdr.readLine()) != null)
            res.add(s);

        return res;
    }

    /**
     * Builds process from metadata.
     *
     * @param childProcId Child process ID.
     * @param startMeta Metadata.
     * @param job Job.
     * @return Started process.
     */
    private Process startJavaProcess(UUID childProcId, HadoopExternalTaskMetadata startMeta,
        HadoopJob job) throws Exception {
        String outFldr = jobWorkFolder(job.id()) + File.separator + childProcId;

        if (log.isDebugEnabled())
            log.debug("Will write process log output to: " + outFldr);

        List<String> cmd = new ArrayList<>();

        File workDir = U.resolveWorkDirectory("", false);

        cmd.add(javaCmd);
        cmd.addAll(startMeta.jvmOptions());
        cmd.add("-cp");
        cmd.add(buildClasspath(startMeta.classpath()));
        cmd.add(HadoopExternalProcessStarter.class.getName());
        cmd.add("-cpid");
        cmd.add(String.valueOf(childProcId));
        cmd.add("-ppid");
        cmd.add(String.valueOf(nodeDesc.processId()));
        cmd.add("-nid");
        cmd.add(String.valueOf(nodeDesc.parentNodeId()));
        cmd.add("-addr");
        cmd.add(nodeDesc.address());
        cmd.add("-tport");
        cmd.add(String.valueOf(nodeDesc.tcpPort()));
        cmd.add("-sport");
        cmd.add(String.valueOf(nodeDesc.sharedMemoryPort()));
        cmd.add("-out");
        cmd.add(outFldr);
        cmd.add("-wd");
        cmd.add(workDir.getAbsolutePath());

        return new ProcessBuilder(cmd)
            .redirectErrorStream(true)
            .directory(workDir)
            .start();
    }

    /**
     * Gets job work folder.
     *
     * @param jobId Job ID.
     * @return Job work folder.
     */
    private String jobWorkFolder(HadoopJobId jobId) {
        return outputBase + File.separator + "Job_" + jobId;
    }

    /**
     * @param cp Classpath collection.
     * @return Classpath string.
     */
    private String buildClasspath(Collection<String> cp) {
        assert !cp.isEmpty();

        StringBuilder sb = new StringBuilder();

        for (String s : cp)
            sb.append(s).append(pathSep);

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    /**
     * Sends job info update request to remote process.
     *
     * @param proc Process to send request to.
     * @param meta Job metadata.
     */
    private void sendJobInfoUpdate(HadoopProcess proc, HadoopJobMetadata meta) {
        Map<Integer, HadoopProcessDescriptor> rdcAddrs = meta.reducersAddresses();

        int rdcNum = meta.mapReducePlan().reducers();

        HadoopProcessDescriptor[] addrs = null;

        if (rdcAddrs != null && rdcAddrs.size() == rdcNum) {
            addrs = new HadoopProcessDescriptor[rdcNum];

            for (int i = 0; i < rdcNum; i++) {
                HadoopProcessDescriptor desc = rdcAddrs.get(i);

                assert desc != null : "Missing reducing address [meta=" + meta + ", rdc=" + i + ']';

                addrs[i] = desc;
            }
        }

        try {
            comm.sendMessage(proc.descriptor(), new HadoopJobInfoUpdateRequest(proc.jobId, meta.phase(), addrs));
        }
        catch (IgniteCheckedException e) {
            if (!proc.terminated()) {
                log.error("Failed to send job state update message to remote child process (will kill the process) " +
                    "[jobId=" + proc.jobId + ", meta=" + meta + ']', e);

                proc.terminate();
            }
        }
    }

    /**
     * Sends prepare request to remote process.
     *
     * @param proc Process to send request to.
     * @param job Job.
     * @param plan Map reduce plan.
     */
    private void prepareForJob(HadoopProcess proc, HadoopJob job, HadoopMapReducePlan plan) {
        try {
            comm.sendMessage(proc.descriptor(), new HadoopPrepareForJobRequest(job.id(), job.info(),
                plan.reducers(), plan.reducers(ctx.localNodeId())));
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send job prepare request to remote process [proc=" + proc + ", job=" + job +
                ", plan=" + plan + ']', e);

            proc.terminate();
        }
    }

    /**
     * Processes task finished message.
     *
     * @param desc Remote process descriptor.
     * @param taskMsg Task finished message.
     */
    private void processTaskFinishedMessage(HadoopProcessDescriptor desc, HadoopTaskFinishedMessage taskMsg) {
        HadoopProcess proc = runningProcsByProcId.get(desc.processId());

        if (proc != null)
            proc.removeTask(taskMsg.taskInfo());

        jobTracker.onTaskFinished(taskMsg.taskInfo(), taskMsg.status());
    }

    /**
     *
     */
    private class MessageListener implements HadoopMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessageReceived(HadoopProcessDescriptor desc, HadoopMessage msg) {
            if (!busyLock.tryReadLock())
                return;

            try {
                if (msg instanceof HadoopProcessStartedAck) {
                    HadoopProcess proc = runningProcsByProcId.get(desc.processId());

                    assert proc != null : "Missing child process for processId: " + desc;

                    HadoopProcessFuture fut = proc.initFut;

                    if (fut != null)
                        fut.onReplyReceived(desc);
                    // Safety.
                    else
                        log.warning("Failed to find process start future (will ignore): " + desc);
                }
                else if (msg instanceof HadoopTaskFinishedMessage) {
                    HadoopTaskFinishedMessage taskMsg = (HadoopTaskFinishedMessage)msg;

                    processTaskFinishedMessage(desc, taskMsg);
                }
                else
                    log.warning("Unexpected message received by node [desc=" + desc + ", msg=" + msg + ']');
            }
            finally {
                busyLock.readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void onConnectionLost(HadoopProcessDescriptor desc) {
            if (!busyLock.tryReadLock())
                return;

            try {
                if (desc == null) {
                    U.warn(log, "Handshake failed.");

                    return;
                }

                // Notify job tracker about failed tasks.
                HadoopProcess proc = runningProcsByProcId.get(desc.processId());

                if (proc != null) {
                    Collection<HadoopTaskInfo> tasks = proc.tasks();

                    if (!F.isEmpty(tasks)) {
                        log.warning("Lost connection with alive process (will terminate): " + desc);

                        HadoopTaskStatus status = new HadoopTaskStatus(CRASHED,
                            new IgniteCheckedException("Failed to run tasks (external process finished unexpectedly): " + desc));

                        for (HadoopTaskInfo info : tasks)
                            jobTracker.onTaskFinished(info, status);

                        runningProcsByJobId.remove(proc.jobId(), proc);
                    }

                    // Safety.
                    proc.terminate();
                }
            }
            finally {
                busyLock.readUnlock();
            }
        }
    }

    /**
     * Hadoop process.
     */
    private static class HadoopProcess extends ReentrantLock {
        /** */
        private static final long serialVersionUID = 0L;

        /** Job ID. */
        private final HadoopJobId jobId;

        /** Process. */
        private Process proc;

        /** Init future. Completes when process is ready to receive messages. */
        private final HadoopProcessFuture initFut;

        /** Process descriptor. */
        private HadoopProcessDescriptor procDesc;

        /** Reducers planned for this process. */
        private Collection<Integer> reducers;

        /** Tasks. */
        private final Collection<HadoopTaskInfo> tasks = new ConcurrentLinkedDeque8<>();

        /** Terminated flag. */
        private volatile boolean terminated;

        /**
         * @param jobId Job ID.
         * @param initFut Init future.
         */
        private HadoopProcess(HadoopJobId jobId, HadoopProcessFuture initFut,
            int[] reducers) {
            this.jobId = jobId;
            this.initFut = initFut;

            if (!F.isEmpty(reducers)) {
                this.reducers = new ArrayList<>(reducers.length);

                for (int r : reducers)
                    this.reducers.add(r);
            }
        }

        /**
         * @return Communication process descriptor.
         */
        private HadoopProcessDescriptor descriptor() {
            return procDesc;
        }

        /**
         * @return Job ID.
         */
        public HadoopJobId jobId() {
            return jobId;
        }

        /**
         * Initialized callback.
         *
         * @param proc Java process representation.
         * @param procDesc Process descriptor.
         */
        private void onInitialized(Process proc, HadoopProcessDescriptor procDesc) {
            this.proc = proc;
            this.procDesc = procDesc;
        }

        /**
         * Terminates process (kills it).
         */
        private void terminate() {
            // Guard against concurrent message sending.
            lock();

            try {
                terminated = true;

                if (!initFut.isDone())
                    initFut.listen(new CI1<IgniteInternalFuture<IgniteBiTuple<Process, HadoopProcessDescriptor>>>() {
                        @Override public void apply(
                            IgniteInternalFuture<IgniteBiTuple<Process, HadoopProcessDescriptor>> f) {
                            proc.destroy();
                        }
                    });
                else
                    proc.destroy();
            }
            finally {
                unlock();
            }
        }

        /**
         * @return Terminated flag.
         */
        private boolean terminated() {
            return terminated;
        }

        /**
         * Sets process tasks.
         *
         * @param tasks Tasks to set.
         */
        private void addTasks(Collection<HadoopTaskInfo> tasks) {
            this.tasks.addAll(tasks);
        }

        /**
         * Removes task when it was completed.
         *
         * @param task Task to remove.
         */
        private void removeTask(HadoopTaskInfo task) {
            if (tasks != null)
                tasks.remove(task);
        }

        /**
         * @return Collection of tasks.
         */
        private Collection<HadoopTaskInfo> tasks() {
            return tasks;
        }

        /**
         * @return Planned reducers.
         */
        private Collection<Integer> reducers() {
            return reducers;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(HadoopProcess.class, this);
        }
    }

    /**
     *
     */
    private class HadoopProcessFuture extends GridFutureAdapter<IgniteBiTuple<Process, HadoopProcessDescriptor>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Child process ID. */
        private UUID childProcId;

        /** Job ID. */
        private HadoopJobId jobId;

        /** Process descriptor. */
        private HadoopProcessDescriptor desc;

        /** Running process. */
        private Process proc;

        /** Process started flag. */
        private volatile boolean procStarted;

        /** Reply received flag. */
        private volatile boolean replyReceived;

        /** Logger. */
        private final IgniteLogger log = HadoopExternalTaskExecutor.this.log;

        /**
         */
        private HadoopProcessFuture(UUID childProcId, HadoopJobId jobId) {
            this.childProcId = childProcId;
            this.jobId = jobId;
        }

        /**
         * Process started callback.
         */
        public void onProcessStarted(Process proc) {
            this.proc = proc;

            procStarted = true;

            if (procStarted && replyReceived)
                onDone(F.t(proc, desc));
        }

        /**
         * Reply received callback.
         */
        public void onReplyReceived(HadoopProcessDescriptor desc) {
            assert childProcId.equals(desc.processId());

            this.desc = desc;

            replyReceived = true;

            if (procStarted && replyReceived)
                onDone(F.t(proc, desc));
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable IgniteBiTuple<Process, HadoopProcessDescriptor> res,
            @Nullable Throwable err) {
            if (err == null) {
                HadoopProcess proc = runningProcsByProcId.get(childProcId);

                assert proc != null;

                assert proc.initFut == this;

                proc.onInitialized(res.get1(), res.get2());

                if (!F.isEmpty(proc.reducers()))
                    jobTracker.onExternalMappersInitialized(jobId, proc.reducers(), desc);
            }
            else {
                // Clean up since init failed.
                runningProcsByJobId.remove(jobId);
                runningProcsByProcId.remove(childProcId);
            }

            if (super.onDone(res, err)) {
                if (err == null) {
                    if (log.isDebugEnabled())
                        log.debug("Initialized child process for external task execution [jobId=" + jobId +
                            ", desc=" + desc + ", initTime=" + duration() + ']');
                }
                else
                    U.error(log, "Failed to initialize child process for external task execution [jobId=" + jobId +
                        ", desc=" + desc + ']', err);

                return true;
            }

            return false;
        }
    }
}