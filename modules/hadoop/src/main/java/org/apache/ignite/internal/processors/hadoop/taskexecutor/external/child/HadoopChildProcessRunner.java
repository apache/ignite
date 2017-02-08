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

package org.apache.ignite.internal.processors.hadoop.taskexecutor.external.child;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.HadoopCommonUtils;
import org.apache.ignite.internal.processors.hadoop.HadoopHelperImpl;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskOutput;
import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.processors.hadoop.shuffle.HadoopShuffleAck;
import org.apache.ignite.internal.processors.hadoop.shuffle.HadoopShuffleJob;
import org.apache.ignite.internal.processors.hadoop.shuffle.HadoopShuffleMessage;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopExecutorService;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopRunnableTask;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopTaskState;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopTaskStatus;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.HadoopJobInfoUpdateRequest;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.HadoopPrepareForJobRequest;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.HadoopProcessDescriptor;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.HadoopProcessStartedAck;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.HadoopTaskExecutionRequest;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.HadoopTaskFinishedMessage;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.communication.HadoopExternalCommunication;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.communication.HadoopMessageListener;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.hadoop.HadoopTaskType.MAP;
import static org.apache.ignite.internal.processors.hadoop.HadoopTaskType.REDUCE;

/**
 * Hadoop process base.
 */
@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
public class HadoopChildProcessRunner {
    /** Node process descriptor. */
    private HadoopProcessDescriptor nodeDesc;

    /** Message processing executor service. */
    private ExecutorService msgExecSvc;

    /** Task executor service. */
    private HadoopExecutorService execSvc;

    /** */
    protected GridUnsafeMemory mem = new GridUnsafeMemory(0);

    /** External communication. */
    private HadoopExternalCommunication comm;

    /** Logger. */
    private IgniteLogger log;

    /** Init guard. */
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Start time. */
    private long startTime;

    /** Init future. */
    private final GridFutureAdapter<?> initFut = new GridFutureAdapter<>();

    /** Job instance. */
    private HadoopJob job;

    /** Number of uncompleted tasks. */
    private final AtomicInteger pendingTasks = new AtomicInteger();

    /** Shuffle job. */
    private HadoopShuffleJob<HadoopProcessDescriptor> shuffleJob;

    /** Concurrent mappers. */
    private int concMappers;

    /** Concurrent reducers. */
    private int concReducers;

    /**
     * Starts child process runner.
     */
    public void start(HadoopExternalCommunication comm, HadoopProcessDescriptor nodeDesc,
        ExecutorService msgExecSvc, IgniteLogger parentLog)
        throws IgniteCheckedException {
        this.comm = comm;
        this.nodeDesc = nodeDesc;
        this.msgExecSvc = msgExecSvc;

        comm.setListener(new MessageListener());
        log = parentLog.getLogger(HadoopChildProcessRunner.class);

        startTime = U.currentTimeMillis();

        // At this point node knows that this process has started.
        comm.sendMessage(this.nodeDesc, new HadoopProcessStartedAck());
    }

    /**
     * Initializes process for task execution.
     *
     * @param req Initialization request.
     */
    @SuppressWarnings("unchecked")
    private void prepareProcess(HadoopPrepareForJobRequest req) {
        if (initGuard.compareAndSet(false, true)) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Initializing external hadoop task: " + req);

                assert job == null;

                Class jobCls;

                try {
                    jobCls = Class.forName(HadoopCommonUtils.JOB_CLS_NAME);
                }
                catch (ClassNotFoundException e) {
                    throw new IgniteException("Failed to load job class: " + HadoopCommonUtils.JOB_CLS_NAME, e);
                }

                job = req.jobInfo().createJob(jobCls, req.jobId(), log, null, new HadoopHelperImpl());

                job.initialize(true, nodeDesc.processId());

                shuffleJob = new HadoopShuffleJob<>(comm.localProcessDescriptor(), log, job, mem,
                    req.totalReducerCount(), req.localReducers(), 0, false);

                initializeExecutors();

                if (log.isDebugEnabled())
                    log.debug("External process initialized [initWaitTime=" +
                        (U.currentTimeMillis() - startTime) + ']');

                initFut.onDone();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to initialize process: " + req, e);

                initFut.onDone(e);
            }
        }
        else
            log.warning("Duplicate initialize process request received (will ignore): " + req);
    }

    /**
     * @param req Task execution request.
     */
    private void runTasks(final HadoopTaskExecutionRequest req) {
        if (!initFut.isDone() && log.isDebugEnabled())
            log.debug("Will wait for process initialization future completion: " + req);

        initFut.listen(new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> f) {
                try {
                    // Make sure init was successful.
                    f.get();

                    boolean set = pendingTasks.compareAndSet(0, req.tasks().size());

                    assert set;

                    HadoopTaskInfo info = F.first(req.tasks());

                    assert info != null;

                    int size = info.type() == MAP ? concMappers : concReducers;

//                    execSvc.setCorePoolSize(size);
//                    execSvc.setMaximumPoolSize(size);

                    if (log.isDebugEnabled())
                        log.debug("Set executor service size for task type [type=" + info.type() +
                            ", size=" + size + ']');

                    for (HadoopTaskInfo taskInfo : req.tasks()) {
                        if (log.isDebugEnabled())
                            log.debug("Submitted task for external execution: " + taskInfo);

                        execSvc.submit(new HadoopRunnableTask(log, job, mem, taskInfo, nodeDesc.parentNodeId()) {
                            @Override protected void onTaskFinished(HadoopTaskStatus status) {
                                onTaskFinished0(this, status);
                            }

                            @Override protected HadoopTaskInput createInput(HadoopTaskContext ctx)
                                throws IgniteCheckedException {
                                return shuffleJob.input(ctx);
                            }

                            @Override protected HadoopTaskOutput createOutput(HadoopTaskContext ctx)
                                throws IgniteCheckedException {
                                return shuffleJob.output(ctx);
                            }
                        });
                    }
                }
                catch (IgniteCheckedException e) {
                    for (HadoopTaskInfo info : req.tasks())
                        notifyTaskFinished(info, new HadoopTaskStatus(HadoopTaskState.FAILED, e), false);
                }
            }
        });
    }

    /**
     * Creates executor services.
     *
     */
    private void initializeExecutors() {
        int cpus = Runtime.getRuntime().availableProcessors();

        execSvc = new HadoopExecutorService(log, "", cpus * 2, 1024);
    }

    /**
     * Updates external process map so that shuffle can proceed with sending messages to reducers.
     *
     * @param req Update request.
     */
    private void updateTasks(final HadoopJobInfoUpdateRequest req) {
        initFut.listen(new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> gridFut) {
                assert initGuard.get();

                assert req.jobId().equals(job.id());

                if (req.reducersAddresses() != null) {
                    if (shuffleJob.initializeReduceAddresses(req.reducersAddresses())) {
                        shuffleJob.startSending("external",
                            new IgniteInClosure2X<HadoopProcessDescriptor, HadoopMessage>() {
                                @Override public void applyx(HadoopProcessDescriptor dest, HadoopMessage msg)
                                    throws IgniteCheckedException {
                                    comm.sendMessage(dest, msg);
                                }
                            });
                    }
                }
            }
        });
    }

    /**
     * Stops all executors and running tasks.
     */
    private void shutdown() {
        if (execSvc != null)
            execSvc.shutdown(5000);

        if (msgExecSvc != null)
            msgExecSvc.shutdownNow();

        try {
            job.dispose(true);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to dispose job.", e);
        }
    }

    /**
     * Notifies node about task finish.
     *
     * @param run Finished task runnable.
     * @param status Task status.
     */
    private void onTaskFinished0(HadoopRunnableTask run, HadoopTaskStatus status) {
        HadoopTaskInfo info = run.taskInfo();

        int pendingTasks0 = pendingTasks.decrementAndGet();

        if (log.isDebugEnabled())
            log.debug("Hadoop task execution finished [info=" + info
                + ", state=" + status.state() + ", waitTime=" + run.waitTime() + ", execTime=" + run.executionTime() +
                ", pendingTasks=" + pendingTasks0 +
                ", err=" + status.failCause() + ']');

        assert info.type() == MAP || info.type() == REDUCE : "Only MAP or REDUCE tasks are supported.";

        boolean flush = pendingTasks0 == 0 && info.type() == MAP;

        notifyTaskFinished(info, status, flush);
    }

    /**
     * @param taskInfo Finished task info.
     * @param status Task status.
     */
    private void notifyTaskFinished(final HadoopTaskInfo taskInfo, final HadoopTaskStatus status,
        boolean flush) {

        final HadoopTaskState state = status.state();
        final Throwable err = status.failCause();

        if (!flush) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Sending notification to parent node [taskInfo=" + taskInfo + ", state=" + state +
                        ", err=" + err + ']');

                comm.sendMessage(nodeDesc, new HadoopTaskFinishedMessage(taskInfo, status));
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to send message to parent node (will terminate child process).", e);

                shutdown();

                terminate();
            }
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Flushing shuffle messages before sending last task completion notification [taskInfo=" +
                    taskInfo + ", state=" + state + ", err=" + err + ']');

            final long start = U.currentTimeMillis();

            try {
                shuffleJob.flush().listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> f) {
                        long end = U.currentTimeMillis();

                        if (log.isDebugEnabled())
                            log.debug("Finished flushing shuffle messages [taskInfo=" + taskInfo +
                                ", flushTime=" + (end - start) + ']');

                        try {
                            // Check for errors on shuffle.
                            f.get();

                            notifyTaskFinished(taskInfo, status, false);
                        }
                        catch (IgniteCheckedException e) {
                            log.error("Failed to flush shuffle messages (will fail the task) [taskInfo=" + taskInfo +
                                ", state=" + state + ", err=" + err + ']', e);

                            notifyTaskFinished(taskInfo,
                                new HadoopTaskStatus(HadoopTaskState.FAILED, e), false);
                        }
                    }
                });
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to flush shuffle messages (will fail the task) [taskInfo=" + taskInfo +
                    ", state=" + state + ", err=" + err + ']', e);

                notifyTaskFinished(taskInfo, new HadoopTaskStatus(HadoopTaskState.FAILED, e), false);
            }
        }
    }

    /**
     * Checks if message was received from parent node and prints warning if not.
     *
     * @param desc Sender process ID.
     * @param msg Received message.
     * @return {@code True} if received from parent node.
     */
    private boolean validateNodeMessage(HadoopProcessDescriptor desc, HadoopMessage msg) {
        if (!nodeDesc.processId().equals(desc.processId())) {
            log.warning("Received process control request from unknown process (will ignore) [desc=" + desc +
                ", msg=" + msg + ']');

            return false;
        }

        return true;
    }

    /**
     * Stops execution of this process.
     */
    private void terminate() {
        System.exit(1);
    }

    /**
     * Message listener.
     */
    private class MessageListener implements HadoopMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessageReceived(final HadoopProcessDescriptor desc, final HadoopMessage msg) {
            if (msg instanceof HadoopTaskExecutionRequest) {
                if (validateNodeMessage(desc, msg))
                    runTasks((HadoopTaskExecutionRequest)msg);
            }
            else if (msg instanceof HadoopJobInfoUpdateRequest) {
                if (validateNodeMessage(desc, msg))
                    updateTasks((HadoopJobInfoUpdateRequest)msg);
            }
            else if (msg instanceof HadoopPrepareForJobRequest) {
                if (validateNodeMessage(desc, msg))
                    prepareProcess((HadoopPrepareForJobRequest)msg);
            }
            else if (msg instanceof HadoopShuffleMessage) {
                if (log.isTraceEnabled())
                    log.trace("Received shuffle message [desc=" + desc + ", msg=" + msg + ']');

                initFut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> f) {
                        try {
                            HadoopShuffleMessage m = (HadoopShuffleMessage)msg;

                            shuffleJob.onShuffleMessage(desc, m);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to process hadoop shuffle message [desc=" + desc + ", msg=" + msg + ']', e);
                        }
                    }
                });
            }
            else if (msg instanceof HadoopShuffleAck) {
                if (log.isTraceEnabled())
                    log.trace("Received shuffle ack [desc=" + desc + ", msg=" + msg + ']');

                shuffleJob.onShuffleAck((HadoopShuffleAck)msg);
            }
            else
                log.warning("Unknown message received (will ignore) [desc=" + desc + ", msg=" + msg + ']');
        }

        /** {@inheritDoc} */
        @Override public void onConnectionLost(HadoopProcessDescriptor desc) {
            if (log.isDebugEnabled())
                log.debug("Lost connection with remote process: " + desc);

            if (desc == null)
                U.warn(log, "Handshake failed.");
            else if (desc.processId().equals(nodeDesc.processId())) {
                log.warning("Child process lost connection with parent node (will terminate child process).");

                shutdown();

                terminate();
            }
        }
    }
}
