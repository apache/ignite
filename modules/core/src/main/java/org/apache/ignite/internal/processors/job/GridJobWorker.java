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

package org.apache.ignite.internal.processors.job;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobMasterLeaveAware;
import org.apache.ignite.compute.ComputeUserUndeclaredException;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.igfs.IgfsOutOfSpaceException;
import org.apache.ignite.internal.GridInternalException;
import org.apache.ignite.internal.GridJobContextImpl;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.GridJobSessionImpl;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.service.GridServiceNotFoundException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_JOB_CANCELLED;
import static org.apache.ignite.events.EventType.EVT_JOB_FAILED;
import static org.apache.ignite.events.EventType.EVT_JOB_FINISHED;
import static org.apache.ignite.events.EventType.EVT_JOB_QUEUED;
import static org.apache.ignite.events.EventType.EVT_JOB_REJECTED;
import static org.apache.ignite.events.EventType.EVT_JOB_STARTED;
import static org.apache.ignite.events.EventType.EVT_JOB_TIMEDOUT;
import static org.apache.ignite.internal.GridTopic.TOPIC_JOB;
import static org.apache.ignite.internal.GridTopic.TOPIC_TASK;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.MANAGEMENT_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Job worker.
 */
public class GridJobWorker extends GridWorker implements GridTimeoutObject {
    /** Per-thread held flag. */
    private static final ThreadLocal<Boolean> HOLD = new ThreadLocal<Boolean>() {
        @Override protected Boolean initialValue() {
            return false;
        }
    };

    /** Static logger to avoid re-creation. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** */
    private final long createTime;

    /** */
    private volatile long startTime;

    /** */
    private volatile long finishTime;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final Object jobTopic;

    /** */
    private final Object taskTopic;

    /** */
    private byte[] jobBytes;

    /** Task originating node. */
    private final ClusterNode taskNode;

    /** Flag set when visor or internal task is running. */
    private final boolean internal;

    /** */
    private final IgniteLogger log;

    /** */
    private final Marshaller marsh;

    /** */
    private final GridJobSessionImpl ses;

    /** */
    private final GridJobContextImpl jobCtx;

    /** */
    private final GridJobEventListener evtLsnr;

    /** Deployment. */
    private final GridDeployment dep;

    /** */
    private final AtomicBoolean finishing = new AtomicBoolean();

    /** Guard ensuring that master-leave callback is not execute more than once. */
    private final AtomicBoolean masterLeaveGuard = new AtomicBoolean();

    /** */
    private volatile boolean timedOut;

    /** */
    private volatile boolean sysCancelled;

    /** */
    private volatile boolean sysStopping;

    /** */
    private volatile boolean isStarted;

    /** Deployed job. */
    private ComputeJob job;

    /** Halted flag (if greater than 0, job is halted). */
    private final AtomicInteger held = new AtomicInteger();

    /** Hold/unhold listener to notify job processor. */
    private final GridJobHoldListener holdLsnr;

    /** Partitions to reservations. */
    private final GridReservable partsReservation;

    /** Request topology version. */
    private final AffinityTopologyVersion reqTopVer;

    /**
     * @param ctx Kernal context.
     * @param dep Grid deployment.
     * @param createTime Create time.
     * @param ses Grid task session.
     * @param jobCtx Job context.
     * @param jobBytes Grid job bytes.
     * @param job Job.
     * @param taskNode Grid task node.
     * @param internal Whether or not task was marked with {@link GridInternal}
     * @param evtLsnr Job event listener.
     * @param holdLsnr Hold listener.
     * @param partsReservation Reserved partitions (must be released at the job finish).
     * @param reqTopVer Affinity topology version of the job request.
     */
    GridJobWorker(
        GridKernalContext ctx,
        GridDeployment dep,
        long createTime,
        GridJobSessionImpl ses,
        GridJobContextImpl jobCtx,
        byte[] jobBytes,
        ComputeJob job,
        ClusterNode taskNode,
        boolean internal,
        GridJobEventListener evtLsnr,
        GridJobHoldListener holdLsnr,
        GridReservable partsReservation,
        AffinityTopologyVersion reqTopVer) {
        super(ctx.gridName(), "grid-job-worker", ctx.log(GridJobWorker.class));

        assert ctx != null;
        assert ses != null;
        assert jobCtx != null;
        assert taskNode != null;
        assert evtLsnr != null;
        assert dep != null;
        assert holdLsnr != null;

        this.ctx = ctx;
        this.createTime = createTime;
        this.evtLsnr = evtLsnr;
        this.dep = dep;
        this.ses = ses;
        this.jobCtx = jobCtx;
        this.jobBytes = jobBytes;
        this.taskNode = taskNode;
        this.internal = internal;
        this.holdLsnr = holdLsnr;
        this.partsReservation = partsReservation;
        this.reqTopVer = reqTopVer;

        if (job != null)
            this.job = job;

        log = U.logger(ctx, logRef, this);

        marsh = ctx.config().getMarshaller();

        UUID locNodeId = ctx.discovery().localNode().id();

        jobTopic = TOPIC_JOB.topic(ses.getJobId(), locNodeId);
        taskTopic = TOPIC_TASK.topic(ses.getJobId(), locNodeId);
    }

    /**
     * Gets deployed job or {@code null} of job could not be deployed.
     *
     * @return Deployed job.
     */
    @Nullable public ComputeJob getJob() {
        return job;
    }

    /**
     * @return Deployed task.
     */
    public GridDeployment getDeployment() {
        return dep;
    }

    /**
     * Returns {@code True} if job was cancelled by the system.
     *
     * @return {@code True} if job was cancelled by the system.
     */
    boolean isSystemCanceled() {
        return sysCancelled;
    }

    /**
     * @return Create time.
     */
    long getCreateTime() {
        return createTime;
    }

    /**
     * @return Unique job ID.
     */
    public IgniteUuid getJobId() {
        IgniteUuid jobId = ses.getJobId();

        assert jobId != null;

        return jobId;
    }

    /**
     * @return Job context.
     */
    public ComputeJobContext getJobContext() {
        return jobCtx;
    }

    /**
     * @return Job communication topic.
     */
    Object getJobTopic() {
        return jobTopic;
    }

    /**
     * @return Task communication topic.
     */
    Object getTaskTopic() {
        return taskTopic;
    }

    /**
     * @return Session.
     */
    public GridJobSessionImpl getSession() {
        return ses;
    }

    /**
     * Gets job finishing state.
     *
     * @return {@code true} if job is being finished after execution
     *      and {@code false} otherwise.
     */
    boolean isFinishing() {
        return finishing.get();
    }

    /**
     * @return Parent task node ID.
     */
    ClusterNode getTaskNode() {
        return taskNode;
    }

    /**
     * @return Job execution time.
     */
    long getExecuteTime() {
        long startTime0 = startTime;
        long finishTime0 = finishTime;

        return startTime0 == 0 ? 0 : finishTime0 == 0 ?
            U.currentTimeMillis() - startTime0 : finishTime0 - startTime0;
    }

    /**
     * @return Time job spent on waiting queue.
     */
    long getQueuedTime() {
        long startTime0 = startTime;

        return startTime0 == 0 ? U.currentTimeMillis() - createTime : startTime0 - createTime;
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return ses.getEndTime();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid timeoutId() {
        IgniteUuid jobId = ses.getJobId();

        assert jobId != null;

        return jobId;
    }

    /**
     * @return {@code True} if job is timed out.
     */
    boolean isTimedOut() {
        return timedOut;
    }

    /**
     * @return {@code True} if parent task is internal or Visor-related.
     */
    public boolean isInternal() {
        return internal;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        if (finishing.get())
            return;

        timedOut = true;

        U.warn(log, "Job has timed out: " + ses);

        cancel();

        if (!internal && ctx.event().isRecordable(EVT_JOB_TIMEDOUT))
            recordEvent(EVT_JOB_TIMEDOUT, "Job has timed out: " + job);
    }

    /**
     * Callback for whenever grid is stopping.
     */
    public void onStopping() {
        sysStopping = true;
    }

    /**
     * @return {@code True} if job was halted.
     */
    public boolean held() {
        return held.get() > 0;
    }

    /**
     * Sets halt flags.
     */
    public boolean hold() {
        HOLD.set(true);

        boolean res;

        if (res = holdLsnr.onHeld(this))
            held.incrementAndGet();

        return res;
    }

    /**
     * Initializes job. Handles deployments and event recording.
     *
     * @param dep Job deployed task.
     * @param taskCls Task class.
     * @return {@code True} if job was successfully initialized.
     */
    boolean initialize(GridDeployment dep, Class<?> taskCls) {
        assert dep != null;

        IgniteException ex = null;

        try {
            if (job == null) {
                MarshallerUtils.jobSenderVersion(taskNode.version());

                try {
                    job = U.unmarshal(marsh, jobBytes, U.resolveClassLoader(dep.classLoader(), ctx.config()));
                }
                finally {
                    MarshallerUtils.jobSenderVersion(null);
                }

                // No need to hold reference any more.
                jobBytes = null;
            }

            // Inject resources.
            ctx.resource().inject(dep, taskCls, job, ses, jobCtx);

            if (!internal && ctx.event().isRecordable(EVT_JOB_QUEUED))
                recordEvent(EVT_JOB_QUEUED, "Job got queued for computation.");
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to initialize job [jobId=" + ses.getJobId() + ", ses=" + ses + ']', e);

            ex = new IgniteException(e);
        }
        catch (Throwable e) {
            ex = handleThrowable(e);

            assert ex != null;

            if (e instanceof Error)
                throw e;
        }
        finally {
            if (ex != null)
                finishJob(null, ex, true);
        }

        return ex == null;
    }

    /** {@inheritDoc} */
    @Override protected void body() {
        assert job != null;

        startTime = U.currentTimeMillis();

        isStarted = true;

        // Event notification.
        evtLsnr.onJobStarted(this);

        if (!internal && ctx.event().isRecordable(EVT_JOB_STARTED))
            recordEvent(EVT_JOB_STARTED, /*no message for success*/null);

        execute0(true);
    }

    /**
     * Executes the job.
     */
    public void execute() {
        execute0(false);
    }

    /**
     * @param skipNtf {@code True} to skip job processor {@code onUnheld()}
     *      notification (only from {@link #body()}).
     */
    private void execute0(boolean skipNtf) {
        // Make sure flag is not set for current thread.
        HOLD.set(false);

        try {
            if (partsReservation != null) {
                try {
                    if (!partsReservation.reserve()) {
                        finishJob(null, null, true, true);

                        return;
                    }
                }
                catch (Exception e) {
                    IgniteException ex = new IgniteException("Failed to lock partitions " +
                        "[jobId=" + ses.getJobId() + ", ses=" + ses + ']', e);

                    U.error(log, "Failed to lock partitions [jobId=" + ses.getJobId() + ", ses=" + ses + ']', e);;

                    finishJob(null, ex, true);

                    return;
                }
            }

            if (isCancelled())
                // If job was cancelled prior to assigning runner to it?
                super.cancel();

            if (!skipNtf) {
                if (holdLsnr.onUnheld(this))
                    held.decrementAndGet();
                else {
                    if (log.isDebugEnabled())
                        log.debug("Ignoring job execution (job was not held).");

                    return;
                }
            }

            boolean sndRes = true;

            Object res = null;

            IgniteException ex = null;

            try {
                ctx.job().currentTaskSession(ses);

                if (reqTopVer != null)
                    GridQueryProcessor.setRequestAffinityTopologyVersion(reqTopVer);

                // If job has timed out, then
                // avoid computation altogether.
                if (isTimedOut())
                    sndRes = false;
                else {
                    res = U.wrapThreadLoader(dep.classLoader(), new Callable<Object>() {
                        @Nullable @Override public Object call() {
                            try {
                                if (internal && ctx.config().isPeerClassLoadingEnabled())
                                    ctx.job().internal(true);

                                return job.execute();
                            }
                            finally {
                                if (internal && ctx.config().isPeerClassLoadingEnabled())
                                    ctx.job().internal(false);
                            }
                        }
                    });

                    if (log.isDebugEnabled()) {
                        log.debug(S.toString("Job execution has successfully finished",
                            "job", job, false,
                            "res", res, true));
                    }
                }
            }
            catch (IgniteException e) {
                if (sysStopping && e.hasCause(IgniteInterruptedCheckedException.class, InterruptedException.class)) {
                    ex = handleThrowable(e);

                    assert ex != null;
                }
                else {
                    if (X.hasCause(e, GridInternalException.class) || X.hasCause(e, IgfsOutOfSpaceException.class)) {
                        // Print exception for internal errors only if debug is enabled.
                        if (log.isDebugEnabled())
                            U.error(log, "Failed to execute job [jobId=" + ses.getJobId() + ", ses=" + ses + ']', e);
                    }
                    else if (X.hasCause(e, InterruptedException.class)) {
                        String msg = "Job was cancelled [jobId=" + ses.getJobId() + ", ses=" + ses + ']';

                        if (log.isDebugEnabled())
                            U.error(log, msg, e);
                        else
                            U.warn(log, msg);
                    }
                    else if (X.hasCause(e, GridServiceNotFoundException.class) ||
                        X.hasCause(e, ClusterTopologyCheckedException.class))
                        // Should be throttled, because GridServiceProxy continuously retry getting service.
                        LT.error(log, e, "Failed to execute job [jobId=" + ses.getJobId() + ", ses=" + ses + ']');
                    else
                        U.error(log, "Failed to execute job [jobId=" + ses.getJobId() + ", ses=" + ses + ']', e);

                    ex = e;
                }
            }
            // Catch Throwable to protect against bad user code except
            // InterruptedException if job is being cancelled.
            catch (Throwable e) {
                ex = handleThrowable(e);

                assert ex != null;

                if (e instanceof Error)
                    throw (Error)e;
            }
            finally {
                // Finish here only if not held by this thread.
                if (!HOLD.get())
                    finishJob(res, ex, sndRes);
                else
                    // Make sure flag is not set for current thread.
                    // This may happen in case of nested internal task call with continuation.
                    HOLD.set(false);

                ctx.job().currentTaskSession(null);

                if (reqTopVer != null)
                    GridQueryProcessor.setRequestAffinityTopologyVersion(null);
            }
        }
        finally {
            if (partsReservation != null)
                partsReservation.release();
        }
    }

    /**
     * Handles {@link Throwable} generic exception for task
     * deployment and execution.
     *
     * @param e Exception.
     * @return Wrapped exception.
     */
    private IgniteException handleThrowable(Throwable e) {
        String msg = null;

        IgniteException ex = null;

        // Special handling for weird interrupted exception which
        // happens due to JDk 1.5 bug.
        if (e instanceof InterruptedException && !sysStopping) {
            msg = "Failed to execute job due to interrupted exception.";

            // Turn interrupted exception into checked exception.
            ex = new IgniteException(msg, e);
        }
        // Special 'NoClassDefFoundError' handling if P2P is on. We had many questions
        // about this exception and decided to change error message.
        else if ((e instanceof NoClassDefFoundError || e instanceof ClassNotFoundException)
            && ctx.config().isPeerClassLoadingEnabled()) {
            msg = "Failed to execute job due to class or resource loading exception (make sure that task " +
                "originating node is still in grid and requested class is in the task class path) [jobId=" +
                ses.getJobId() + ", ses=" + ses + ']';

            ex = new ComputeUserUndeclaredException(msg, e);
        }
        else if (sysStopping && X.hasCause(e, InterruptedException.class, IgniteInterruptedCheckedException.class)) {
            msg = "Job got interrupted due to system stop (will attempt failover).";

            ex = new ComputeExecutionRejectedException(e);
        }

        if (msg == null) {
            msg = "Failed to execute job due to unexpected runtime exception [jobId=" + ses.getJobId() +
                ", ses=" + ses + ']';

            ex = new ComputeUserUndeclaredException(msg, e);
        }

        assert msg != null;
        assert ex != null;

        U.error(log, msg, e);

        return ex;
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        cancel(false);
    }

    /**
     * @param sys System flag.
     */
    public void cancel(boolean sys) {
        try {
            super.cancel();

            final ComputeJob job0 = job;

            if (sys)
                sysCancelled = true;

            if (job0 != null) {
                if (log.isDebugEnabled())
                    log.debug("Cancelling job: " + ses);

                U.wrapThreadLoader(dep.classLoader(), new IgniteRunnable() {
                    @Override public void run() {
                        job0.cancel();
                    }
                });
            }

            if (!internal && ctx.event().isRecordable(EVT_JOB_CANCELLED))
                recordEvent(EVT_JOB_CANCELLED, "Job was cancelled: " + job0);
        }
        // Catch throwable to protect against bad user code.
        catch (Throwable e) {
            U.error(log, "Failed to cancel job due to undeclared user exception [jobId=" + ses.getJobId() +
                ", ses=" + ses + ']', e);

            if (e instanceof Error)
                throw e;
        }
    }

    /**
     * @param evtType Event type.
     * @param msg Message.
     */
    private void recordEvent(int evtType, @Nullable String msg) {
        assert ctx.event().isRecordable(evtType);
        assert !internal;

        JobEvent evt = new JobEvent();

        evt.jobId(ses.getJobId());
        evt.message(msg);
        evt.node(ctx.discovery().localNode());
        evt.taskName(ses.getTaskName());
        evt.taskClassName(ses.getTaskClassName());
        evt.taskSessionId(ses.getId());
        evt.type(evtType);
        evt.taskNode(taskNode);
        evt.taskSubjectId(ses.subjectId());

        ctx.event().record(evt);
    }

    /**
     * @param res Result.
     * @param ex Error.
     * @param sndReply If {@code true}, reply will be sent.
     */
    void finishJob(@Nullable Object res,
        @Nullable IgniteException ex,
        boolean sndReply) {
        finishJob(res, ex, sndReply, false);
    }

    /**
     * @param res Resuilt.
     * @param ex Exception
     * @param sndReply If {@code true}, reply will be sent.
     * @param retry If {@code true}, retry response will be sent.
     */
    void finishJob(@Nullable Object res,
        @Nullable IgniteException ex,
        boolean sndReply,
        boolean retry)
    {
        // Avoid finishing a job more than once from different threads.
        if (!finishing.compareAndSet(false, true))
            return;

        // Do not send reply if job has been cancelled from system.
        if (sndReply)
            sndReply = !sysCancelled;

        // We should save message ID here since listener callback will reset sequence.
        ClusterNode sndNode = ctx.discovery().node(taskNode.id());

        finishTime = U.currentTimeMillis();

        Collection<IgniteBiTuple<Integer, String>> evts = null;

        try {
            if (ses.isFullSupport())
                evtLsnr.onBeforeJobResponseSent(this);

            // Send response back only if job has not timed out.
            if (!isTimedOut()) {
                if (sndReply) {
                    if (sndNode == null) {
                        onMasterNodeLeft();

                        U.warn(log, "Failed to reply to sender node because it left grid [nodeId=" + taskNode.id() +
                            ", ses=" + ses + ", jobId=" + ses.getJobId() + ", job=" + job + ']');

                        // Record job reply failure.
                        if (!internal && ctx.event().isRecordable(EVT_JOB_FAILED))
                            evts = addEvent(evts, EVT_JOB_FAILED, "Job reply failed (task node left grid): " + job);
                    }
                    else {
                        try {
                            byte[] resBytes = null;
                            byte[] exBytes = null;
                            byte[] attrBytes = null;

                            boolean loc = ctx.localNodeId().equals(sndNode.id()) && !ctx.config().isMarshalLocalJobs();

                            Map<Object, Object> attrs = jobCtx.getAttributes();

                            // Try serialize response, and if exception - return to client.
                            if (!loc) {
                                try {
                                    resBytes = U.marshal(marsh, res);
                                }
                                catch (IgniteCheckedException e) {
                                    resBytes = U.marshal(marsh, null);

                                    if (ex != null)
                                        ex.addSuppressed(e);
                                    else
                                        ex = U.convertException(e);

                                    U.error(log, "Failed to serialize job response [nodeId=" + taskNode.id() +
                                        ", ses=" + ses + ", jobId=" + ses.getJobId() + ", job=" + job +
                                        ", resCls=" + (res == null ? null : res.getClass()) + ']', e);
                                }

                                try {
                                    attrBytes = U.marshal(marsh, attrs);
                                }
                                catch (IgniteCheckedException e) {
                                    attrBytes = U.marshal(marsh, Collections.emptyMap());

                                    if (ex != null)
                                        ex.addSuppressed(e);
                                    else
                                        ex = U.convertException(e);

                                    U.error(log, "Failed to serialize job attributes [nodeId=" + taskNode.id() +
                                        ", ses=" + ses + ", jobId=" + ses.getJobId() + ", job=" + job +
                                        ", attrs=" + attrs + ']', e);
                                }

                                try {
                                    exBytes = U.marshal(marsh, ex);
                                }
                                catch (IgniteCheckedException e) {
                                    String msg = "Failed to serialize job exception [nodeId=" + taskNode.id() +
                                        ", ses=" + ses + ", jobId=" + ses.getJobId() + ", job=" + job +
                                        ", msg=\"" + e.getMessage() + "\"]";

                                    ex = new IgniteException(msg);

                                    U.error(log, msg, e);

                                    exBytes = U.marshal(marsh, ex);
                                }
                            }

                            if (ex != null) {
                                if (isStarted) {
                                    // Job failed.
                                    if (!internal && ctx.event().isRecordable(EVT_JOB_FAILED))
                                        evts = addEvent(evts, EVT_JOB_FAILED, "Job failed due to exception [ex=" +
                                            ex + ", job=" + job + ']');
                                }
                                else if (!internal && ctx.event().isRecordable(EVT_JOB_REJECTED))
                                    evts = addEvent(evts, EVT_JOB_REJECTED, "Job has not been started " +
                                        "[ex=" + ex + ", job=" + job + ']');
                            }
                            else if (!internal && ctx.event().isRecordable(EVT_JOB_FINISHED))
                                evts = addEvent(evts, EVT_JOB_FINISHED, /*no message for success. */null);

                            GridJobExecuteResponse jobRes = new GridJobExecuteResponse(
                                ctx.localNodeId(),
                                ses.getId(),
                                ses.getJobId(),
                                exBytes,
                                loc ? ex : null,
                                resBytes,
                                loc ? res : null,
                                attrBytes,
                                loc ? attrs : null,
                                isCancelled(),
                                retry ? ctx.cache().context().exchange().readyAffinityVersion() : null);

                            long timeout = ses.getEndTime() - U.currentTimeMillis();

                            if (timeout <= 0)
                                // Ignore the actual timeout and send response anyway.
                                timeout = 1;

                            if (ses.isFullSupport()) {
                                // Send response to designated job topic.
                                // Always go through communication to preserve order,
                                // if attributes are enabled.
                                ctx.io().sendOrderedMessage(
                                    sndNode,
                                    taskTopic,
                                    jobRes,
                                    internal ? MANAGEMENT_POOL : SYSTEM_POOL,
                                    timeout,
                                    false);
                            }
                            else if (ctx.localNodeId().equals(sndNode.id()))
                                ctx.task().processJobExecuteResponse(ctx.localNodeId(), jobRes);
                            else
                                // Send response to common topic as unordered message.
                                ctx.io().send(sndNode, TOPIC_TASK, jobRes, internal ? MANAGEMENT_POOL : SYSTEM_POOL);
                        }
                        catch (IgniteCheckedException e) {
                            // Log and invoke the master-leave callback.
                            if (isDeadNode(taskNode.id())) {
                                onMasterNodeLeft();

                                // Avoid stack trace for left nodes.
                                U.warn(log, "Failed to reply to sender node because it left grid " +
                                    "[nodeId=" + taskNode.id() + ", jobId=" + ses.getJobId() +
                                    ", ses=" + ses + ", job=" + job + ']');
                            }
                            else
                                U.error(log, "Error sending reply for job [nodeId=" + sndNode.id() + ", jobId=" +
                                    ses.getJobId() + ", ses=" + ses + ", job=" + job + ']', e);

                            if (!internal && ctx.event().isRecordable(EVT_JOB_FAILED))
                                evts = addEvent(evts, EVT_JOB_FAILED, "Failed to send reply for job [nodeId=" +
                                    taskNode.id() + ", job=" + job + ']');
                        }
                        // Catching interrupted exception because
                        // it gets thrown for some reason.
                        catch (Exception e) {
                            String msg = "Failed to send reply for job [nodeId=" + taskNode.id() + ", job=" + job + ']';

                            U.error(log, msg, e);

                            if (!internal && ctx.event().isRecordable(EVT_JOB_FAILED))
                                evts = addEvent(evts, EVT_JOB_FAILED, msg);
                        }
                    }
                }
                else {
                    if (ex != null) {
                        if (isStarted) {
                            if (!internal && ctx.event().isRecordable(EVT_JOB_FAILED))
                                evts = addEvent(evts, EVT_JOB_FAILED, "Job failed due to exception [ex=" + ex +
                                    ", job=" + job + ']');
                        }
                        else if (!internal && ctx.event().isRecordable(EVT_JOB_REJECTED))
                            evts = addEvent(evts, EVT_JOB_REJECTED, "Job has not been started [ex=" + ex +
                                ", job=" + job + ']');
                    }
                    else if (!internal && ctx.event().isRecordable(EVT_JOB_FINISHED))
                        evts = addEvent(evts, EVT_JOB_FINISHED, /*no message for success. */null);
                }
            }
            // Job timed out.
            else if (!internal && ctx.event().isRecordable(EVT_JOB_FAILED))
                evts = addEvent(evts, EVT_JOB_FAILED, "Job failed due to timeout: " + job);
        }
        finally {
            if (evts != null) {
                for (IgniteBiTuple<Integer, String> t : evts)
                    recordEvent(t.get1(), t.get2());
            }

            // Listener callback.
            evtLsnr.onJobFinished(this);
        }
    }

    /**
     * If the job implements {@link org.apache.ignite.compute.ComputeJobMasterLeaveAware#onMasterNodeLeft} interface then invoke
     * {@link org.apache.ignite.compute.ComputeJobMasterLeaveAware#onMasterNodeLeft(org.apache.ignite.compute.ComputeTaskSession)} method.
     *
     * @return {@code True} if master leave has been handled (either by this call or before).
     */
    boolean onMasterNodeLeft() {
        if (job instanceof ComputeJobMasterLeaveAware) {
            if (masterLeaveGuard.compareAndSet(false, true)) {
                try {
                    ((ComputeJobMasterLeaveAware)job).onMasterNodeLeft(ses.session());

                    if (log.isDebugEnabled())
                        log.debug("Successfully executed ComputeJobMasterLeaveAware.onMasterNodeLeft() callback " +
                            "[nodeId=" + taskNode.id() + ", jobId=" + ses.getJobId() + ", job=" + job + ']');
                }
                catch (Exception e) {
                    U.error(log, "Failed to execute ComputeJobMasterLeaveAware.onMasterNodeLeft() callback " +
                        "[nodeId=" + taskNode.id() + ", jobId=" + ses.getJobId() + ", job=" + job + ']', e);
                }
            }

            return true;
        }

        return false;
    }

    /**
     * @param evts Collection (created if {@code null}).
     * @param evt Event.
     * @param msg Message (optional).
     * @return Collection with event added.
     */
    Collection<IgniteBiTuple<Integer, String>> addEvent(@Nullable Collection<IgniteBiTuple<Integer, String>> evts,
        Integer evt, @Nullable String msg) {
        assert ctx.event().isRecordable(evt);
        assert !internal;

        if (evts == null)
            evts = new ArrayList<>();

        evts.add(F.t(evt, msg));

        return evts;
    }

    /**
     * Checks whether node is alive or dead.
     *
     * @param uid UID of node to check.
     * @return {@code true} if node is dead, {@code false} is node is alive.
     */
    private boolean isDeadNode(UUID uid) {
        return ctx.discovery().node(uid) == null || !ctx.discovery().pingNodeNoError(uid);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null)
            return false;

        assert obj instanceof GridJobWorker;

        IgniteUuid jobId1 = ses.getJobId();
        IgniteUuid jobId2 = ((GridJobWorker)obj).ses.getJobId();

        assert jobId1 != null;
        assert jobId2 != null;

        return jobId1.equals(jobId2);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        IgniteUuid jobId = ses.getJobId();

        assert jobId != null;

        return jobId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobWorker.class, this);
    }
}
