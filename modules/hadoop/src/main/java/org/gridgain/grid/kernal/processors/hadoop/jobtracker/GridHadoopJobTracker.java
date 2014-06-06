/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.hadoop.GridHadoopJobProperty.*;
import static org.gridgain.grid.hadoop.GridHadoopTaskType.*;
import static org.gridgain.grid.hadoop.GridHadoopJobPhase.*;
import static org.gridgain.grid.kernal.processors.hadoop.taskexecutor.GridHadoopTaskState.*;

/**
 * Hadoop job tracker.
 */
public class GridHadoopJobTracker extends GridHadoopComponent {
    /** System cache. */
    private GridCacheProjection<GridHadoopJobId, GridHadoopJobMetadata> jobMetaPrj;

    /** Map-reduce execution planner. */
    private GridHadoopMapReducePlanner mrPlanner;

    /** Locally active jobs. */
    private ConcurrentMap<GridHadoopJobId, JobLocalState> activeJobs = new ConcurrentHashMap8<>();

    /** Locally requested finish futures. */
    private ConcurrentMap<GridHadoopJobId, GridFutureAdapter<GridHadoopJobId>> activeFinishFuts =
        new ConcurrentHashMap8<>();

    /** Event processing service. */
    private ExecutorService evtProcSvc;

    /** Component busy lock. */
    private GridSpinReadWriteLock busyLock;

    /** Closure to check result of async transform of system cache. */
    private CI1<GridFuture<?>> failsLogger = new CI1<GridFuture<?>>() {
        @Override public void apply(GridFuture<?> gridFuture) {
            try {
                gridFuture.get();
            }
            catch (GridException e) {
                U.error(log, "Failed to transform system cache.", e);
            }
        }
    };

    /** {@inheritDoc} */
    @Override public void start(GridHadoopContext ctx) throws GridException {
        super.start(ctx);

        busyLock = new GridSpinReadWriteLock();

        evtProcSvc = Executors.newFixedThreadPool(1);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        GridCache<Object, Object> sysCache = ctx.kernalContext().cache().cache(CU.SYS_CACHE_HADOOP_MR);

        assert sysCache != null;

        mrPlanner = ctx.planner();

        ctx.kernalContext().resource().injectGeneric(mrPlanner);

        jobMetaPrj = sysCache.projection(GridHadoopJobId.class, GridHadoopJobMetadata.class);

        GridCacheContinuousQuery<GridHadoopJobId, GridHadoopJobMetadata> qry = jobMetaPrj.queries()
            .createContinuousQuery();

        qry.callback(new GridBiPredicate<UUID,
            Collection<Map.Entry<GridHadoopJobId, GridHadoopJobMetadata>>>() {
            @Override public boolean apply(UUID nodeId,
                final Collection<Map.Entry<GridHadoopJobId, GridHadoopJobMetadata>> evts) {
                if (!busyLock.tryReadLock())
                    return false;

                try {
                    // Must process query callback in a separate thread to avoid deadlocks.
                    evtProcSvc.submit(new EventHandler() {
                        @Override protected void body() {
                            processJobMetadata(evts);
                        }
                    });

                    return true;
                }
                finally {
                    busyLock.readUnlock();
                }
            }
        });

        qry.execute();

        ctx.kernalContext().event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(final GridEvent evt) {
                if (!busyLock.tryReadLock())
                    return;

                try {
                    // Must process discovery callback in a separate thread to avoid deadlock.
                    evtProcSvc.submit(new EventHandler() {
                        @Override protected void body() {
                            processNodeLeft((GridDiscoveryEvent)evt);
                        }
                    });
                }
                finally {
                    busyLock.readUnlock();
                }
            }
        }, GridEventType.EVT_NODE_FAILED, GridEventType.EVT_NODE_LEFT);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        super.onKernalStop(cancel);

        busyLock.writeLock();

        evtProcSvc.shutdown();

        // Fail all pending futures.
        for (GridFutureAdapter<GridHadoopJobId> fut : activeFinishFuts.values())
            fut.onDone(new GridException("Failed to execute Hadoop map-reduce job (grid is stopping)."));
    }

    /**
     * Submits execution of Hadoop job to grid.
     *
     * @param jobId Job ID.
     * @param info Job info.
     * @return Job completion future.
     */
    @SuppressWarnings("unchecked")
    public GridFuture<GridHadoopJobId> submit(GridHadoopJobId jobId, GridHadoopJobInfo info) {
        if (!busyLock.tryReadLock()) {
            return new GridFinishedFutureEx<>(new GridException("Failed to execute map-reduce job " +
                "(grid is stopping): " + info));
        }

        try {
            GridHadoopJob job = ctx.jobFactory().createJob(jobId, info);

            Collection<GridHadoopInputSplit> splits = job.input();

            GridHadoopMapReducePlan mrPlan = mrPlanner.preparePlan(splits, ctx.nodes(), job, null);

            GridHadoopJobMetadata meta = new GridHadoopJobMetadata(jobId, info);

            meta.mapReducePlan(mrPlan);

            meta.pendingSplits(allSplits(mrPlan));
            meta.pendingReducers(allReducers(job));

            GridFutureAdapter<GridHadoopJobId> completeFut = new GridFutureAdapter<>();

            GridFutureAdapter<GridHadoopJobId> old = activeFinishFuts.put(jobId, completeFut);

            assert old == null : "Duplicate completion future [jobId=" + jobId + ", old=" + old + ']';

            if (log.isDebugEnabled())
                log.debug("Submitting job metadata [jobId=" + jobId + ", meta=" + meta + ']');

            jobMetaPrj.put(jobId, meta);

            return completeFut;
        }
        catch (GridException e) {
            U.error(log, "Failed to submit job: " + jobId, e);

            return new GridFinishedFutureEx<>(e);
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /**
     * Gets hadoop job status for given job ID.
     *
     * @param jobId Job ID to get status for.
     * @return Job status for given job ID or {@code null} if job was not found.
     */
    @Nullable public GridHadoopJobStatus status(GridHadoopJobId jobId) throws GridException {
        if (!busyLock.tryReadLock())
            return null; // Grid is stopping.

        try {
            GridHadoopJobMetadata meta = jobMetaPrj.get(jobId);

            return meta != null ? GridHadoopUtils.status(meta) : null;
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /**
     * Gets job finish future.
     *
     * @param jobId Job ID.
     * @return Finish future or {@code null}.
     * @throws GridException If failed.
     */
    @Nullable public GridFuture<?> finishFuture(GridHadoopJobId jobId) throws GridException {
        if (!busyLock.tryReadLock())
            return null; // Grid is stopping.

        try {
            GridHadoopJobMetadata meta = jobMetaPrj.get(jobId);

            if (meta == null)
                return null;

            if (log.isTraceEnabled())
                log.trace("Got job metadata for status check [locNodeId=" + ctx.localNodeId() + ", meta=" + meta + ']');

            if (meta.phase() == PHASE_COMPLETE) {
                if (log.isTraceEnabled())
                    log.trace("Job is complete, returning finished future: " + jobId);

                return new GridFinishedFutureEx<>(jobId, meta.failCause());
            }

            GridFutureAdapter<GridHadoopJobId> fut = F.addIfAbsent(activeFinishFuts, jobId,
                new GridFutureAdapter<GridHadoopJobId>());

            // Get meta from cache one more time to close the window.
            meta = jobMetaPrj.get(jobId);

            if (log.isTraceEnabled())
                log.trace("Re-checking job metadata [locNodeId=" + ctx.localNodeId() + ", meta=" + meta + ']');

            if (meta == null) {
                fut.onDone();

                activeFinishFuts.remove(jobId , fut);
            }
            else if (meta.phase() == PHASE_COMPLETE) {
                fut.onDone(jobId, meta.failCause());

                activeFinishFuts.remove(jobId , fut);
            }

            return fut;
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /**
     * Gets job plan by job ID.
     *
     * @param jobId Job ID.
     * @return Job plan.
     * @throws GridException If failed.
     */
    public GridHadoopMapReducePlan plan(GridHadoopJobId jobId) throws GridException {
        if (!busyLock.tryReadLock())
            return null;

        try {
            GridHadoopJobMetadata meta = jobMetaPrj.get(jobId);

            if (meta != null)
                return meta.mapReducePlan();

            return null;
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /**
     * Callback from task executor invoked when a task has been finished.
     *
     * @param info Task info.
     * @param status Task status.
     */
    @SuppressWarnings({"ConstantConditions", "ThrowableResultOfMethodCallIgnored"})
    public void onTaskFinished(GridHadoopTaskInfo info, GridHadoopTaskStatus status) {
        if (!busyLock.tryReadLock())
            return;

        try {
            assert status.state() != RUNNING;

            if (log.isDebugEnabled())
                log.debug("Received task finished callback [info=" + info + ", status=" + status + ']');

            JobLocalState state = activeJobs.get(info.jobId());

            // Task CRASHes with null fail cause.
            assert (status.state() != FAILED) || status.failCause() != null :
                "Invalid task status [info=" + info + ", status=" + status + ']';

            assert state != null || (ctx.jobUpdateLeader() && (info.type() == COMMIT || info.type() == ABORT)):
                "Missing local state for finished task [info=" + info + ", status=" + status + ']';

            switch (info.type()) {
                case MAP: {
                    state.onMapFinished(info, status);

                    break;
                }

                case REDUCE: {
                    state.onReduceFinished(info, status);

                    break;
                }

                case COMBINE: {
                    state.onCombineFinished(info, status);

                    break;
                }

                case COMMIT:
                case ABORT: {
                    GridCacheEntry<GridHadoopJobId, GridHadoopJobMetadata> entry = jobMetaPrj.entry(info.jobId());

                    entry.timeToLive(ctx.configuration().getFinishedJobInfoTtl());

                    entry.transformAsync(new UpdatePhaseClosure(PHASE_COMPLETE)).listenAsync(failsLogger);

                    break;
                }
            }
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /**
     * @param jobId Job id.
     * @param c Closure of operation.
     */
    private void transform(GridHadoopJobId jobId, GridClosure<GridHadoopJobMetadata, GridHadoopJobMetadata> c) {
        jobMetaPrj.transformAsync(jobId, c).listenAsync(failsLogger);
    }

    /**
     * Callback from task executor called when process is ready to received shuffle messages.
     *
     * @param jobId Job ID.
     * @param reducers Reducers.
     * @param desc Process descriptor.
     */
    public void onExternalMappersInitialized(GridHadoopJobId jobId, Collection<Integer> reducers,
        GridHadoopProcessDescriptor desc) {
        transform(jobId, new InitializeReducersClosure(reducers, desc));
    }

    /**
     * Gets all input splits for given hadoop map-reduce plan.
     *
     * @param plan Map-reduce plan.
     * @return Collection of all input splits that should be processed.
     */
    @SuppressWarnings("ConstantConditions")
    private Collection<GridHadoopInputSplit> allSplits(GridHadoopMapReducePlan plan) {
        Collection<GridHadoopInputSplit> res = new HashSet<>();

        for (UUID nodeId : plan.mapperNodeIds())
            res.addAll(plan.mappers(nodeId));

        return res;
    }

    /**
     * Gets all reducers for this job.
     *
     * @param job Job to get reducers for.
     * @return Collection of reducers.
     */
    private Collection<Integer> allReducers(GridHadoopJob job) {
        Collection<Integer> res = new HashSet<>();

        for (int i = 0; i < job.reducers(); i++)
            res.add(i);

        return res;
    }

    /**
     * Processes node leave (oro fail) event.
     *
     * @param evt Discovery event.
     */
    @SuppressWarnings("ConstantConditions")
    private void processNodeLeft(GridDiscoveryEvent evt) {
        if (log.isDebugEnabled())
            log.debug("Processing discovery event [locNodeId=" + ctx.localNodeId() + ", evt=" + evt + ']');

        ctx.localNodeId()

//        evt.eventNode().order()

        // Check only if this node is responsible for job status updates.
        if (ctx.jobUpdateLeader()) {
            // Iteration over all local entries is correct since system cache is REPLICATED.
            for (GridHadoopJobMetadata meta : jobMetaPrj.values()) {
                try {
                    GridHadoopMapReducePlan plan = meta.mapReducePlan();

                    GridHadoopJobPhase phase = meta.phase();

                    if (phase == PHASE_SETUP) {
                        //
                    }
                    else if (phase == PHASE_MAP || phase == PHASE_REDUCE) {
                        // Must check all nodes, even that are not event node ID due to
                        // multiple node failure possibility.
                        Collection<GridHadoopInputSplit> cancelSplits = null;

                        for (UUID nodeId : plan.mapperNodeIds()) {
                            if (ctx.kernalContext().discovery().node(nodeId) == null) {
                                // Node has left the grid.
                                Collection<GridHadoopInputSplit> mappers = plan.mappers(nodeId);

                                if (cancelSplits == null)
                                    cancelSplits = new HashSet<>();

                                cancelSplits.addAll(mappers);
                            }
                        }

                        Collection<Integer> cancelReducers = null;

                        for (UUID nodeId : plan.reducerNodeIds()) {
                            if (ctx.kernalContext().discovery().node(nodeId) == null) {
                                // Node has left the grid.
                                int[] reducers = plan.reducers(nodeId);

                                if (cancelReducers == null)
                                    cancelReducers = new HashSet<>();

                                for (int rdc : reducers)
                                    cancelReducers.add(rdc);
                            }
                        }

                        if (cancelSplits != null || cancelReducers != null)
                            jobMetaPrj.transform(meta.jobId(), new CancelJobClosure(new GridException("One or " +
                                "more nodes participating in map-reduce job execution failed."), cancelSplits,
                                cancelReducers));
                    }
                }
                catch (GridException e) {
                    U.error(log, "Failed to cancel job: " + meta, e);
                }
            }
        }
    }

    /**
     * @param updated Updated cache entries.
     */
    private void processJobMetadata(Iterable<Map.Entry<GridHadoopJobId, GridHadoopJobMetadata>> updated) {
        UUID locNodeId = ctx.localNodeId();

        for (Map.Entry<GridHadoopJobId, GridHadoopJobMetadata> entry : updated) {
            GridHadoopJobId jobId = entry.getKey();
            GridHadoopJobMetadata meta = entry.getValue();

            if (meta == null)
                continue;

            if (log.isDebugEnabled())
                log.debug("Processing job metadata update callback [locNodeId=" + locNodeId +
                    ", meta=" + meta + ']');

            JobLocalState state = activeJobs.get(jobId);

            GridHadoopJob job = ctx.jobFactory().createJob(jobId, meta.jobInfo());

            GridHadoopMapReducePlan plan = meta.mapReducePlan();

            try {
                ctx.taskExecutor().onJobStateChanged(job, meta);
            }
            catch (GridException e) {
                U.error(log, "Failed to process job state changed callback (will fail the job) " +
                    "[locNodeId=" + locNodeId + ", jobId=" + jobId + ", meta=" + meta + ']', e);

                transform(jobId, new CancelJobClosure(e));

                continue;
            }

            switch (meta.phase()) {
                case PHASE_MAP: {
                    // Check if we should initiate new task on local node.
                    Collection<GridHadoopTaskInfo> tasks = mapperTasks(plan.mappers(locNodeId), job, meta);

                    if (tasks != null)
                        ctx.taskExecutor().run(job, tasks);

                    break;
                }

                case PHASE_REDUCE: {
                    if (meta.pendingReducers().isEmpty() && ctx.jobUpdateLeader()) {
                        GridHadoopTaskInfo info = new GridHadoopTaskInfo(ctx.localNodeId(), GridHadoopTaskType.COMMIT,
                            jobId, 0, 0, null);

                        if (log.isDebugEnabled())
                            log.debug("Submitting COMMIT task for execution [locNodeId=" + locNodeId +
                                ", jobId=" + jobId + ']');

                        ctx.taskExecutor().run(job, Collections.singletonList(info));

                        return;
                    }

                    Collection<GridHadoopTaskInfo> tasks = reducerTasks(plan.reducers(locNodeId), job, meta, null);

                    if (tasks != null)
                        ctx.taskExecutor().run(job, tasks);

                    break;
                }

                case PHASE_CANCELLING: {
                    // Prevent multiple task executor notification.
                    if (state != null && state.onCancel()) {
                        if (log.isDebugEnabled())
                            log.debug("Cancelling local task execution for job: " + meta);

                        ctx.taskExecutor().cancelTasks(jobId);
                    }

                    if (meta.pendingSplits().isEmpty() && meta.pendingReducers().isEmpty()) {
                        if (ctx.jobUpdateLeader()) {
                            if (state == null)
                                state = initState(job, meta);

                            // Prevent running multiple abort tasks.
                            if (state.onAborted()) {
                                GridHadoopTaskInfo info = new GridHadoopTaskInfo(ctx.localNodeId(),
                                    GridHadoopTaskType.ABORT, jobId, 0, 0, null);

                                if (log.isDebugEnabled())
                                    log.debug("Submitting ABORT task for execution [locNodeId=" + locNodeId +
                                        ", jobId=" + jobId + ']');

                                ctx.taskExecutor().run(job, Collections.singletonList(info));
                            }
                        }

                        return;
                    }
                    else {
                        // Check if there are unscheduled mappers or reducers.
                        Collection<GridHadoopInputSplit> cancelMappers = new ArrayList<>();
                        Collection<Integer> cancelReducers = new ArrayList<>();

                        Collection<GridHadoopInputSplit> mappers = plan.mappers(ctx.localNodeId());

                        if (mappers != null) {
                            for (GridHadoopInputSplit b : mappers) {
                                if (state == null || !state.mapperScheduled(b))
                                    cancelMappers.add(b);
                            }
                        }

                        int[] rdc = plan.reducers(ctx.localNodeId());

                        if (rdc != null) {
                            for (int r : rdc) {
                                if (state == null || !state.reducerScheduled(r))
                                    cancelReducers.add(r);
                            }
                        }

                        if (!cancelMappers.isEmpty() || !cancelReducers.isEmpty())
                            transform(jobId, new CancelJobClosure(cancelMappers, cancelReducers));
                    }

                    break;
                }

                case PHASE_COMPLETE: {
                    if (log.isDebugEnabled())
                        log.debug("Job execution is complete, will remove local state from active jobs " +
                            "[jobId=" + jobId + ", meta=" + meta +
                            ", setupTime=" + meta.setupTime() +
                            ", mapTime=" + meta.mapTime() +
                            ", reduceTime=" + meta.reduceTime() +
                            ", totalTime=" + meta.totalTime() + ']');

                    if (state != null) {
                        state = activeJobs.remove(jobId);

                        assert state != null;

                        ctx.shuffle().jobFinished(jobId);
                    }

                    GridFutureAdapter<GridHadoopJobId> finishFut = activeFinishFuts.remove(jobId);

                    if (finishFut != null) {
                        if (log.isDebugEnabled())
                            log.debug("Completing job future [locNodeId=" + locNodeId + ", meta=" + meta + ']');

                        finishFut.onDone(jobId, meta.failCause());
                    }

                    break;
                }

                default:
                    assert false;
            }
        }
    }

    /**
     * Creates mapper tasks based on job information.
     *
     * @param mappers Mapper blocks.
     * @param job Job instance.
     * @param meta Job metadata.
     * @return Collection of created task infos or {@code null} if no mapper tasks scheduled for local node.
     */
    private Collection<GridHadoopTaskInfo> mapperTasks(Iterable<GridHadoopInputSplit> mappers,
        GridHadoopJob job, GridHadoopJobMetadata meta) {
        UUID locNodeId = ctx.localNodeId();
        GridHadoopJobId jobId = job.id();

        JobLocalState state = activeJobs.get(jobId);

        Collection<GridHadoopTaskInfo> tasks = null;

        if (mappers != null) {
            if (state == null)
                state = initState(job, meta);

            for (GridHadoopInputSplit split : mappers) {
                if (state.addMapper(split)) {
                    if (log.isDebugEnabled())
                        log.debug("Submitting MAP task for execution [locNodeId=" + locNodeId +
                            ", split=" + split + ']');

                    GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(locNodeId,
                        GridHadoopTaskType.MAP, jobId, meta.taskNumber(split), 0, split);

                    if (tasks == null)
                        tasks = new ArrayList<>();

                    tasks.add(taskInfo);
                }
            }
        }

        return tasks;
    }

    /**
     * Creates reducer tasks based on job information.
     *
     * @param reducers Reducers (may be {@code null}).
     * @param job Job instance.
     * @param meta Job metadata.
     * @param tasks Optional collection of tasks to add new tasks to.
     * @return Collection of task infos.
     */
    private Collection<GridHadoopTaskInfo> reducerTasks(int[] reducers, GridHadoopJob job, GridHadoopJobMetadata meta,
        Collection<GridHadoopTaskInfo> tasks) {
        UUID locNodeId = ctx.localNodeId();
        GridHadoopJobId jobId = job.id();

        JobLocalState state = activeJobs.get(jobId);

        if (reducers != null) {
            if (state == null)
                state = initState(job, meta);

            for (int rdc : reducers) {
                if (state.addReducer(rdc)) {
                    if (log.isDebugEnabled())
                        log.debug("Submitting REDUCE task for execution [locNodeId=" + locNodeId +
                            ", rdc=" + rdc + ']');

                    GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(locNodeId,
                        GridHadoopTaskType.REDUCE, jobId, rdc, 0, null);

                    if (tasks == null)
                        tasks = new ArrayList<>();

                    tasks.add(taskInfo);
                }
            }
        }

        return tasks;
    }

    /**
     * Initializes local state for given job metadata.
     *
     * @param meta Job metadata.
     * @return Local state.
     */
    private JobLocalState initState(GridHadoopJob job, GridHadoopJobMetadata meta) {
        GridHadoopJobId jobId = meta.jobId();

        JobLocalState state = new JobLocalState(job, meta);

        return F.addIfAbsent(activeJobs, jobId, state);
    }

    /**
     * Gets job instance by job ID.
     *
     * @param jobId Job ID.
     * @return Job.
     */
    @Nullable public GridHadoopJob job(GridHadoopJobId jobId) throws GridException {
        JobLocalState state = activeJobs.get(jobId);

        if (state != null)
            return state.job;

        GridHadoopJobMetadata meta = jobMetaPrj.get(jobId);

        if (meta == null)
            return null;

        return ctx.jobFactory().createJob(jobId, meta.jobInfo());
    }

    /**
     * Kills job.
     *
     * @param jobId Job ID.
     * @return {@code True} if job was killed.
     * @throws GridException If failed.
     */
    public boolean killJob(GridHadoopJobId jobId) throws GridException {
        if (!busyLock.tryReadLock())
            return false; // Grid is stopping.

        try {
            GridHadoopJobMetadata meta = jobMetaPrj.get(jobId);

            if (meta != null && meta.phase() != PHASE_COMPLETE && meta.phase() != PHASE_CANCELLING) {
                GridHadoopTaskCancelledException err = new GridHadoopTaskCancelledException("Job cancelled.");

                jobMetaPrj.transform(jobId, new CancelJobClosure(err));
            }
        }
        finally {
            busyLock.readUnlock();
        }

        GridFuture<?> fut = finishFuture(jobId);

        if (fut != null) {
            try {
                fut.get();
            } catch (Throwable e) {
                if (e.getCause() instanceof GridHadoopTaskCancelledException)
                    return true;
            }
        }

        return false;
    }

    /**
     * Event handler protected by busy lock.
     */
    private abstract class EventHandler implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            if (!busyLock.tryReadLock())
                return;

            try {
                body();
            }
            catch (Throwable e) {
                U.error(log, "Unhandled exception while processing event.", e);
            }
            finally {
                busyLock.readUnlock();
            }
        }

        /**
         * Handler body.
         */
        protected abstract void body();
    }

    /**
     *
     */
    private class JobLocalState {
        /** Job info. */
        private GridHadoopJob job;

        /** Execution plan. */
        private GridHadoopJobMetadata meta;

        /** Mappers. */
        private Collection<GridHadoopInputSplit> currMappers = new HashSet<>();

        /** Reducers. */
        private Collection<Integer> currReducers = new HashSet<>();

        /** Number of completed mappers. */
        private AtomicInteger completedMappersCnt = new AtomicInteger();

        /** Cancelled flag. */
        private boolean cancelled;

        /** Aborted flag. */
        private boolean aborted;

        /**
         * @param job Job.
         */
        private JobLocalState(GridHadoopJob job, GridHadoopJobMetadata meta) {
            this.job = job;
            this.meta = meta;
        }

        /**
         * @param mapSplit Map split to add.
         * @return {@code True} if mapper was added.
         */
        private boolean addMapper(GridHadoopInputSplit mapSplit) {
            return currMappers.add(mapSplit);
        }

        /**
         * @param rdc Reducer number to add.
         * @return {@code True} if reducer was added.
         */
        private boolean addReducer(int rdc) {
            return currReducers.add(rdc);
        }

        /**
         * Checks whether this split was scheduled for given attempt.
         *
         * @param mapSplit Map split to check.
         * @return {@code True} if mapper was scheduled.
         */
        public boolean mapperScheduled(GridHadoopInputSplit mapSplit) {
            return currMappers.contains(mapSplit);
        }

        /**
         * Checks whether this split was scheduled for given attempt.
         *
         * @param rdc Reducer number to check.
         * @return {@code True} if reducer was scheduled.
         */
        public boolean reducerScheduled(int rdc) {
            return currReducers.contains(rdc);
        }

        /**
         * @param taskInfo Task info.
         * @param status Task status.
         */
        private void onMapFinished(final GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {
            final GridHadoopJobId jobId = taskInfo.jobId();

            boolean lastMapperFinished = completedMappersCnt.incrementAndGet() == currMappers.size();

            if (status.state() == FAILED || status.state() == CRASHED) {
                // Fail the whole job.
                transform(jobId, new RemoveMappersClosure(taskInfo.inputSplit(), status.failCause()));

                return;
            }

            if (job.hasCombiner() && get(job, SINGLE_COMBINER_FOR_ALL_MAPPERS, false) && status.state() != CANCELED) {
                // Create combiner.
                if (lastMapperFinished) {
                    GridHadoopTaskInfo info = new GridHadoopTaskInfo(ctx.localNodeId(), COMBINE, jobId,
                        meta.taskNumber(ctx.localNodeId()), taskInfo.attempt(), null);

                    ctx.taskExecutor().run(job, Collections.singletonList(info));
                }
            }
            else {
                GridInClosure<GridFuture<?>> cacheUpdater = new CIX1<GridFuture<?>>() {
                    @Override public void applyx(GridFuture<?> f) {
                        Throwable err = null;

                        if (f != null) {
                            try {
                                f.get();
                            }
                            catch (GridException e) {
                                err = e;
                            }
                        }

                        transform(jobId, new RemoveMappersClosure(taskInfo.inputSplit(), err));
                    }
                };

                if (lastMapperFinished)
                    ctx.shuffle().flush(jobId).listenAsync(cacheUpdater);
                else
                    cacheUpdater.apply(null);
            }
        }

        /**
         * @param taskInfo Task info.
         * @param status Task status.
         */
        private void onReduceFinished(GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {
            GridHadoopJobId jobId = taskInfo.jobId();
            if (status.state() == FAILED || status.state() == CRASHED)
                // Fail the whole job.
                transform(jobId, new RemoveReducerClosure(taskInfo.taskNumber(), status.failCause()));
            else
                transform(jobId, new RemoveReducerClosure(taskInfo.taskNumber()));
        }

        /**
         * @param taskInfo Task info.
         * @param status Task status.
         */
        private void onCombineFinished(GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {
            final GridHadoopJobId jobId = taskInfo.jobId();

            assert job.hasCombiner();

            if (status.state() == FAILED || status.state() == CRASHED)
                // Fail the whole job.
                transform(jobId, new RemoveMappersClosure(currMappers, status.failCause()));
            else {
                ctx.shuffle().flush(jobId).listenAsync(new CIX1<GridFuture<?>>() {
                    @Override public void applyx(GridFuture<?> f) {
                        Throwable err = null;

                        if (f != null) {
                            try {
                                f.get();
                            }
                            catch (GridException e) {
                                err = e;
                            }
                        }

                        transform(jobId, new RemoveMappersClosure(currMappers, err));
                    }
                });
            }
        }

        /**
         * @return {@code True} if job was cancelled by this (first) call.
         */
        public boolean onCancel() {
            if (!cancelled && !aborted) {
                cancelled = true;

                return true;
            }

            return false;
        }

        /**
         * @return {@code True} if job was aborted this (first) call.
         */
        public boolean onAborted() {
            if (!aborted) {
                aborted = true;

                return true;
            }

            return false;
        }
    }

    /**
     * Update job phase transform closure.
     */
    private static class UpdatePhaseClosure implements GridClosure<GridHadoopJobMetadata, GridHadoopJobMetadata> {
        /** Phase to update. */
        private GridHadoopJobPhase phase;

        /**
         * @param phase Phase to update.
         */
        private UpdatePhaseClosure(GridHadoopJobPhase phase) {
            this.phase = phase;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobMetadata apply(GridHadoopJobMetadata meta) {
            GridHadoopJobMetadata cp = new GridHadoopJobMetadata(meta);

            cp.phase(phase);

            if (phase == PHASE_COMPLETE)
                cp.completeTimestamp(System.currentTimeMillis());

            return cp;
        }
    }

    /**
     * Remove mapper transform closure.
     */
    private static class RemoveMappersClosure implements GridClosure<GridHadoopJobMetadata, GridHadoopJobMetadata> {
        /** Mapper split to remove. */
        private Collection<GridHadoopInputSplit> splits;

        /** Error. */
        private Throwable err;

        /**
         * @param split Mapper split to remove.
         */
        private RemoveMappersClosure(GridHadoopInputSplit split, Throwable err) {
            this(Collections.singletonList(split), err);
        }

        /**
         * @param splits Mapper splits to remove.
         */
        private RemoveMappersClosure(Collection<GridHadoopInputSplit> splits, Throwable err) {
            this.splits = splits;
            this.err = err;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobMetadata apply(GridHadoopJobMetadata meta) {
            GridHadoopJobMetadata cp = new GridHadoopJobMetadata(meta);

            Collection<GridHadoopInputSplit> splitsCp = new HashSet<>(cp.pendingSplits());

            splitsCp.removeAll(splits);

            cp.pendingSplits(splitsCp);

            if (cp.phase() != PHASE_CANCELLING)
                cp.failCause(err);

            if (err != null)
                cp.phase(PHASE_CANCELLING);

            if (splitsCp.isEmpty()) {
                if (cp.phase() != PHASE_CANCELLING) {
                    cp.phase(PHASE_REDUCE);

                    cp.mapCompleteTimestamp(System.currentTimeMillis());
                }
            }

            return cp;
        }
    }

    /**
     * Remove reducer transform closure.
     */
    private static class RemoveReducerClosure implements GridClosure<GridHadoopJobMetadata, GridHadoopJobMetadata> {
        /** Mapper split to remove. */
        private int rdc;

        /** Error. */
        private Throwable err;

        /**
         * @param rdc Reducer to remove.
         */
        private RemoveReducerClosure(int rdc) {
            this.rdc = rdc;
        }

        /**
         * @param rdc Reducer to remove.
         */
        private RemoveReducerClosure(int rdc, Throwable err) {
            this.rdc = rdc;
            this.err = err;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobMetadata apply(GridHadoopJobMetadata meta) {
            GridHadoopJobMetadata cp = new GridHadoopJobMetadata(meta);

            Collection<Integer> rdcCp = new HashSet<>(cp.pendingReducers());

            rdcCp.remove(rdc);

            cp.pendingReducers(rdcCp);

            if (err != null) {
                cp.phase(PHASE_CANCELLING);
                cp.failCause(err);
            }

            return cp;
        }
    }

    private static class InitializeReducersClosure
        implements GridClosure<GridHadoopJobMetadata, GridHadoopJobMetadata> {
        /** Reducers. */
        private Collection<Integer> rdc;

        /** Process descriptor for reducers. */
        private GridHadoopProcessDescriptor desc;

        /**
         * @param rdc Reducers to initialize.
         * @param desc External process descriptor.
         */
        private InitializeReducersClosure(Collection<Integer> rdc, GridHadoopProcessDescriptor desc) {
            assert !F.isEmpty(rdc);
            assert desc != null;

            this.rdc = rdc;
            this.desc = desc;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobMetadata apply(GridHadoopJobMetadata meta) {
            GridHadoopJobMetadata cp = new GridHadoopJobMetadata(meta);

            Map<Integer, GridHadoopProcessDescriptor> oldMap = meta.reducersAddresses();

            Map<Integer, GridHadoopProcessDescriptor> rdcMap = oldMap == null ?
                new HashMap<Integer, GridHadoopProcessDescriptor>() : new HashMap<>(oldMap);

            for (Integer r : rdc)
                rdcMap.put(r, desc);

            cp.reducersAddresses(rdcMap);

            return cp;
        }
    }

    /**
     * Remove reducer transform closure.
     */
    private static class CancelJobClosure implements GridClosure<GridHadoopJobMetadata, GridHadoopJobMetadata> {
        /** Mapper split to remove. */
        private Collection<GridHadoopInputSplit> splits;

        /** Reducers to remove. */
        private Collection<Integer> rdc;

        /** Error. */
        private Throwable err;

        /**
         * @param err Fail cause.
         */
        private CancelJobClosure(Throwable err) {
            this(err, null, null);
        }

        /**
         * @param splits Splits to remove.
         * @param rdc Reducers to remove.
         */
        private CancelJobClosure(Collection<GridHadoopInputSplit> splits, Collection<Integer> rdc) {
            this(null, splits, rdc);
        }

        /**
         * @param err Error.
         * @param splits Splits to remove.
         * @param rdc Reducers to remove.
         */
        private CancelJobClosure(Throwable err, Collection<GridHadoopInputSplit> splits, Collection<Integer> rdc) {
            this.splits = splits;
            this.rdc = rdc;
            this.err = err;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobMetadata apply(GridHadoopJobMetadata meta) {
            if (meta == null)
                return null;

            assert meta.phase() == PHASE_CANCELLING || err != null: "Invalid phase for cancel: " + meta;

            GridHadoopJobMetadata cp = new GridHadoopJobMetadata(meta);

            Collection<Integer> rdcCp = new HashSet<>(cp.pendingReducers());

            if (rdc != null)
                rdcCp.removeAll(rdc);

            cp.pendingReducers(rdcCp);

            Collection<GridHadoopInputSplit> splitsCp = new HashSet<>(cp.pendingSplits());

            if (splits != null)
                splitsCp.removeAll(splits);

            cp.pendingSplits(splitsCp);

            cp.phase(PHASE_CANCELLING);

            cp.failCause(err);

            return cp;
        }
    }
}
