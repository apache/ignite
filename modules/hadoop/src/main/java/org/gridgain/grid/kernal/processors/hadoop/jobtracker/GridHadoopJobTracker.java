/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.apache.hadoop.fs.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.hadoop.GridHadoopTaskType.*;
import static org.gridgain.grid.kernal.processors.hadoop.jobtracker.GridHadoopJobPhase.*;
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
    private ExecutorService eventProcSvc;

    /** Component busy lock. */
    private GridSpinReadWriteLock busyLock;

    /** {@inheritDoc} */
    @Override public void start(GridHadoopContext ctx) throws GridException {
        super.start(ctx);

        busyLock = new GridSpinReadWriteLock();

        eventProcSvc = Executors.newFixedThreadPool(1);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        GridCache<Object, Object> sysCache = ctx.kernalContext().cache().cache(ctx.systemCacheName());

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
                    eventProcSvc.submit(new EventHandler() {
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
                    eventProcSvc.submit(new EventHandler() {
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

        eventProcSvc.shutdown();

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
    public GridFuture<GridHadoopJobId> submit(GridHadoopJobId jobId, GridHadoopJobInfo info) {
        if (!busyLock.tryReadLock()) {
            return new GridFinishedFutureEx<>(new GridException("Failed to execute map-reduce job " +
                "(grid is stopping): " + info));
        }

        try {
            GridHadoopJob job = ctx.jobFactory().createJob(jobId, info);

            Collection<GridHadoopFileBlock> blocks = job.input();

            GridHadoopMapReducePlan mrPlan = mrPlanner.preparePlan(blocks, ctx.nodes(), job, null);

            GridHadoopJobMetadata meta = new GridHadoopJobMetadata(jobId, info);

            meta.mapReducePlan(mrPlan);

            meta.pendingBlocks(allBlocks(mrPlan));
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

            if (log.isDebugEnabled())
                log.debug("Got job metadata for status check [locNodeId=" + ctx.localNodeId() + ", meta=" + meta + ']');

            if (meta.phase() == PHASE_COMPLETE) {
                if (log.isDebugEnabled())
                    log.debug("Job is complete, returning finished future: " + jobId);

                return new GridFinishedFutureEx<>(jobId);
            }

            GridFutureAdapter<GridHadoopJobId> fut = F.addIfAbsent(activeFinishFuts, jobId,
                new GridFutureAdapter<GridHadoopJobId>());

            // Get meta from cache one more time to close the window.
            meta = jobMetaPrj.get(jobId);

            if (log.isDebugEnabled())
                log.debug("Re-checking job metadata [locNodeId=" + ctx.localNodeId() + ", meta=" + meta + ']');

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
     * @param taskInfo Task info.
     * @param status Task status.
     */
    public void onTaskFinished(GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {
        if (!busyLock.tryReadLock())
            return;

        try {
            assert status.state() != RUNNING;

            if (log.isDebugEnabled())
                log.debug("Received task finished callback [taskInfo=" + taskInfo + ", status=" + status + ']');

            JobLocalState state = activeJobs.get(taskInfo.jobId());

            assert (status.state() != FAILED && status.state() != CRASHED) || status.failCause() != null :
                "Invalid task status [taskInfo=" + taskInfo + ", status=" + status + ']';

            assert state != null;

            switch (taskInfo.type()) {
                case MAP: {
                    state.onMapFinished(taskInfo, status);

                    break;
                }

                case REDUCE: {
                    state.onReduceFinished(taskInfo, status);

                    break;
                }

                case COMBINE: {
                    state.onCombineFinished(taskInfo, status);

                    break;
                }

                case COMMIT:
                case ABORT: {
                    GridCacheEntry<GridHadoopJobId, GridHadoopJobMetadata> entry = jobMetaPrj.entry(taskInfo.jobId());

                    entry.timeToLive(ctx.configuration().getFinishedJobInfoTtl());

                    entry.transformAsync(new UpdatePhaseClosure(PHASE_COMPLETE));

                    break;
                }
            }
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /**
     * @param info Job info.
     * @param blocks Blocks to init nodes for.
     */
    private void assignBlockHosts(GridHadoopDefaultJobInfo info, Iterable<GridHadoopFileBlock> blocks)
        throws GridException {
        Path path = null;
        FileSystem fs = null;
        FileStatus stat = null;

        for (GridHadoopFileBlock block : blocks) {
            try {
                Path p = new Path(block.file());

                // Get file sustem and status only on path change.
                if (!F.eq(path, p)) {
                    path = p;

                    fs = path.getFileSystem(info.configuration());

                    stat = fs.getFileStatus(path);
                }

                BlockLocation[] locs = fs.getFileBlockLocations(stat, block.start(), block.length());

                assert locs != null;
                assert locs.length != 0;

                long maxLen = Long.MIN_VALUE;
                BlockLocation max = null;

                for (BlockLocation l : locs) {
                    if (maxLen < l.getLength()) {
                        maxLen = l.getLength();
                        max = l;
                    }
                }

                assert max != null;

                block.hosts(max.getHosts());
            }
            catch (IOException e) {
                throw new GridException(e);
            }
        }
    }

    /**
     * Gets all file blocks for given hadoop map-reduce plan.
     *
     * @param plan Map-reduce plan.
     * @return Collection of all file blocks that should be processed.
     */
    private Collection<GridHadoopFileBlock> allBlocks(GridHadoopMapReducePlan plan) {
        Collection<GridHadoopFileBlock> res = new HashSet<>();

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
    private void processNodeLeft(GridDiscoveryEvent evt) {
        if (log.isDebugEnabled())
            log.debug("Processing discovery event [locNodeId=" + ctx.localNodeId() + ", evt=" + evt + ']');

        // Check only if this node is responsible for job status updates.
        if (ctx.jobUpdateLeader()) {
            // Iteration over all local entries is correct since system cache is REPLICATED.
            for (GridHadoopJobMetadata meta : jobMetaPrj.values()) {
                try {
                    GridHadoopMapReducePlan plan = meta.mapReducePlan();

                    GridHadoopJobPhase phase = meta.phase();

                    if (phase == PHASE_MAP || phase == PHASE_REDUCE) {
                        // Must check all nodes, even that are not event node ID due to
                        // multiple node failure possibility.
                        Collection<GridHadoopFileBlock> cancelBlocks = null;

                        for (UUID nodeId : plan.mapperNodeIds()) {
                            if (ctx.kernalContext().discovery().node(nodeId) == null) {
                                // Node has left the grid.
                                Collection<GridHadoopFileBlock> mappers = plan.mappers(nodeId);

                                if (cancelBlocks == null)
                                    cancelBlocks = new HashSet<>();

                                cancelBlocks.addAll(mappers);
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

                        if (cancelBlocks != null || cancelReducers != null)
                            jobMetaPrj.transform(meta.jobId(), new CancelJobClosure(new GridException("One or more nodes " +
                                "participating in map-reduce job execution failed."), cancelBlocks, cancelReducers));
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

            if (log.isDebugEnabled())
                log.debug("Processing job metadata update callback [locNodeId=" + locNodeId +
                    ", meta=" + meta + ']');

            JobLocalState state = activeJobs.get(jobId);

            GridHadoopJob job = ctx.jobFactory().createJob(jobId, meta.jobInfo());

            GridHadoopMapReducePlan plan = meta.mapReducePlan();

            switch (meta.phase()) {
                case PHASE_MAP: {
                    // Check if we should initiate new task on local node.
                    Collection<GridHadoopFileBlock> mappers = plan.mappers(locNodeId);

                    if (mappers != null) {
                        if (state == null)
                            state = initState(job, meta);

                        Collection<GridHadoopTask> tasks = null;

                        for (GridHadoopFileBlock block : mappers) {
                            if (state.addMapper(block)) {
                                if (log.isDebugEnabled())
                                    log.debug("Submitting MAP task for execution [locNodeId=" + locNodeId +
                                        ", block=" + block + ']');

                                GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(locNodeId,
                                    GridHadoopTaskType.MAP, jobId, meta.taskNumber(block), 0, block);

                                GridHadoopTask task = job.createTask(taskInfo);

                                if (tasks == null)
                                    tasks = new ArrayList<>();

                                tasks.add(task);
                            }
                        }

                        if (tasks != null)
                            ctx.taskExecutor().run(job, tasks);
                    }

                    break;
                }

                case PHASE_REDUCE: {
                    if (meta.pendingReducers().isEmpty() && ctx.jobUpdateLeader()) {
                        GridHadoopTaskInfo info = new GridHadoopTaskInfo(ctx.localNodeId(), GridHadoopTaskType.COMMIT,
                            jobId, 0, 0, null);

                        GridHadoopTask cleanupTask = job.createTask(info);

                        if (log.isDebugEnabled())
                            log.debug("Submitting COMMIT task for execution [locNodeId=" + locNodeId +
                                ", jobId=" + jobId + ']');

                        ctx.taskExecutor().run(job, Collections.singletonList(cleanupTask));

                        return;
                    }

                    int[] reducers = plan.reducers(locNodeId);

                    if (reducers != null) {
                        if (state == null)
                            state = initState(job, meta);

                        Collection<GridHadoopTask> tasks = null;

                        for (int rdc : reducers) {
                            if (state.addReducer(rdc)) {
                                if (log.isDebugEnabled())
                                    log.debug("Submitting REDUCE task for execution [locNodeId=" + locNodeId +
                                        ", rdc=" + rdc + ']');

                                GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(locNodeId,
                                    GridHadoopTaskType.REDUCE, jobId, rdc, 0, null);

                                GridHadoopTask task = job.createTask(taskInfo);

                                assert task != null : "Job created null task: " + job;

                                if (tasks == null)
                                    tasks = new ArrayList<>();

                                tasks.add(task);
                            }
                        }

                        if (tasks != null)
                            ctx.taskExecutor().run(job, tasks);
                    }

                    break;
                }

                case PHASE_CANCELLING: {
                    // Prevent multiple task executor notification.
                    if (state != null && state.onCancel()) {
                        if (log.isDebugEnabled())
                            log.debug("Cancelling local task execution for job: " + meta);

                        ctx.taskExecutor().cancelTasks(jobId);
                    }

                    if (meta.pendingBlocks().isEmpty() && meta.pendingReducers().isEmpty()) {
                        GridHadoopTaskInfo info = new GridHadoopTaskInfo(ctx.localNodeId(), GridHadoopTaskType.ABORT,
                            jobId, 0, 0, null);

                        GridHadoopTask cleanupTask = job.createTask(info);

                        if (log.isDebugEnabled())
                            log.debug("Submitting ABORT task for execution [locNodeId=" + locNodeId +
                                ", jobId=" + jobId + ']');

                        ctx.taskExecutor().run(job, Collections.singletonList(cleanupTask));

                        return;
                    }
                    else {
                        // Check if there are unscheduled mappers or reducers.
                        Collection<GridHadoopFileBlock> cancelMappers = new ArrayList<>();
                        Collection<Integer> cancelReducers = new ArrayList<>();

                        Collection<GridHadoopFileBlock> mappers = plan.mappers(ctx.localNodeId());

                        if (mappers != null) {
                            for (GridHadoopFileBlock b : mappers) {
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
                            jobMetaPrj.transformAsync(jobId, new CancelJobClosure(cancelMappers, cancelReducers));
                    }

                    break;
                }

                case PHASE_COMPLETE: {
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
        private Collection<GridHadoopFileBlock> currentMappers = new HashSet<>();

        /** Reducers. */
        private Collection<Integer> currentReducers = new HashSet<>();

        /** Number of completed mappers. */
        private AtomicInteger completedMappersCnt = new AtomicInteger();

        /** Cancelled flag. */
        private boolean cancelled;

        /**
         * @param job Job.
         */
        private JobLocalState(GridHadoopJob job, GridHadoopJobMetadata meta) {
            this.job = job;
            this.meta = meta;
        }

        /**
         * @param mapBlock Map block to add.
         * @return {@code True} if mapper was added.
         */
        private boolean addMapper(GridHadoopFileBlock mapBlock) {
            return currentMappers.add(mapBlock);
        }

        /**
         * @param rdc Reducer number to add.
         * @return {@code True} if reducer was added.
         */
        private boolean addReducer(int rdc) {
            return currentReducers.add(rdc);
        }

        /**
         * Checks whether this block was scheduled for given attempt.
         *
         * @param mapBlock Map block to check.
         * @return {@code True} if mapper was scheduled.
         */
        public boolean mapperScheduled(GridHadoopFileBlock mapBlock) {
            return currentMappers.contains(mapBlock);
        }

        /**
         * Checks whether this block was scheduled for given attempt.
         *
         * @param rdc Reducer number to check.
         * @return {@code True} if reducer was scheduled.
         */
        public boolean reducerScheduled(int rdc) {
            return currentReducers.contains(rdc);
        }

        /**
         * @param taskInfo Task info.
         * @param status Task status.
         */
        private void onMapFinished(final GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {
            final GridHadoopJobId jobId = taskInfo.jobId();

            boolean lastMapperFinished = completedMappersCnt.incrementAndGet() == currentMappers.size();

            if (status.state() == FAILED || status.state() == CRASHED) {
                // Fail the whole job.
                jobMetaPrj.transformAsync(jobId, new RemoveMappersClosure(taskInfo.fileBlock(), status.failCause()));

                return;
            }

            if (job.hasCombiner()) {
                // Create combiner.
                if (lastMapperFinished) {
                    GridHadoopTaskInfo info = new GridHadoopTaskInfo(ctx.localNodeId(), COMBINE, jobId,
                        meta.taskNumber(ctx.localNodeId()), taskInfo.attempt(), null);

                    GridHadoopTask task = job.createTask(info);

                    ctx.taskExecutor().run(job, Collections.singletonList(task));
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

                        jobMetaPrj.transformAsync(jobId, new RemoveMappersClosure(taskInfo.fileBlock(), err));
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
                jobMetaPrj.transformAsync(jobId, new RemoveReducerClosure(taskInfo.taskNumber(), status.failCause()));
            else
                jobMetaPrj.transformAsync(jobId, new RemoveReducerClosure(taskInfo.taskNumber()));
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
                jobMetaPrj.transformAsync(jobId, new RemoveMappersClosure(currentMappers, status.failCause()));
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

                        jobMetaPrj.transformAsync(jobId, new RemoveMappersClosure(currentMappers, err));
                    }
                });
            }
        }

        /**
         * @return {@code True} if job was cancelled by this (first) call.
         */
        public boolean onCancel() {
            if (!cancelled) {
                cancelled = true;

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

            return cp;
        }
    }

    /**
     * Remove mapper transform closure.
     */
    private static class RemoveMappersClosure implements GridClosure<GridHadoopJobMetadata, GridHadoopJobMetadata> {
        /** Mapper block to remove. */
        private Collection<GridHadoopFileBlock> blocks;

        /** Error. */
        private Throwable err;

        /**
         * @param block Mapper block to remove.
         */
        private RemoveMappersClosure(GridHadoopFileBlock block) {
            blocks = Collections.singletonList(block);
        }

        /**
         * @param block Mapper block to remove.
         */
        private RemoveMappersClosure(GridHadoopFileBlock block, Throwable err) {
            blocks = Collections.singletonList(block);
            this.err = err;
        }

        /**
         * @param blocks Mapper blocks to remove.
         */
        private RemoveMappersClosure(Collection<GridHadoopFileBlock> blocks) {
            this.blocks = blocks;
        }

        /**
         * @param blocks Mapper blocks to remove.
         */
        private RemoveMappersClosure(Collection<GridHadoopFileBlock> blocks, Throwable err) {
            this.blocks = blocks;
            this.err = err;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobMetadata apply(GridHadoopJobMetadata meta) {
            GridHadoopJobMetadata cp = new GridHadoopJobMetadata(meta);

            Collection<GridHadoopFileBlock> blocksCp = new HashSet<>(cp.pendingBlocks());

            blocksCp.removeAll(blocks);

            cp.pendingBlocks(blocksCp);

            cp.failCause(err);

            if (err != null)
                cp.phase(PHASE_CANCELLING);

            if (blocksCp.isEmpty()) {
                if (cp.phase() != PHASE_CANCELLING)
                    cp.phase(PHASE_REDUCE);
            }

            return cp;
        }
    }

    /**
     * Remove reducer transform closure.
     */
    private static class RemoveReducerClosure implements GridClosure<GridHadoopJobMetadata, GridHadoopJobMetadata> {
        /** Mapper block to remove. */
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

            if (err != null)
                cp.phase(PHASE_CANCELLING);

            return cp;
        }
    }

    /**
     * Remove reducer transform closure.
     */
    private static class CancelJobClosure implements GridClosure<GridHadoopJobMetadata, GridHadoopJobMetadata> {
        /** Mapper block to remove. */
        private Collection<GridHadoopFileBlock> blocks;

        /** Reducers to remove. */
        private Collection<Integer> rdc;

        /** Error. */
        private Throwable err;

        /**
         * @param blocks Blocks to remove.
         * @param rdc Reducers to remove.
         */
        private CancelJobClosure(Collection<GridHadoopFileBlock> blocks, Collection<Integer> rdc) {
            this.blocks = blocks;
            this.rdc = rdc;
        }

        /**
         * @param err Error.
         * @param blocks Blocks to remove.
         * @param rdc Reducers to remove.
         */
        private CancelJobClosure(Throwable err, Collection<GridHadoopFileBlock> blocks, Collection<Integer> rdc) {
            this.blocks = blocks;
            this.rdc = rdc;
            this.err = err;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobMetadata apply(GridHadoopJobMetadata meta) {
            assert meta.phase() == PHASE_CANCELLING || err != null: "Invalid phase for cancel: " + meta;

            GridHadoopJobMetadata cp = new GridHadoopJobMetadata(meta);

            Collection<Integer> rdcCp = new HashSet<>(cp.pendingReducers());

            rdcCp.removeAll(rdc);

            cp.pendingReducers(rdcCp);

            Collection<GridHadoopFileBlock> blocksCp = new HashSet<>(cp.pendingBlocks());

            blocksCp.removeAll(blocks);

            cp.pendingBlocks(blocksCp);

            cp.phase(PHASE_CANCELLING);

            if (blocksCp.isEmpty() && rdcCp.isEmpty())
                cp.phase(PHASE_COMPLETE);

            return cp;
        }
    }
}
