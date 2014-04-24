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
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.hadoop.GridHadoopTaskType.COMBINE;
import static org.gridgain.grid.kernal.processors.hadoop.jobtracker.GridHadoopJobPhase.*;

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

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        GridCache<Object, Object> sysCache = ctx.kernalContext().cache().cache(ctx.systemCacheName());

        mrPlanner = ctx.planner();

        jobMetaPrj = sysCache.projection(GridHadoopJobId.class, GridHadoopJobMetadata.class);

        GridCacheContinuousQuery<GridHadoopJobId, GridHadoopJobMetadata> qry = jobMetaPrj.queries()
            .createContinuousQuery();

        qry.callback(new GridBiPredicate<UUID,
            Collection<Map.Entry<GridHadoopJobId, GridHadoopJobMetadata>>>() {
            @Override public boolean apply(UUID nodeId,
                Collection<Map.Entry<GridHadoopJobId, GridHadoopJobMetadata>> evts) {
                processJobMetadata(nodeId, evts);

                return true;
            }
        });

        qry.execute();
    }

    /**
     * Submits execution of Hadoop job to grid.
     *
     * @param jobId Job ID.
     * @param info Job info.
     * @return Job completion future.
     */
    public GridFuture<GridHadoopJobId> submit(GridHadoopJobId jobId, GridHadoopJobInfo info) {
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
    }

    /**
     * Gets hadoop job status for given job ID.
     *
     * @param jobId Job ID to get status for.
     * @return Job status for given job ID or {@code null} if job was not found.
     */
    @Nullable public GridHadoopJobStatus status(GridHadoopJobId jobId) throws GridException {
        GridHadoopJobMetadata meta = jobMetaPrj.get(jobId);

        if (meta == null)
            return null;

        if (log.isDebugEnabled())
            log.debug("Got job metadata for status check [locNodeId=" + ctx.localNodeId() + ", meta=" + meta + ']');

        GridHadoopJobInfo info = meta.jobInfo();

        if (meta.phase() == PHASE_COMPLETE) {
            if (log.isDebugEnabled())
                log.debug("Job is complete, returning finished future: " + jobId);

            return new GridHadoopJobStatus(new GridFinishedFutureEx<>(jobId), info);
        }

        GridFutureAdapter<GridHadoopJobId> fut = F.addIfAbsent(activeFinishFuts, jobId,
            new GridFutureAdapter<GridHadoopJobId>());

        // Get meta from cache one more time to close the window.
        meta = jobMetaPrj.get(jobId);

        if (log.isDebugEnabled())
            log.debug("Re-checking job metadata [locNodeId=" + ctx.localNodeId() + ", meta=" + meta + ']');

        if (meta == null || meta.phase() == PHASE_COMPLETE) {
            // TODO exception.
            fut.onDone(jobId);

            activeFinishFuts.remove(jobId , fut);
        }

        return new GridHadoopJobStatus(fut, info);
    }

    /**
     * Gets job info for running Hadoop map-reduce task by job ID.
     *
     * @param jobId Job ID to get job info for.
     * @return Job info or {@code null} if no information found for this job ID.
     * @throws GridException If cache lookup for this job ID failed.
     */
    @Nullable public GridHadoopJobInfo jobInfo(GridHadoopJobId jobId) throws GridException {
        JobLocalState state = activeJobs.get(jobId);

        if (state != null)
            return state.job.info();

        GridHadoopJobMetadata meta = jobMetaPrj.get(jobId);

        if (meta != null)
            return meta.jobInfo();

        return null;
    }

    /**
     * Callback from task executor invoked when a task has been finished.
     *
     * @param taskInfo Task info.
     * @param status Task status.
     */
    public void onTaskFinished(GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {
        if (log.isDebugEnabled())
            log.debug("Received task finished callback [taskInfo=" + taskInfo + ", status=" + status + ']');

        JobLocalState state = activeJobs.get(taskInfo.jobId());

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
     * @param origNodeId Originating node ID.
     * @param updated Updated cache entries.
     */
    private void processJobMetadata(UUID origNodeId,
        Iterable<Map.Entry<GridHadoopJobId, GridHadoopJobMetadata>> updated) {
        UUID locNodeId = ctx.localNodeId();

        for (Map.Entry<GridHadoopJobId, GridHadoopJobMetadata> entry : updated) {
            GridHadoopJobId jobId = entry.getKey();
            GridHadoopJobMetadata meta = entry.getValue();

            if (log.isDebugEnabled())
                log.debug("Processing job metadata update callback [locNodeId=" + locNodeId +
                    ", meta=" + meta + ']');

            JobLocalState state = activeJobs.get(jobId);

            GridHadoopJob job = ctx.jobFactory().createJob(jobId, meta.jobInfo());

            switch (meta.phase()) {
                case PHASE_MAP: {
                    // Check if we should initiate new task on local node.
                    Collection<GridHadoopFileBlock> mappers = meta.mapReducePlan().mappers(locNodeId);

                    if (mappers != null) {
                        if (state == null)
                            state = initState(meta);

                        Collection<GridHadoopTask> tasks = null;

                        for (GridHadoopFileBlock block : mappers) {
                            int attempt = meta.attempt(block);

                            if (state.addMapper(attempt, block)) {
                                if (log.isDebugEnabled())
                                    log.debug("Submitting MAP task for execution [locNodeId=" + locNodeId +
                                        ", block=" + block + ']');

                                // TODO task number - how do we count it?
                                GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(locNodeId,
                                    GridHadoopTaskType.MAP, jobId, 0, attempt, block);

                                GridHadoopTask task = job.createTask(taskInfo);

                                assert task != null : "Job created null task: " + job;


                                if (tasks == null)
                                    tasks = new ArrayList<>();

                                tasks.add(task);
                            }
                        }

                        if (tasks != null)
                            ctx.taskExecutor().run(tasks);
                    }

                    break;
                }

                case PHASE_REDUCE: {
                    if (meta.pendingReducers().isEmpty() && ctx.jobUpdateLeader()) {
                        if (log.isDebugEnabled())
                            log.debug("Moving job to COMPLETE state [locNodeId=" + locNodeId +
                                ", meta=" + meta + ']');

                        GridCacheEntry<GridHadoopJobId, GridHadoopJobMetadata> jobEntry = jobMetaPrj.entry(jobId);

                        jobEntry.timeToLive(10_000); // TODO configuration?

                        jobEntry.transformAsync(new UpdatePhaseClosure(PHASE_COMPLETE));

                        return;
                    }

                    int[] reducers = meta.mapReducePlan().reducers(locNodeId);

                    if (reducers != null) {
                        if (state == null)
                            state = initState(meta);

                        Collection<GridHadoopTask> tasks = null;

                        for (int rdc : reducers) {
                            int attempt = meta.attempt(rdc);

                            if (state.addReducer(attempt, rdc)) {
                                if (log.isDebugEnabled())
                                    log.debug("Submitting REDUCE task for execution [locNodeId=" + locNodeId +
                                        ", rdc=" + rdc + ']');

                                GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(locNodeId,
                                    GridHadoopTaskType.REDUCE, jobId, rdc, attempt, null);

                                GridHadoopTask task = job.createTask(taskInfo);

                                assert task != null : "Job created null task: " + job;

                                if (tasks == null)
                                    tasks = new ArrayList<>();

                                tasks.add(task);
                            }
                        }

                        if (tasks != null)
                            ctx.taskExecutor().run(tasks);
                    }

                    break;
                }

                case PHASE_COMPLETE: {
                    if (state != null) {
                        state = activeJobs.remove(jobId);

                        assert state != null;
                    }

                    GridFutureAdapter<GridHadoopJobId> finishFut = activeFinishFuts.remove(jobId);

                    if (finishFut != null) {
                        if (log.isDebugEnabled())
                            log.debug("Completing job future [locNodeId=" + locNodeId + ", meta=" + meta + ']');

                        finishFut.onDone(jobId); // TODO exception.
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
    private JobLocalState initState(GridHadoopJobMetadata meta) {
        GridHadoopJobId jobId = meta.jobId();

        GridHadoopJob job = ctx.jobFactory().createJob(jobId, meta.jobInfo());

        JobLocalState state = new JobLocalState(job);

        return F.addIfAbsent(activeJobs, jobId, state);
    }

    /**
     *
     */
    private class JobLocalState {
        /** Job info. */
        private GridHadoopJob job;

        /** Attempts. */
        private Map<Integer, AttemptGroup> attempts = new HashMap<>();

        /**
         * @param job Job.
         */
        private JobLocalState(GridHadoopJob job) {
            this.job = job;
        }

        /**
         * @param attempt Attempt number.
         * @param mapBlock Map block to add.
         * @return {@code True} if mapper was added.
         */
        private boolean addMapper(int attempt, GridHadoopFileBlock mapBlock) {
            AttemptGroup grp = attempts.get(attempt);

            if (grp == null)
                attempts.put(attempt, grp = new AttemptGroup());

            return grp.addMapper(mapBlock);
        }

        /**
         * @param attempt Attempt number.
         * @param rdc Reducer number to add.
         * @return {@code True} if reducer was added.
         */
        private boolean addReducer(int attempt, int rdc) {
            AttemptGroup grp = attempts.get(attempt);

            if (grp == null)
                attempts.put(attempt, grp = new AttemptGroup());

            return grp.addReducer(rdc);
        }

        /**
         * @param taskInfo Task info.
         * @param status Task status.
         */
        private void onMapFinished(GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {
            GridHadoopJobId jobId = taskInfo.jobId();

            AttemptGroup group = attempts.get(taskInfo.attempt());

            assert group != null;

            boolean combine = group.onMapFinished();

            if (job.hasCombiner()) {
                // Create combiner.
                if (combine) {
                    GridHadoopTaskInfo info = new GridHadoopTaskInfo(ctx.localNodeId(), COMBINE, jobId,
                        0, taskInfo.attempt(), null);

                    GridHadoopTask task = job.createTask(info);

                    ctx.taskExecutor().run(Collections.singletonList(task));
                }
            }
            else {
                jobMetaPrj.transformAsync(jobId, new RemoveMappersClosure(taskInfo.fileBlock()));
            }
        }

        /**
         * @param taskInfo Task info.
         * @param status Task status.
         */
        private void onReduceFinished(GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {
            jobMetaPrj.transformAsync(taskInfo.jobId(), new RemoveReducerClosure(taskInfo.taskNumber()));
        }

        /**
         * @param taskInfo Task info.
         * @param status Task status.
         */
        private void onCombineFinished(GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {
            AttemptGroup group = attempts.get(taskInfo.attempt());

            assert group != null;

            GridHadoopJobId jobId = taskInfo.jobId();

            assert job.hasCombiner();

            jobMetaPrj.transformAsync(jobId, new RemoveMappersClosure(group.mappers()));
        }
    }

    /**
     * Job tracker's local job state.
     */
    private static class AttemptGroup {
        /** Mappers. */
        private Collection<GridHadoopFileBlock> currentMappers = new HashSet<>();

        /** Number of completed mappers. */
        private AtomicInteger completedMappersCnt = new AtomicInteger();

        /** Reducers. */
        private Collection<Integer> currentReducers = new HashSet<>();

        /**
         * Adds mapper for local job state if this mapper has not been added yet.
         *
         * @param block Block to add.
         * @return {@code True} if mapper was not added to this local node  yet.
         */
        public boolean addMapper(GridHadoopFileBlock block) {
            return currentMappers.add(block);
        }

        /**
         * Adds reducer for local job state if this reducer has not been added yet.
         *
         * @param rdcIdx Reducer index.
         * @return {@code True} if reducer was not added to this local node yet.
         */
        public boolean addReducer(int rdcIdx) {
            return currentReducers.add(rdcIdx);
        }

        /**
         * Gets this group's mappers.
         *
         * @return Collection of group mappers.
         */
        public Collection<GridHadoopFileBlock> mappers() {
            return currentMappers;
        }

        /**
         * @return {@code True} if last mapper has been completed.
         */
        public boolean onMapFinished() {
            return completedMappersCnt.incrementAndGet() == currentMappers.size();
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

        /**
         * @param block Mapper block to remove.
         */
        private RemoveMappersClosure(GridHadoopFileBlock block) {
            blocks = Collections.singletonList(block);
        }

        /**
         * @param blocks Mapper blocks to remove.
         */
        private RemoveMappersClosure(Collection<GridHadoopFileBlock> blocks) {
            this.blocks = blocks;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobMetadata apply(GridHadoopJobMetadata meta) {
            GridHadoopJobMetadata cp = new GridHadoopJobMetadata(meta);

            Collection<GridHadoopFileBlock> blocksCp = new HashSet<>(cp.pendingBlocks());

            blocksCp.removeAll(blocks);

            cp.pendingBlocks(blocksCp);

            if (blocksCp.isEmpty())
                cp.phase(PHASE_REDUCE);

            return cp;
        }
    }

    /**
     * Remove reducer transform closure.
     */
    private static class RemoveReducerClosure implements GridClosure<GridHadoopJobMetadata, GridHadoopJobMetadata> {
        /** Mapper block to remove. */
        private int rdc;

        /**
         * @param rdc Reducer to remove.
         */
        private RemoveReducerClosure(int rdc) {
            this.rdc = rdc;
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJobMetadata apply(GridHadoopJobMetadata meta) {
            GridHadoopJobMetadata cp = new GridHadoopJobMetadata(meta);

            Collection<Integer> rdcCp = new HashSet<>(cp.pendingReducers());

            rdcCp.remove(rdc);

            cp.pendingReducers(rdcCp);

            return cp;
        }
    }
}
