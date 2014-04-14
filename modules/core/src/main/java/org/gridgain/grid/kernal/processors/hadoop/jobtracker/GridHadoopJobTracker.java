/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.apache.hadoop.conf.*;
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

import java.util.*;
import java.util.concurrent.*;

/**
 * Hadoop job tracker.
 */
public class GridHadoopJobTracker extends GridHadoopComponent {
    /** System cache. */
    private GridCache<Object, Object> sysCache;

    /** Map-reduce execution planner. */
    private GridHadoopMapReducePlanner mrPlanner;

    /** Locally active jobs. */
    private ConcurrentMap<GridHadoopJobId, JobLocalState> activeJobs = new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        sysCache = ctx.kernalContext().cache().cache(ctx.systemCacheName());

        mrPlanner = ctx.planner();

        GridCacheProjection<GridHadoopJobId, GridHadoopJobMetadata<Configuration>> projection = sysCache
            .projection(GridHadoopJobId.class, GridHadoopJobMetadata.class);

        GridCacheContinuousQuery<GridHadoopJobId, GridHadoopJobMetadata<Configuration>> qry = projection.queries()
            .createContinuousQuery();

        qry.callback(new GridBiPredicate<UUID,
            Collection<Map.Entry<GridHadoopJobId, GridHadoopJobMetadata<Configuration>>>>() {
            @Override public boolean apply(UUID nodeId,
                Collection<Map.Entry<GridHadoopJobId, GridHadoopJobMetadata<Configuration>>> evts) {
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
    public GridFuture<?> submit(GridHadoopJobId jobId, GridHadoopJobInfo<Configuration> info) {
        try {
            GridHadoopJob<Configuration> job = ctx.<Configuration>jobFactory().createJob(jobId, info);

            Collection<GridHadoopFileBlock> blocks = job.input();

            GridHadoopMapReducePlan mrPlan = mrPlanner.preparePlan(blocks, ctx.nodes(), job, null);

            GridHadoopJobMetadata<Configuration> meta = new GridHadoopJobMetadata<>(jobId, info);

            meta.mapReducePlan(mrPlan);

            sysCache.put(jobId, meta);

            return null;
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
    @Nullable public GridHadoopJobStatus status(GridHadoopJobId jobId) {
        return null;
    }

    /**
     * Callback from task executor invoked when a task has been finished.
     *
     * @param taskInfo Task info.
     * @param status Task status.
     */
    public void onTaskFinished(GridHadoopTaskInfo taskInfo, GridHadoopTaskStatus status) {

    }

    /**
     * @param blocks Blocks to init nodes for.
     */
    private void initBlockNodes(Collection<GridHadoopFileBlock> blocks) {

    }

    /**
     * @param origNodeId Originating node ID.
     * @param updated Updated cache entries.
     */
    private void processJobMetadata(UUID origNodeId,
        Iterable<Map.Entry<GridHadoopJobId, GridHadoopJobMetadata<Configuration>>> updated) {
        for (Map.Entry<GridHadoopJobId, GridHadoopJobMetadata<Configuration>> entry : updated) {
            GridHadoopJobId jobId = entry.getKey();

            JobLocalState state = activeJobs.get(jobId);

            if (state == null)
                state = F.addIfAbsent(activeJobs, jobId, new JobLocalState());

            GridHadoopJobMetadata<Configuration> meta = entry.getValue();

            GridHadoopJob<Configuration> job = ctx.<Configuration>jobFactory().createJob(jobId, meta.jobInfo());

            // Check if we should initiate new task on local node.
            for (GridHadoopFileBlock block : meta.mapReducePlan().mappers(ctx.localNodeId())) {
                if (state.addMapper(block)) {
                    GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo();

                    GridHadoopTask task = job.createTask(taskInfo);

                    ctx.taskExecutor().run(taskInfo, task);
                }
            }

            for (int reducer : meta.mapReducePlan().reducers(ctx.localNodeId())) {
                if (state.addReducer(reducer)) {
                    GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo();

                    GridHadoopTask task = job.createTask(taskInfo);

                    ctx.taskExecutor().run(taskInfo, task);
                }
            }
        }
    }

    /**
     *
     */
    private static class JobLocalState {
        /**
         * Adds mapper for local job state if this mapper has not been added yet.
         *
         * @param block Block to add.
         * @return {@code True} if mapper was not added to this local node  yet.
         */
        public boolean addMapper(GridHadoopFileBlock block) {
            // TODO.
            return true;
        }

        /**
         * Adds reducer for local job state if this reducer has not been added yet.
         *
         * @param rdcIdx Reducer index.
         * @return {@code True} if reducer was not added to this local node yet.
         */
        public boolean addReducer(int rdcIdx) {
            // TODO.
            return true;
        }
    }
}
