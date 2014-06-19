/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.gridgain.grid.util.*;
import java.util.concurrent.*;
import java.util.*;


/**
 * Task executor.
 */
public class GridHadoopEmbeddedTaskExecutor extends GridHadoopTaskExecutorAdapter {
    /** Job tracker. */
    private GridHadoopJobTracker jobTracker;

    /** */
    private final ConcurrentMap<GridHadoopJobId, Collection<GridHadoopRunnableTask>> jobs = new ConcurrentHashMap<>();

    /** Tasks shared contexts. */
    private final ConcurrentMap<GridHadoopJobId, GridHadoopJobClassLoadingContext> ctxs = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        jobTracker = ctx.jobTracker();
    }

    /** {@inheritDoc} */
    @Override public void run(final GridHadoopJob job, Collection<GridHadoopTaskInfo> tasks) {
        if (log.isDebugEnabled())
            log.debug("Submitting tasks for local execution [locNodeId=" + ctx.localNodeId() +
                ", tasksCnt=" + tasks.size() + ']');

        Collection<GridHadoopRunnableTask> executedTasks = jobs.get(job.id());

        if (executedTasks == null) {
            executedTasks = new GridConcurrentHashSet<>();

            Collection<GridHadoopRunnableTask> extractedCol = jobs.put(job.id(), executedTasks);

            assert extractedCol == null;
        }

        GridHadoopJobClassLoadingContext clsLdrCtx = ctxs.get(job.id());

        final Collection<GridHadoopRunnableTask> finalExecutedTasks = executedTasks;

        for (final GridHadoopTaskInfo info : tasks) {
            assert info != null;

            GridHadoopRunnableTask task = new GridHadoopRunnableTask(log, job, ctx.shuffle().memory(), info, clsLdrCtx) {
                @Override protected void onTaskFinished(GridHadoopTaskState state, Throwable err) {
                    if (log.isDebugEnabled())
                        log.debug("Finished task execution [jobId=" + job.id() + ", taskInfo=" + info + ", " +
                                "waitTime=" + waitTime() + ", execTime=" + executionTime() + ']');

                    finalExecutedTasks.remove(this);

                    jobTracker.onTaskFinished(info, new GridHadoopTaskStatus(state, err));
                }

                @Override protected GridHadoopTaskInput createInput(GridHadoopTaskInfo info) throws GridException {
                    return ctx.shuffle().input(info);
                }

                @Override protected GridHadoopTaskOutput createOutput(GridHadoopTaskInfo info) throws GridException {
                    return ctx.shuffle().output(info);
                }
            };

            executedTasks.add(task);

            ctx.kernalContext().closure().callLocalSafe(task, false);
        }
    }

    /**
     * Cancels all currently running tasks for given job ID and cancels scheduled execution of tasks
     * for this job ID.
     * <p>
     * It is guaranteed that this method will not be called concurrently with
     * {@link #run(GridHadoopJob, Collection)} method. No more job submissions will be performed via
     * {@link #run(GridHadoopJob, Collection)} method for given job ID after this method is called.
     *
     * @param jobId Job ID to cancel.
     */
    @Override public void cancelTasks(GridHadoopJobId jobId) {
        Collection<GridHadoopRunnableTask> executedTasks = jobs.get(jobId);

        if (executedTasks != null) {
            for (GridHadoopRunnableTask task : executedTasks)
                task.cancel();
        }
    }

    /** {@inheritDoc} */
    @Override public void onJobStateChanged(GridHadoopJob job, GridHadoopJobMetadata meta) throws GridException {
        if (meta.phase() == GridHadoopJobPhase.PHASE_COMPLETE) {
            Collection<GridHadoopRunnableTask> executedTasks = jobs.remove(job.id());

            assert executedTasks == null || executedTasks.isEmpty();

            GridHadoopJobClassLoadingContext ctx = ctxs.remove(job.id());

            if (ctx != null)
                ctx.destroy();
        }
        else if (meta.phase() != GridHadoopJobPhase.PHASE_CANCELLING) {
            GridHadoopJobClassLoadingContext tctx = ctxs.get(job.id());

            if (tctx == null) {
                GridHadoopMapReducePlan plan = meta.mapReducePlan();

                UUID locNodeId = ctx.localNodeId();

                if (plan.mapperNodeIds().contains(locNodeId) || plan.reducerNodeIds().contains(locNodeId)) {
                    tctx = new GridHadoopJobClassLoadingContext(ctx.localNodeId(), job, log);

                    tctx.prepareJobFiles();
                    tctx.initializeClassLoader();

                    GridHadoopJobClassLoadingContext old = ctxs.putIfAbsent(job.id(), tctx);

                    assert old == null;
                }
            }
        }
    }
}
