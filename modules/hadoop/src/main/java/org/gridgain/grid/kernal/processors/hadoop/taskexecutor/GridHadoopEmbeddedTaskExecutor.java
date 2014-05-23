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
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.util.concurrent.*;
import java.util.*;

import static org.gridgain.grid.hadoop.GridHadoopJobProperty.*;
import static org.gridgain.grid.hadoop.GridHadoopTaskType.*;
import static org.gridgain.grid.kernal.processors.hadoop.taskexecutor.GridHadoopTaskState.*;

/**
 * Task executor.
 */
public class GridHadoopEmbeddedTaskExecutor extends GridHadoopTaskExecutorAdapter {
    /** Job tracker. */
    private GridHadoopJobTracker jobTracker;

    /** */
    private final ConcurrentMap<GridHadoopJobId, Collection<GridFuture<?>>> jobs = new ConcurrentHashMap<>();

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

        Collection<GridFuture<?>> futures = jobs.get(job.id());

        if (futures == null) {
            futures = new GridConcurrentHashSet<>();

            Collection<GridFuture<?>> extractedCol = jobs.put(job.id(), futures);

            assert extractedCol == null;
        }

        // Are we going to run combiner in the same thread as mapper?
        final boolean runCombiner = job.hasCombiner() && !get(job, SINGLE_COMBINER_FOR_ALL_MAPPERS, false);

        for (final GridHadoopTaskInfo info : tasks) {
            assert info != null;

            GridFuture<GridFuture<?>> fut = ctx.kernalContext().closure().callLocalSafe(
                new GridPlainCallable<GridFuture<?>>() {
                @Override public GridFuture<?> call() throws Exception {
                    runTask(job, info); // Mapper.

                    if (runCombiner) // Combiner.
                        runTask(job, new GridHadoopTaskInfo(info.nodeId(), COMBINE, info.jobId(), info.taskNumber(),
                            info.attempt(), null));

                    return null;
                }
            }, false);

            futures.add(fut);

            fut.listenAsync(new CIX1<GridFuture<?>>() {
                @Override public void applyx(GridFuture<?> f) {
                    Collection<GridFuture<?>> futs = jobs.get(info.jobId());

                    futs.remove(f);

                    GridHadoopTaskState state = COMPLETED;
                    Throwable err = null;

                    try {
                        f.get();
                    }
                    catch (GridFutureCancelledException ignored) {
                        state = CANCELED;
                    }
                    catch (Throwable e) {
                        state = FAILED;
                        err = e;
                    }

                    jobTracker.onTaskFinished(info, new GridHadoopTaskStatus(state, err));
                }
            });
        }
    }

    /**
     * @param job Job.
     * @param info Task info.
     */
    private void runTask(final GridHadoopJob job, final GridHadoopTaskInfo info) throws Exception {
        long start = System.currentTimeMillis();

        ClassLoader old = GridHadoopJobClassLoadingContext.prepareClassLoader(ctxs.get(job.id()), job.info());

        try (GridHadoopTaskOutput out = createOutput(info);
             GridHadoopTaskInput in = createInput(info)) {
            GridHadoopTaskContext taskCtx = new GridHadoopTaskContext(job, in, out);

            GridHadoopTask task = job.createTask(info);

            if (log.isDebugEnabled())
                log.debug("Running task: " + task);

            task.run(taskCtx);
        }
        catch (Exception e) {
            U.error(log, "Failed to execute task: " + info, e);

            throw e;
        }
        finally {
            Thread.currentThread().setContextClassLoader(old);

            long end = System.currentTimeMillis();

            if (log.isDebugEnabled())
                log.debug("Finished task execution [jobId=" + job.id() + ", taskInfo=" + info + ", " +
                    "execTime=" + (end - start) + ']');
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
        Collection<GridFuture<?>> futures = jobs.get(jobId);

        if (futures != null) {
            for (GridFuture<?> f : futures) {
                try {
                    f.cancel();
                }
                catch (GridException e) {
                    log.error("Future cancelling failed", e);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onJobStateChanged(GridHadoopJob job, GridHadoopJobMetadata meta) throws GridException {
        if (meta.phase() == GridHadoopJobPhase.PHASE_COMPLETE) {
            Collection<GridFuture<?>> futures = jobs.remove(job.id());

            assert futures == null || futures.isEmpty();

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

    /**
     * Creates task output.
     *
     * @param taskInfo Task info.
     * @return Task output.
     */
    private GridHadoopTaskOutput createOutput(GridHadoopTaskInfo taskInfo) throws GridException {
        if (taskInfo.type() == REDUCE || taskInfo.type() == COMMIT || taskInfo.type() == ABORT)
            return null;

        return ctx.shuffle().output(taskInfo);
    }

    /**
     * Creates task input.
     *
     * @param taskInfo Task info.
     * @return Task input.
     */
    private GridHadoopTaskInput createInput(GridHadoopTaskInfo taskInfo) throws GridException {
        if (taskInfo.type() == MAP || taskInfo.type() == COMMIT || taskInfo.type() == ABORT)
            return null;

        return ctx.shuffle().input(taskInfo);
    }
}
