/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.*;


/**
 * Task executor.
 */
public class GridHadoopEmbeddedTaskExecutor extends GridHadoopTaskExecutorAdapter {
    /** Job tracker. */
    private GridHadoopJobTracker jobTracker;

    /** */
    private final ConcurrentMap<GridHadoopJobId, Collection<GridHadoopRunnableTask>> jobs = new ConcurrentHashMap<>();

    /** Executor service to run tasks. */
    private GridHadoopExecutorService exec;

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        super.onKernalStart();

        jobTracker = ctx.jobTracker();

        exec = new GridHadoopExecutorService(log, ctx.kernalContext().gridName(),
            ctx.configuration().getMaxParallelTasks(), ctx.configuration().getMaxTaskQueueSize());
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (exec != null) {
            exec.shutdown(3000);

            if (cancel) {
                for (GridHadoopJobId jobId : jobs.keySet())
                    cancelTasks(jobId);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        if (exec != null && !exec.shutdown(30000))
            U.warn(log, "Failed to finish running tasks in 30 sec.");
    }

    /** {@inheritDoc} */
    @Override public void run(final GridHadoopJob job, Collection<GridHadoopTaskInfo> tasks) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Submitting tasks for local execution [locNodeId=" + ctx.localNodeId() +
                ", tasksCnt=" + tasks.size() + ']');

        Collection<GridHadoopRunnableTask> executedTasks = jobs.get(job.id());

        if (executedTasks == null) {
            executedTasks = new GridConcurrentHashSet<>();

            Collection<GridHadoopRunnableTask> extractedCol = jobs.put(job.id(), executedTasks);

            assert extractedCol == null;
        }

        final Collection<GridHadoopRunnableTask> finalExecutedTasks = executedTasks;

        for (final GridHadoopTaskInfo info : tasks) {
            assert info != null;

            GridHadoopRunnableTask task = new GridHadoopRunnableTask(log, job, ctx.shuffle().memory(), info,
                ctx.localNodeId()) {
                @Override protected void onTaskFinished(GridHadoopTaskStatus status) {
                    if (log.isDebugEnabled())
                        log.debug("Finished task execution [jobId=" + job.id() + ", taskInfo=" + info + ", " +
                            "waitTime=" + waitTime() + ", execTime=" + executionTime() + ']');

                    finalExecutedTasks.remove(this);

                    jobTracker.onTaskFinished(info, status);
                }

                @Override protected GridHadoopTaskInput createInput(GridHadoopTaskContext taskCtx) throws IgniteCheckedException {
                    return ctx.shuffle().input(taskCtx);
                }

                @Override protected GridHadoopTaskOutput createOutput(GridHadoopTaskContext taskCtx) throws IgniteCheckedException {
                    return ctx.shuffle().output(taskCtx);
                }
            };

            executedTasks.add(task);

            exec.submit(task);
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
    @Override public void onJobStateChanged(GridHadoopJobMetadata meta) throws IgniteCheckedException {
        if (meta.phase() == GridHadoopJobPhase.PHASE_COMPLETE) {
            Collection<GridHadoopRunnableTask> executedTasks = jobs.remove(meta.jobId());

            assert executedTasks == null || executedTasks.isEmpty();
        }
    }
}
