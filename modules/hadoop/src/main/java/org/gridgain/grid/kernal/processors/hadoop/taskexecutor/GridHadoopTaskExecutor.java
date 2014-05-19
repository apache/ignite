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
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.util.concurrent.*;
import java.util.*;

import static org.gridgain.grid.hadoop.GridHadoopTaskType.*;
import static org.gridgain.grid.kernal.processors.hadoop.taskexecutor.GridHadoopTaskState.*;

/**
 * Task executor.
 */
public class GridHadoopTaskExecutor extends GridHadoopComponent {
    /** Job tracker. */
    private GridHadoopJobTracker jobTracker;

    /** */
    private final ConcurrentMap<GridHadoopJobId, Collection<GridFuture<?>>> jobs = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        jobTracker = ctx.jobTracker();
    }

    /**
     * Runs tasks.
     *
     * @param job Job.
     * @param tasks Tasks.
     */
    public void run(final GridHadoopJob job, Collection<GridHadoopTask> tasks) {
        if (log.isDebugEnabled())
            log.debug("Submitting tasks for local execution [locNodeId=" + ctx.localNodeId() +
                ", tasksCnt=" + tasks.size() + ']');

        for (Map.Entry<GridHadoopJobId, Collection<GridFuture<?>>> entry : jobs.entrySet()) {
            if (entry.getValue().isEmpty())
                jobs.remove(entry.getKey());
        }

        Collection<GridFuture<?>> futures = jobs.get(job.id());

        if (futures == null) {
            futures = new GridConcurrentHashSet<>();

            Collection<GridFuture<?>> extractedCol = jobs.put(job.id(), futures);

            assert extractedCol == null;
        }

        for (final GridHadoopTask task : tasks) {
            assert task != null;

            GridFuture<GridFuture<?>> fut = ctx.kernalContext().closure().callLocalSafe(new GridPlainCallable<GridFuture<?>>() {
                @Override public GridFuture<?> call() throws Exception {
                    GridHadoopTaskInfo info = task.info();

                    try (GridHadoopTaskOutput out = createOutput(info);
                         GridHadoopTaskInput in = createInput(info)) {
                        GridHadoopTaskContext taskCtx = new GridHadoopTaskContext(ctx.kernalContext(), job, in, out);

                        if (log.isDebugEnabled())
                            log.debug("Running task: " + task);

                        task.run(taskCtx);
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to execute task: " + task.info(), e);

                        throw e;
                    }

                    return null;
                }
            }, false);

            futures.add(fut);

            fut.listenAsync(new CIX1<GridFuture<?>>() {
                @Override public void applyx(GridFuture<?> f) throws GridException {
                    Collection<GridFuture<?>> futs = jobs.get(task.info().jobId());

                    futs.remove(f);

                    GridHadoopTaskState state = COMPLETED;
                    Throwable err = null;

                    try {
                        f.get();
                    }
                    catch (GridFutureCancelledException e) {
                        state = CANCELED;
                    }
                    catch (Throwable e) {
                        state = FAILED;
                        err = e;
                    }

                    jobTracker.onTaskFinished(task.info(), new GridHadoopTaskStatus(state, err));
                }
            });
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
    public void cancelTasks(GridHadoopJobId jobId) {
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
