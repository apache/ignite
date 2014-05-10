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
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

import static org.gridgain.grid.hadoop.GridHadoopTaskType.*;
import static org.gridgain.grid.kernal.processors.hadoop.taskexecutor.GridHadoopTaskState.*;

/**
 * TODO write doc
 */
public class GridHadoopEmbeddedTaskExecutor extends GridHadoopTaskExecutorAdapter {
    /** Job tracker. */
    private GridHadoopJobTracker jobTracker;

    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        jobTracker = ctx.jobTracker();
    }

    /** {@inheritDoc} */
    @Override public void run(final GridHadoopJob job, Collection<GridHadoopTaskInfo> tasks) {
        if (log.isDebugEnabled())
            log.debug("Submitting tasks for local execution [locNodeId=" + ctx.localNodeId() +
                ", tasksCnt=" + tasks.size() + ']');

        for (final GridHadoopTaskInfo info : tasks) {
            assert info != null;

            ctx.kernalContext().closure().callLocalSafe(new GridPlainCallable<GridFuture<?>>() {
                @Override public GridFuture<?> call() throws Exception {
                    try (GridHadoopTaskOutput out = createOutput(info);
                         GridHadoopTaskInput in = createInput(info)) {
                        GridHadoopTaskContext taskCtx = new GridHadoopTaskContext(ctx.kernalContext(), job, in, out);

                        GridHadoopTask task = job.createTask(info);

                        if (log.isDebugEnabled())
                            log.debug("Running task: " + task);

                        task.run(taskCtx);
                    }
                    catch (Exception e) {
                        U.error(log, "Failed to execute task: " + info, e);

                        throw e;
                    }

                    return null;
                }
            }, false).listenAsync(new CIX1<GridFuture<?>>() {
                @Override public void applyx(GridFuture<?> f) throws GridException {
                    GridHadoopTaskState state = COMPLETED;
                    Throwable err = null;

                    try {
                        f.get();
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

    /** {@inheritDoc} */
    @Override public void cancelTasks(GridHadoopJobId jobId) {
        // TODO.
    }

    /** {@inheritDoc} */
    @Override public void onJobStateChanged(GridHadoopJobId jobId, GridHadoopJobMetadata meta) {
        // No-op.
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
