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
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;

import java.util.*;

/**
 * TODO write doc
 */
public class GridHadoopTaskExecutor extends GridHadoopComponent {
    /** Job tracker. */
    private GridHadoopJobTracker jobTracker;

    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        jobTracker = ctx.jobTracker();
    }

    /**
     * Runs tasks.
     *
     * @param tasks Tasks.
     */
    public void run(Collection<GridHadoopTask> tasks) {
        if (log.isDebugEnabled())
            log.debug("Submitting tasks for local execution [locNodeId=" + ctx.localNodeId() +
                ", tasksCnt=" + tasks.size() + ']');

        for (final GridHadoopTask task : tasks) {
            assert task != null;

            ctx.kernalContext().closure().callLocalSafe(new GridPlainCallable<GridFuture<?>>() {
                @Override public GridFuture<?> call() throws Exception {
                    GridHadoopTaskInfo info = task.info();

                    try (GridHadoopTaskOutput out = createOutput(info);
                         GridHadoopTaskInput in = createInput(info)) {
                        GridHadoopTaskContext taskCtx = new GridHadoopTaskContext(ctx.kernalContext());

                        try {
                            if (log.isDebugEnabled())
                                log.debug("Running task: " + task);

                            task.run(taskCtx);

                            return out.finish();
                        }
                        finally {
                            // TODO status.
                            jobTracker.onTaskFinished(task.info(), new GridHadoopTaskStatus());
                        }
                    }
                }
            }, false);
        }
    }

    /**
     * Creates task output.
     *
     * @param taskInfo Task info.
     * @return Task output.
     */
    private GridHadoopTaskOutput createOutput(GridHadoopTaskInfo taskInfo) {
        return new GridHadoopTaskOutput() {
            /** {@inheritDoc} */
            @Override public void write(Object key, Object val) {
                // TODO: implement.
            }

            /** {@inheritDoc} */
            @Override public GridFuture<?> finish() {
                return new GridFinishedFutureEx<>();
            }

            /** {@inheritDoc} */
            @Override public void close() throws Exception {
                // TODO: implement.
            }
        };
    }

    /**
     * Creates task input.
     *
     * @param taskInfo Task info.
     * @return Task input.
     */
    private GridHadoopTaskInput createInput(GridHadoopTaskInfo taskInfo) {
        return new GridHadoopTaskInput() {
            /** {@inheritDoc} */
            @Override public boolean next() {
                // TODO: implement.
                return false;
            }

            /** {@inheritDoc} */
            @Override public Object key() {
                // TODO: implement.
                return null;
            }

            /** {@inheritDoc} */
            @Override public Iterator<?> values() {
                // TODO: implement.
                return null;
            }

            /** {@inheritDoc} */
            @Override public void close() throws Exception {
                // TODO: implement.

            }
        };
    }
}
