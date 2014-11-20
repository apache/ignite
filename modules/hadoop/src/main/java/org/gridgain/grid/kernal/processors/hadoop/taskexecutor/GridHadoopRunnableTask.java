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
import org.gridgain.grid.kernal.processors.hadoop.counter.*;
import org.gridgain.grid.kernal.processors.hadoop.shuffle.collections.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.UUID;
import java.util.concurrent.*;

import static org.gridgain.grid.hadoop.GridHadoopJobProperty.*;
import static org.gridgain.grid.hadoop.GridHadoopTaskType.*;

/**
 * Runnable task.
 */
public abstract class GridHadoopRunnableTask implements Callable<Void> {
    /** */
    private final GridUnsafeMemory mem;

    /** */
    private final GridLogger log;

    /** */
    private final GridHadoopJob job;

    /** Task to run. */
    private final GridHadoopTaskInfo info;

    /** Submit time. */
    private final long submitTs = U.currentTimeMillis();

    /** Execution start timestamp. */
    private long execStartTs;

    /** Execution end timestamp. */
    private long execEndTs;

    /** */
    private GridHadoopMultimap local;

    /** */
    private volatile GridHadoopTaskContext ctx;

    /** Set if task is to cancelling. */
    private volatile boolean cancelled;

    /** Task part of the job statistics. */
    private GridHadoopJobStatistics stats;

    /**
     * @param log Log.
     * @param job Job.
     * @param mem Memory.
     * @param info Task info.
     * @param nodeId Node id.
     */
    protected GridHadoopRunnableTask(GridLogger log, GridHadoopJob job, GridUnsafeMemory mem, GridHadoopTaskInfo info,
        UUID nodeId) {
        this.log = log.getLogger(GridHadoopRunnableTask.class);
        this.job = job;
        this.mem = mem;
        this.info = info;

        stats = new GridHadoopJobStatistics(nodeId);
    }

    /**
     * @return Wait time.
     */
    public long waitTime() {
        return execStartTs - submitTs;
    }

    /**
     * @return Execution time.
     */
    public long executionTime() {
        return execEndTs - execStartTs;
    }

    /** {@inheritDoc} */
    @Override public Void call() throws GridException {
        execStartTs = U.currentTimeMillis();

        stats.onTaskSubmit(info, submitTs);
        stats.onTaskPrepare(info, execStartTs);

        Throwable err = null;

        GridHadoopTaskState state = GridHadoopTaskState.COMPLETED;

        try {
            ctx = job.getTaskContext(info);

            ctx.prepareTaskEnvironment();

            runTask();

            if (info.type() == MAP && job.info().hasCombiner()) {
                ctx.taskInfo(new GridHadoopTaskInfo(COMBINE, info.jobId(), info.taskNumber(), info.attempt(), null));

                try {
                    runTask();
                }
                finally {
                    ctx.taskInfo(info);
                }
            }
        }
        catch (GridHadoopTaskCancelledException ignored) {
            state = GridHadoopTaskState.CANCELED;
        }
        catch (Throwable e) {
            state = GridHadoopTaskState.FAILED;
            err = e;

            U.error(log, "Task execution failed.", e);
        }
        finally {
            execEndTs = U.currentTimeMillis();

            stats.onTaskFinish(info, execEndTs);

            GridHadoopCounters counters = ctx.counters();

            counters.counter(GridHadoopStatCounter.GROUP_NAME, GridHadoopStatCounter.COUNTER_NAME,
                GridHadoopStatCounter.class).append(stats);

            onTaskFinished(new GridHadoopTaskStatus(state, err, counters));

            if (local != null)
                local.close();

            if (ctx != null)
                ctx.cleanupTaskEnvironment();
        }

        return null;
    }

    /**
     * @throws GridException If failed.
     */
    private void runTask() throws GridException {
        if (cancelled)
            throw new GridHadoopTaskCancelledException("Task cancelled.");

        try (GridHadoopTaskOutput out = createOutputInternal(ctx);
             GridHadoopTaskInput in = createInputInternal(ctx)) {

            ctx.input(in);
            ctx.output(out);

            stats.onTaskRun(ctx.taskInfo(), U.currentTimeMillis());

            ctx.run();
        }
    }

    /**
     * Cancel the executed task.
     */
    public void cancel() {
        cancelled = true;

        if (ctx != null)
            ctx.cancel();
    }

    /**
     * @param status Task status.
     */
    protected abstract void onTaskFinished(GridHadoopTaskStatus status);

    /**
     * @param ctx Task context.
     * @return Task input.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    private GridHadoopTaskInput createInputInternal(GridHadoopTaskContext ctx) throws GridException {
        switch (ctx.taskInfo().type()) {
            case SETUP:
            case MAP:
            case COMMIT:
            case ABORT:
                return null;

            case COMBINE:
                assert local != null;

                return local.input(ctx);

            default:
                return createInput(ctx);
        }
    }

    /**
     * @param ctx Task context.
     * @return Input.
     * @throws GridException If failed.
     */
    protected abstract GridHadoopTaskInput createInput(GridHadoopTaskContext ctx) throws GridException;

    /**
     * @param ctx Task info.
     * @return Output.
     * @throws GridException If failed.
     */
    protected abstract GridHadoopTaskOutput createOutput(GridHadoopTaskContext ctx) throws GridException;

    /**
     * @param ctx Task info.
     * @return Task output.
     * @throws GridException If failed.
     */
    private GridHadoopTaskOutput createOutputInternal(GridHadoopTaskContext ctx) throws GridException {
        switch (ctx.taskInfo().type()) {
            case SETUP:
            case REDUCE:
            case COMMIT:
            case ABORT:
                return null;

            case MAP:
                if (job.info().hasCombiner()) {
                    assert local == null;

                    local = get(job.info(), SHUFFLE_COMBINER_NO_SORTING, false) ?
                        new GridHadoopHashMultimap(job.info(), mem, get(job.info(), COMBINER_HASHMAP_SIZE, 8 * 1024)):
                        new GridHadoopSkipList(job.info(), mem); // TODO replace with red-black tree

                    return local.startAdding(ctx);
                }

            default:
                return createOutput(ctx);
        }
    }

    /**
     * @return Task info.
     */
    public GridHadoopTaskInfo taskInfo() {
        return info;
    }
}
