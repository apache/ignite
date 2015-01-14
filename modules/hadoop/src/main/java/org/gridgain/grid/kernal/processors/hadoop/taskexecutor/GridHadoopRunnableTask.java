/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.counter.*;
import org.gridgain.grid.kernal.processors.hadoop.shuffle.collections.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;
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
    private final IgniteLogger log;

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
    private GridHadoopMultimap combinerInput;

    /** */
    private volatile GridHadoopTaskContext ctx;

    /** Set if task is to cancelling. */
    private volatile boolean cancelled;

    /** Node id. */
    private UUID nodeId;

    /**
     * @param log Log.
     * @param job Job.
     * @param mem Memory.
     * @param info Task info.
     * @param nodeId Node id.
     */
    protected GridHadoopRunnableTask(IgniteLogger log, GridHadoopJob job, GridUnsafeMemory mem, GridHadoopTaskInfo info,
        UUID nodeId) {
        this.nodeId = nodeId;
        this.log = log.getLogger(GridHadoopRunnableTask.class);
        this.job = job;
        this.mem = mem;
        this.info = info;
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
    @Override public Void call() throws IgniteCheckedException {
        execStartTs = U.currentTimeMillis();

        Throwable err = null;

        GridHadoopTaskState state = GridHadoopTaskState.COMPLETED;

        GridHadoopPerformanceCounter perfCntr = null;

        try {
            ctx = job.getTaskContext(info);

            perfCntr = GridHadoopPerformanceCounter.getCounter(ctx.counters(), nodeId);

            perfCntr.onTaskSubmit(info, submitTs);
            perfCntr.onTaskPrepare(info, execStartTs);

            ctx.prepareTaskEnvironment();

            runTask(perfCntr);

            if (info.type() == MAP && job.info().hasCombiner()) {
                ctx.taskInfo(new GridHadoopTaskInfo(COMBINE, info.jobId(), info.taskNumber(), info.attempt(), null));

                try {
                    runTask(perfCntr);
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

            if (perfCntr != null)
                perfCntr.onTaskFinish(info, execEndTs);

            onTaskFinished(new GridHadoopTaskStatus(state, err, ctx==null ? null : ctx.counters()));

            if (combinerInput != null)
                combinerInput.close();

            if (ctx != null)
                ctx.cleanupTaskEnvironment();
        }

        return null;
    }

    /**
     * @param perfCntr Performance counter.
     * @throws IgniteCheckedException If failed.
     */
    private void runTask(GridHadoopPerformanceCounter perfCntr) throws IgniteCheckedException {
        if (cancelled)
            throw new GridHadoopTaskCancelledException("Task cancelled.");

        try (GridHadoopTaskOutput out = createOutputInternal(ctx);
             GridHadoopTaskInput in = createInputInternal(ctx)) {

            ctx.input(in);
            ctx.output(out);

            perfCntr.onTaskStart(ctx.taskInfo(), U.currentTimeMillis());

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
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private GridHadoopTaskInput createInputInternal(GridHadoopTaskContext ctx) throws IgniteCheckedException {
        switch (ctx.taskInfo().type()) {
            case SETUP:
            case MAP:
            case COMMIT:
            case ABORT:
                return null;

            case COMBINE:
                assert combinerInput != null;

                return combinerInput.input(ctx);

            default:
                return createInput(ctx);
        }
    }

    /**
     * @param ctx Task context.
     * @return Input.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract GridHadoopTaskInput createInput(GridHadoopTaskContext ctx) throws IgniteCheckedException;

    /**
     * @param ctx Task info.
     * @return Output.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract GridHadoopTaskOutput createOutput(GridHadoopTaskContext ctx) throws IgniteCheckedException;

    /**
     * @param ctx Task info.
     * @return Task output.
     * @throws IgniteCheckedException If failed.
     */
    private GridHadoopTaskOutput createOutputInternal(GridHadoopTaskContext ctx) throws IgniteCheckedException {
        switch (ctx.taskInfo().type()) {
            case SETUP:
            case REDUCE:
            case COMMIT:
            case ABORT:
                return null;

            case MAP:
                if (job.info().hasCombiner()) {
                    assert combinerInput == null;

                    combinerInput = get(job.info(), SHUFFLE_COMBINER_NO_SORTING, false) ?
                        new GridHadoopHashMultimap(job.info(), mem, get(job.info(), COMBINER_HASHMAP_SIZE, 8 * 1024)):
                        new GridHadoopSkipList(job.info(), mem); // TODO replace with red-black tree

                    return combinerInput.startAdding(ctx);
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
