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

package org.apache.ignite.internal.processors.hadoop.taskexecutor;

import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskCancelledException;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskOutput;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopPerformanceCounter;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopHashMultimap;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopMultimap;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopSkipList;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.COMBINER_HASHMAP_SIZE;
import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.SHUFFLE_COMBINER_NO_SORTING;
import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.get;
import static org.apache.ignite.internal.processors.hadoop.HadoopTaskType.COMBINE;
import static org.apache.ignite.internal.processors.hadoop.HadoopTaskType.MAP;

/**
 * Runnable task.
 */
public abstract class HadoopRunnableTask implements Callable<Void> {
    /** */
    private final GridUnsafeMemory mem;

    /** */
    private final IgniteLogger log;

    /** */
    private final HadoopJob job;

    /** Task to run. */
    private final HadoopTaskInfo info;

    /** Submit time. */
    private final long submitTs = U.currentTimeMillis();

    /** Execution start timestamp. */
    private long execStartTs;

    /** Execution end timestamp. */
    private long execEndTs;

    /** */
    private HadoopMultimap combinerInput;

    /** */
    private volatile HadoopTaskContext ctx;

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
    protected HadoopRunnableTask(IgniteLogger log, HadoopJob job, GridUnsafeMemory mem, HadoopTaskInfo info,
        UUID nodeId) {
        this.nodeId = nodeId;
        this.log = log.getLogger(HadoopRunnableTask.class);
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
        ctx = job.getTaskContext(info);

        return ctx.runAsJobOwner(new Callable<Void>() {
            @Override public Void call() throws Exception {
                call0();

                return null;
            }
        });
    }

    /**
     * Implements actual task running.
     * @throws IgniteCheckedException On error.
     */
    void call0() throws IgniteCheckedException {
        execStartTs = U.currentTimeMillis();

        Throwable err = null;

        HadoopTaskState state = HadoopTaskState.COMPLETED;

        HadoopPerformanceCounter perfCntr = null;

        try {
            perfCntr = HadoopPerformanceCounter.getCounter(ctx.counters(), nodeId);

            perfCntr.onTaskSubmit(info, submitTs);
            perfCntr.onTaskPrepare(info, execStartTs);

            ctx.prepareTaskEnvironment();

            runTask(perfCntr);

            if (info.type() == MAP && job.info().hasCombiner()) {
                // Switch to combiner.
                HadoopTaskInfo combineTaskInfo = new HadoopTaskInfo(COMBINE, info.jobId(), info.taskNumber(),
                    info.attempt(), null);

                // Mapper and combiner share the same index.
                if (ctx.taskInfo().hasMapperIndex())
                    combineTaskInfo.mapperIndex(ctx.taskInfo().mapperIndex());

                ctx.taskInfo(combineTaskInfo);

                try {
                    runTask(perfCntr);
                }
                finally {
                    ctx.taskInfo(info);
                }
            }
        }
        catch (HadoopTaskCancelledException ignored) {
            state = HadoopTaskState.CANCELED;
        }
        catch (Throwable e) {
            state = HadoopTaskState.FAILED;
            err = e;

            U.error(log, "Task execution failed.", e);

            if (e instanceof Error)
                throw e;
        }
        finally {
            execEndTs = U.currentTimeMillis();

            if (perfCntr != null)
                perfCntr.onTaskFinish(info, execEndTs);

            onTaskFinished(new HadoopTaskStatus(state, err, ctx==null ? null : ctx.counters()));

            if (combinerInput != null)
                combinerInput.close();

            if (ctx != null)
                ctx.cleanupTaskEnvironment();
        }
    }

    /**
     * @param perfCntr Performance counter.
     * @throws IgniteCheckedException If failed.
     */
    private void runTask(HadoopPerformanceCounter perfCntr) throws IgniteCheckedException {
        if (cancelled)
            throw new HadoopTaskCancelledException("Task cancelled.");

        try (HadoopTaskOutput out = createOutputInternal(ctx);
             HadoopTaskInput in = createInputInternal(ctx)) {

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
    protected abstract void onTaskFinished(HadoopTaskStatus status);

    /**
     * @param ctx Task context.
     * @return Task input.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private HadoopTaskInput createInputInternal(HadoopTaskContext ctx) throws IgniteCheckedException {
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
    protected abstract HadoopTaskInput createInput(HadoopTaskContext ctx) throws IgniteCheckedException;

    /**
     * @param ctx Task info.
     * @return Output.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract HadoopTaskOutput createOutput(HadoopTaskContext ctx) throws IgniteCheckedException;

    /**
     * @param ctx Task info.
     * @return Task output.
     * @throws IgniteCheckedException If failed.
     */
    private HadoopTaskOutput createOutputInternal(HadoopTaskContext ctx) throws IgniteCheckedException {
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
                        new HadoopHashMultimap(job.info(), mem, get(job.info(), COMBINER_HASHMAP_SIZE, 8 * 1024)):
                        new HadoopSkipList(job.info(), mem); // TODO replace with red-black tree

                    return combinerInput.startAdding(ctx);
                }

            default:
                return createOutput(ctx);
        }
    }

    /**
     * @return Task info.
     */
    public HadoopTaskInfo taskInfo() {
        return info;
    }
}