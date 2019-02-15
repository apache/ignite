/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop.taskexecutor;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopJobEx;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobPhase;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskOutput;
import org.apache.ignite.internal.processors.hadoop.jobtracker.HadoopJobMetadata;
import org.apache.ignite.internal.processors.hadoop.jobtracker.HadoopJobTracker;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;


/**
 * Task executor.
 */
public class HadoopEmbeddedTaskExecutor extends HadoopTaskExecutorAdapter {
    /** Job tracker. */
    private HadoopJobTracker jobTracker;

    /** */
    private final ConcurrentMap<HadoopJobId, Collection<HadoopRunnableTask>> jobs = new ConcurrentHashMap<>();

    /** Executor service to run tasks. */
    private HadoopExecutorService exec;

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        super.onKernalStart();

        jobTracker = ctx.jobTracker();

        exec = new HadoopExecutorService(log, ctx.kernalContext().igniteInstanceName(),
            ctx.configuration().getMaxParallelTasks(), ctx.configuration().getMaxTaskQueueSize());
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (exec != null) {
            exec.shutdown(3000);

            if (cancel) {
                for (HadoopJobId jobId : jobs.keySet())
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
    @Override public void run(final HadoopJobEx job, Collection<HadoopTaskInfo> tasks) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Submitting tasks for local execution [locNodeId=" + ctx.localNodeId() +
                ", tasksCnt=" + tasks.size() + ']');

        Collection<HadoopRunnableTask> executedTasks = jobs.get(job.id());

        if (executedTasks == null) {
            executedTasks = new GridConcurrentHashSet<>();

            Collection<HadoopRunnableTask> extractedCol = jobs.put(job.id(), executedTasks);

            assert extractedCol == null;
        }

        final Collection<HadoopRunnableTask> finalExecutedTasks = executedTasks;

        for (final HadoopTaskInfo info : tasks) {
            assert info != null;

            HadoopRunnableTask task = new HadoopRunnableTask(log, job, ctx.shuffle().memory(), info,
                ctx.localNodeId()) {
                @Override protected void onTaskFinished(HadoopTaskStatus status) {
                    if (log.isDebugEnabled())
                        log.debug("Finished task execution [jobId=" + job.id() + ", taskInfo=" + info + ", " +
                            "waitTime=" + waitTime() + ", execTime=" + executionTime() + ']');

                    finalExecutedTasks.remove(this);

                    jobTracker.onTaskFinished(info, status);
                }

                @Override protected HadoopTaskInput createInput(HadoopTaskContext taskCtx) throws IgniteCheckedException {
                    return ctx.shuffle().input(taskCtx);
                }

                @Override protected HadoopTaskOutput createOutput(HadoopTaskContext taskCtx) throws IgniteCheckedException {
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
     * {@link #run(HadoopJobEx, Collection)} method. No more job submissions will be performed via
     * {@link #run(HadoopJobEx, Collection)} method for given job ID after this method is called.
     *
     * @param jobId Job ID to cancel.
     */
    @Override public void cancelTasks(HadoopJobId jobId) {
        Collection<HadoopRunnableTask> executedTasks = jobs.get(jobId);

        if (executedTasks != null) {
            for (HadoopRunnableTask task : executedTasks)
                task.cancel();
        }
    }

    /** {@inheritDoc} */
    @Override public void onJobStateChanged(HadoopJobMetadata meta) throws IgniteCheckedException {
        if (meta.phase() == HadoopJobPhase.PHASE_COMPLETE) {
            Collection<HadoopRunnableTask> executedTasks = jobs.remove(meta.jobId());

            assert executedTasks == null || executedTasks.isEmpty();
        }
    }
}