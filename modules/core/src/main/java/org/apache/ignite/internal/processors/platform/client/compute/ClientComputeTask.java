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

package org.apache.ignite.internal.processors.platform.client.compute;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ComputeTaskInternalFuture;
import org.apache.ignite.internal.processors.platform.client.ClientCloseableResource;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientNotification;
import org.apache.ignite.internal.processors.platform.client.ClientObjectNotification;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.processors.task.GridTaskProcessor;
import org.apache.ignite.internal.processors.task.TaskExecutionOptions;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.internal.processors.platform.client.ClientMessageParser.OP_COMPUTE_TASK_FINISHED;
import static org.apache.ignite.internal.processors.task.TaskExecutionOptions.options;
import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.nodeForNodeIds;

/**
 * Client compute task.
  */
class ClientComputeTask implements ClientCloseableResource {
    /** No failover flag mask. */
    private static final byte NO_FAILOVER_FLAG_MASK = 0x01;

    /** No result cache flag mask. */
    private static final byte NO_RESULT_CACHE_FLAG_MASK = 0x02;

    /** Keep binary flag mask. */
    public static final byte KEEP_BINARY_FLAG_MASK = 0x04;

    /** Context. */
    private final ClientConnectionContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Task id. */
    private volatile long taskId;

    /** Task future. */
    private volatile ComputeTaskInternalFuture<Object> taskFut;

    /** Task closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** */
    private final boolean systemTask;

    /**
     * Ctor.
     *
     * @param ctx Connection context.
     * @param sysTask {@code True} if task is system.
     */
    ClientComputeTask(ClientConnectionContext ctx, boolean sysTask) {
        assert ctx != null;

        this.ctx = ctx;
        this.systemTask = sysTask;

        log = ctx.kernalContext().log(getClass());
    }

    /**
     * @param taskId Task ID.
     * @param taskName Task name.
     * @param arg Task arguments.
     * @param nodeIds Nodes to run task jobs.
     * @param flags Flags for task.
     * @param timeout Task timeout.
     */
    void execute(long taskId, String taskName, Object arg, Set<UUID> nodeIds, byte flags, long timeout) {
        assert taskName != null;

        this.taskId = taskId;

        GridTaskProcessor task = ctx.kernalContext().task();

        IgnitePredicate<ClusterNode> nodePredicate = F.isEmpty(nodeIds) ? node -> !node.isClient() : nodeForNodeIds(nodeIds);

        TaskExecutionOptions opts = options()
            .asPublicRequest()
            .withProjectionPredicate(nodePredicate)
            .withTimeout(timeout);

        if ((flags & NO_FAILOVER_FLAG_MASK) != 0)
            opts.withFailoverDisabled();

        if ((flags & NO_RESULT_CACHE_FLAG_MASK) != 0)
            opts.withResultCacheDisabled();

        taskFut = task.execute(taskName, arg, opts);

        // Fail fast.
        if (taskFut.isDone() && taskFut.error() != null) {
            if (ctx.kernalContext().clientListener().sendServerExceptionStackTraceToClient())
                throw new IgniteClientException(ClientStatus.FAILED, taskFut.error().getMessage(), taskFut.error());
            else
                throw new IgniteClientException(ClientStatus.FAILED, taskFut.error().getMessage());
        }
    }

    /**
     * Callback for response sent event.
     */
    void onResponseSent() {
        // Listener should be registered only after response for this task was sent, to ensure that client doesn't
        // receive notification before response for the task.
        taskFut.listen(() -> {
            try {
                ClientNotification notification;

                if (taskFut.error() != null) {
                    String msg = ctx.kernalContext().clientListener().sendServerExceptionStackTraceToClient()
                            ? taskFut.error().getMessage() + U.nl() + X.getFullStackTrace(taskFut.error())
                            : taskFut.error().getMessage();

                    notification = new ClientNotification(OP_COMPUTE_TASK_FINISHED, taskId, msg);
                }
                else if (taskFut.isCancelled())
                    notification = new ClientNotification(OP_COMPUTE_TASK_FINISHED, taskId, "Task was cancelled");
                else
                    notification = new ClientObjectNotification(OP_COMPUTE_TASK_FINISHED, taskId, taskFut.result());

                ctx.notifyClient(notification);
            }
            finally {
                // If task was explicitly closed before, resource is already released.
                if (closed.compareAndSet(false, true)) {
                    if (!systemTask)
                        ctx.decrementActiveTasksCount();

                    ctx.resources().release(taskId);
                }
            }
        });
    }

    /**
     * Gets task ID.
     */
    long taskId() {
        return taskId;
    }

    /**
     * Closes the task resource.
     */
    @Override public void close() {
        if (closed.compareAndSet(false, true)) {
            if (!systemTask)
                ctx.decrementActiveTasksCount();

            try {
                if (taskFut != null)
                    taskFut.cancel();
            }
            catch (IgniteCheckedException e) {
                log.warning("Failed to cancel task", e);
            }
        }
    }
}
