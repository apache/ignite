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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.internal.processors.platform.client.ClientMessageParser.OP_COMPUTE_TASK_FINISHED;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_NO_FAILOVER;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_NO_RESULT_CACHE;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SUBGRID_PREDICATE;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SUBJ_ID;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_TIMEOUT;

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

    /**
     * Ctor.
     *
     * @param ctx Connection context.
     */
    ClientComputeTask(ClientConnectionContext ctx) {
        assert ctx != null;

        this.ctx = ctx;

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

        IgnitePredicate<ClusterNode> nodePredicate = F.isEmpty(nodeIds) ? node -> !node.isClient() :
            F.nodeForNodeIds(nodeIds);

        UUID subjId = ctx.securityContext() == null ? null : ctx.securityContext().subject().id();

        task.setThreadContext(TC_SUBGRID_PREDICATE, nodePredicate);
        task.setThreadContextIfNotNull(TC_SUBJ_ID, subjId);
        task.setThreadContext(TC_TIMEOUT, timeout);
        task.setThreadContext(TC_NO_FAILOVER, (flags & NO_FAILOVER_FLAG_MASK) != 0);
        task.setThreadContext(TC_NO_RESULT_CACHE, (flags & NO_RESULT_CACHE_FLAG_MASK) != 0);

        taskFut = task.execute(taskName, arg);

        // Fail fast.
        if (taskFut.isDone() && taskFut.error() != null)
            throw new IgniteClientException(ClientStatus.FAILED, taskFut.error().getMessage());
    }

    /**
     * Callback for response sent event.
     */
    void onResponseSent() {
        // Listener should be registered only after response for this task was sent, to ensure that client doesn't
        // receive notification before response for the task.
        taskFut.listen(f -> {
            try {
                ClientNotification notification;

                if (f.error() != null)
                    notification = new ClientNotification(OP_COMPUTE_TASK_FINISHED, taskId, f.error().getMessage());
                else if (f.isCancelled())
                    notification = new ClientNotification(OP_COMPUTE_TASK_FINISHED, taskId, "Task was cancelled");
                else
                    notification = new ClientObjectNotification(OP_COMPUTE_TASK_FINISHED, taskId, f.result());

                ctx.notifyClient(notification);
            }
            finally {
                // If task was explicitly closed before, resource is already released.
                if (closed.compareAndSet(false, true)) {
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
