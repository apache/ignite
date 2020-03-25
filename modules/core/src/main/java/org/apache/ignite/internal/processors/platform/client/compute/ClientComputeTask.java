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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ComputeTaskInternalFuture;
import org.apache.ignite.internal.processors.platform.client.ClientCloseableResource;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientNotification;
import org.apache.ignite.internal.processors.platform.client.ClientObjectNotification;
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

    /** Context. */
    private final ClientConnectionContext ctx;

    /** Task name. */
    private final String taskName;

    /** Task argument. */
    private final Object arg;

    /** Node ids. */
    private final Set<UUID> nodeIds;

    /** Flags. */
    private final byte flags;

    /** Timeout. */
    private final long timeout;

    /** Task future. */
    private volatile ComputeTaskInternalFuture<Object> taskFut;

    /** Task closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Ctor.
     *
     * @param ctx Connection context.
     * @param taskName Task name.
     * @param arg Task arguments.
     * @param nodeIds Nodes to run task jobs.
     * @param flags Flags for task.
     * @param timeout Task timeout.
     */
    ClientComputeTask(ClientConnectionContext ctx, String taskName, Object arg, Set<UUID> nodeIds, byte flags, long timeout) {
        assert ctx != null;
        assert taskName != null;

        this.ctx = ctx;
        this.taskName = taskName;
        this.arg = arg;
        this.nodeIds = nodeIds;
        this.flags = flags;
        this.timeout = timeout;
    }

    /**
     * @param taskId Task id.
     */
    void execute(long taskId) {
        GridTaskProcessor task = ctx.kernalContext().task();

        IgnitePredicate<ClusterNode> nodePredicate = F.isEmpty(nodeIds) ? F.alwaysTrue() : F.nodeForNodeIds(nodeIds);
        UUID subjId = ctx.securityContext() == null ? null : ctx.securityContext().subject().id();

        task.setThreadContextIfNotNull(TC_SUBGRID_PREDICATE, nodePredicate);
        task.setThreadContextIfNotNull(TC_SUBJ_ID, subjId);
        task.setThreadContext(TC_TIMEOUT, timeout);
        task.setThreadContext(TC_NO_FAILOVER, (flags & NO_FAILOVER_FLAG_MASK) != 0);
        task.setThreadContext(TC_NO_RESULT_CACHE, (flags & NO_RESULT_CACHE_FLAG_MASK) != 0);

        taskFut = task.execute(taskName, arg);

        taskFut.listen(f -> {
            try {
                ClientNotification notification;

                try {
                    notification = new ClientObjectNotification(OP_COMPUTE_TASK_FINISHED, taskId, f.get());
                }
                catch (Throwable e) {
                    notification = new ClientNotification(OP_COMPUTE_TASK_FINISHED, taskId, e.getMessage());
                }

                ctx.notifyClient(notification);
            }
            finally {
                if (!closed.get()) // If task was explicitly closed before, resource is already released.
                    ctx.resources().release(taskId);
            }
        });
    }

    /**
     * Closes the task resource.
     */
    @Override public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                taskFut.cancel();
            }
            catch (IgniteCheckedException ignore) {
                // No-op.
            }
        }
    }
}
