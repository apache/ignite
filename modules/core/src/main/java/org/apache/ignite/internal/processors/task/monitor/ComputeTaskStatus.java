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

package org.apache.ignite.internal.processors.task.monitor;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.GridTaskSessionImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.FAILED;
import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.FINISHED;
import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.RUNNING;

/**
 * Task status container.
 *
 * @see ComputeTaskStatusSnapshot
 */
public class ComputeTaskStatus implements ComputeTaskStatusSnapshot {
    /** Session ID of the task being executed. */
    private final IgniteUuid sessionId;

    /** Status of the task. */
    private final ComputeTaskStatusEnum status;

    /** Task name of the task this session belongs to. */
    private final String taskName;

    /** ID of the node on which task execution originated. */
    private final UUID originatingNodeId;

    /** Start of computation time for the task. */
    private final long startTime;

    /** End of computation time for the task. */
    private final long endTime;

    /** Nodes IDs on which the task jobs will execute. */
    private final List<UUID> jobNodes;

    /** All session attributes. */
    private final Map<?, ?> attributes;

    /** Reason for the failure of the task. */
    @Nullable private final Throwable failReason;

    /** Availability of changing task attributes. */
    private final boolean fullSupport;

    /** User who created the task, {@code null} if security is not available. */
    @Nullable private Object createdBy;

    /** Internal task flag. */
    private final boolean internal;

    /**
     * Constructor for a new task.
     *
     * @param sessionId Session ID of the task being executed.
     * @param status Status of the task.
     * @param taskName Task name of the task this session belongs to.
     * @param originatingNodeId ID of the node on which task execution originated.
     * @param startTime Start of computation time for the task.
     * @param endTime End of computation time for the task.
     * @param jobNodes Nodes IDs on which the task jobs will execute.
     * @param attributes All session attributes.
     * @param failReason Reason for the failure of the task.
     * @param fullSupport Availability of changing task attributes.
     * @param createdBy User who created the task, {@code null} if security is not available.
     * @param internal Internal task flag.
     */
    private ComputeTaskStatus(
        IgniteUuid sessionId,
        ComputeTaskStatusEnum status,
        String taskName,
        UUID originatingNodeId,
        long startTime,
        long endTime,
        List<UUID> jobNodes,
        Map<?, ?> attributes,
        @Nullable Throwable failReason,
        boolean fullSupport,
        @Nullable Object createdBy,
        boolean internal
    ) {
        this.sessionId = sessionId;
        this.status = status;
        this.taskName = taskName;
        this.originatingNodeId = originatingNodeId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.jobNodes = F.isEmpty(jobNodes) ? emptyList() : jobNodes;
        this.attributes = F.isEmpty(attributes) ? emptyMap() : attributes;
        this.failReason = failReason;
        this.fullSupport = fullSupport;
        this.createdBy = createdBy;
        this.internal = internal;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid sessionId() {
        return sessionId;
    }

    /** {@inheritDoc} */
    @Override public String taskName() {
        return taskName;
    }

    /** {@inheritDoc} */
    @Override public UUID originatingNodeId() {
        return originatingNodeId;
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return endTime;
    }

    /** {@inheritDoc} */
    @Override public List<UUID> jobNodes() {
        return jobNodes;
    }

    /** {@inheritDoc} */
    @Override public Map<?, ?> attributes() {
        return attributes;
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskStatusEnum status() {
        return status;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Throwable failReason() {
        return failReason;
    }

    /** {@inheritDoc} */
    @Override public boolean fullSupport() {
        return fullSupport;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Object createBy() {
        return createdBy;
    }

    /** {@inheritDoc} */
    @Override public boolean internal() {
        return internal;
    }

    /**
     * Creates the status of a task that is in progress.
     *
     * @param sessionImp Task session.
     * @return New instance.
     */
    public static ComputeTaskStatus snapshot(GridTaskSessionImpl sessionImp) {
        return new ComputeTaskStatus(
            sessionImp.getId(),
            RUNNING,
            sessionImp.getTaskName(),
            sessionImp.getTaskNodeId(),
            sessionImp.getStartTime(),
            0L,
            sessionImp.jobNodesSafeCopy(),
            sessionImp.attributesSafeCopy(),
            null,
            sessionImp.isFullSupport(),
            sessionImp.login(),
            sessionImp.isInternal()
        );
    }

    /**
     * Creates a task status on finishing task.
     *
     * @param sessionImp Task session.
     * @param err – Reason for the failure of the task, null if the task completed successfully.
     * @return New instance.
     */
    public static ComputeTaskStatus onFinishTask(GridTaskSessionImpl sessionImp, @Nullable Throwable err) {
        return new ComputeTaskStatus(
            sessionImp.getId(),
            err == null ? FINISHED : FAILED,
            sessionImp.getTaskName(),
            sessionImp.getTaskNodeId(),
            sessionImp.getStartTime(),
            U.currentTimeMillis(),
            sessionImp.jobNodesSafeCopy(),
            sessionImp.attributesSafeCopy(),
            err,
            sessionImp.isFullSupport(),
            sessionImp.login(),
            sessionImp.isInternal()
        );
    }
}
