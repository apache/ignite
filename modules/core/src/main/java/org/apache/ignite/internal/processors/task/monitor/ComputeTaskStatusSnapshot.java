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
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot of the task status.
 */
public interface ComputeTaskStatusSnapshot {
    /**
     * @return Session ID of the task being executed.
     */
    IgniteUuid sessionId();

    /**
     * @return Task name of the task this session belongs to.
     */
    String taskName();

    /**
     * @return ID of the node on which task execution originated.
     */
    UUID originatingNodeId();

    /**
     * @return Start of computation time for the task.
     */
    long startTime();

    /**
     * @return End of computation time for the task.
     */
    long endTime();

    /**
     * @return Nodes IDs on which the task jobs will execute.
     */
    List<UUID> jobNodes();

    /**
     * @return All session attributes.
     */
    Map<?, ?> attributes();

    /**
     * @return Status of the task.
     */
    ComputeTaskStatusEnum status();

    /**
     * @return Reason for the failure of the task.
     */
    @Nullable Throwable failReason();

    /**
     * @return {@code true} if change of task attributes is available.
     */
    boolean fullSupport();

    /**
     * @return User who created the task, {@code null} if security is not available.
     */
    @Nullable Object createBy();

    /**
     * @return {@code True} if task is internal.
     */
    boolean internal();
}
