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

package org.apache.ignite.spi.systemview.view;

import java.util.UUID;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.task.GridTaskWorker;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Compute task representation for a {@link SystemView}.
 */
public class ComputeTaskView {
    /** Worker for task. */
    private final GridTaskWorker worker;

    /** Task id. */
    private final IgniteUuid id;

    /**
     * @param id Task id.
     * @param worker Worker for task.
     */
    public ComputeTaskView(IgniteUuid id, GridTaskWorker worker) {
        this.id = id;
        this.worker = worker;
    }

    /** @return Task id. */
    @Order
    public IgniteUuid id() {
        return id;
    }

    /**
     * {@link ComputeTaskView#sessionId()} value equal to the value of {@link ComputeJobView#sessionId()}
     * if both records represents parts of the same computation.
     *
     * @return Session id.
     * @see ComputeJobView#sessionId()
     */
    @Order(1)
    public IgniteUuid sessionId() {
        return worker.getSession().getId();
    }

    /** @return Task node id. */
    @Order(2)
    public UUID taskNodeId() {
        return worker.getSession().getTaskNodeId();
    }

    /** @return {@code True} if task is internal. */
    public boolean internal() {
        return worker.isInternal();
    }

    /** @return Task name. */
    @Order(3)
    public String taskName() {
        return worker.getSession().getTaskName();
    }

    /** @return Task class name. */
    @Order(4)
    public String taskClassName() {
        return worker.getSession().getTaskClassName();
    }

    /** @return Start time in milliseconds. */
    @Order(7)
    public long startTime() {
        return worker.getSession().getStartTime();
    }

    /** @return End time in milliseconds. */
    @Order(8)
    public long endTime() {
        return worker.getSession().getEndTime();
    }

    /** @return Executor name. */
    public String execName() {
        return worker.getSession().executorName();
    }

    /** @return Affinity cache name or {@code null} for non affinity call. */
    @Order(6)
    @Nullable public String affinityCacheName() {
        return worker.affCacheName();
    }

    /** @return Affinity partition id or {@code -1} for non affinity call. */
    @Order(5)
    public int affinityPartitionId() {
        return worker.affPartId();
    }

    /**
     * @return Job id.
     * @deprecated Use {@link #id()} or {@link #sessionId()} instead.
     */
    @Deprecated
    public IgniteUuid jobId() {
        return worker.getSession().getJobId();
    }

    /** @return User provided version. */
    public String userVersion() {
        return worker.getSession().getUserVersion();
    }
}
