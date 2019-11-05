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
import org.apache.ignite.internal.processors.task.GridTaskWorker;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Compute task representation for a {@link SystemView}.
 */
public class ComputeTaskView {
    /** Worker for task. */
    private final GridTaskWorker worker;

    /**
     * @param worker Worker for task.
     */
    public ComputeTaskView(GridTaskWorker worker) {
        this.worker = worker;
    }

    /** @return {@code True} if task is internal. */
    public boolean internal() {
        return worker.isInternal();
    }

    /** @return Task name. */
    public String taskName() {
        return worker.getSession().getTaskName();
    }

    /** @return Task class name. */
    public String taskClassName() {
        return worker.getSession().getTaskClassName();
    }

    /** @return Start time. */
    public long startTime() {
        return worker.getSession().getStartTime();
    }

    /** @return End time. */
    public long endTime() {
        return worker.getSession().getEndTime();
    }

    /** @return Task node id. */
    public UUID taskNodeId() {
        return worker.getSession().getTaskNodeId();
    }

    /** @return Executor name. */
    public String execName() {
        return worker.getSession().executorName();
    }

    /** @return Affinity cache name. */
    public String affinityCacheName() {
        return worker.affCacheName();
    }

    /** @return Affinity partition id. */
    public int affinityPartitionId() {
        return worker.affPartId();
    }

    /** @return Job id. */
    public IgniteUuid jobId() {
        return worker.getSession().getJobId();
    }

    /** @return User provided version. */
    public String userVersion() {
        return worker.getSession().getUserVersion();
    }
}
