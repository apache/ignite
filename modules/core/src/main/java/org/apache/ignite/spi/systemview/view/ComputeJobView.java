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

import java.util.StringJoiner;
import java.util.UUID;
import org.apache.ignite.internal.managers.collision.GridCollisionManager;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.job.GridJobProcessor;
import org.apache.ignite.internal.processors.job.GridJobWorker;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Compute job representation for a {@link SystemView}.
 */
public class ComputeJobView {
    /** Compute job state. */
    public enum ComputeJobState {
        /**
         * Job scheduled for the execution.
         * If collision not configured all jobs in this state by default.
         *
         * @see GridCollisionManager
         * @see CollisionSpi
         */
        ACTIVE,

        /**
         * If collision configured jobs may be passivated before execution.
         *
         * @see GridCollisionManager
         * @see CollisionSpi
         */
        PASSIVE,

        /**
         * Job execution canceled.
         *
         * @see GridJobProcessor#cancelJob(IgniteUuid, IgniteUuid, boolean)
         */
        CANCELED
    }

    /** Job. */
    private final GridJobWorker job;

    /** Job id. */
    private final IgniteUuid id;

    /** Job state. */
    private final ComputeJobState state;

    /**
     * @param id Job id.
     * @param job Job.
     * @param state Job state.
     */
    public ComputeJobView(IgniteUuid id, GridJobWorker job, ComputeJobState state) {
        this.id = id;
        this.job = job;
        this.state = state;
    }

    /** @return Job id. */
    @Order
    public IgniteUuid id() {
        return id;
    }

    /**
     * {@link ComputeJobView#sessionId()} value equal to the value of {@link ComputeTaskView#sessionId()}
     * if both records represents parts of the same computation.
     *
     * @return Session id.
     * @see ComputeTaskView#sessionId()
     */
    @Order(1)
    public IgniteUuid sessionId() {
        return job.getSession().getId();
    }

    /** @return Origin node id. */
    @Order(2)
    public UUID originNodeId() {
        return job.getTaskNode().id();
    }

    /** @return Task name. */
    @Order(3)
    public String taskName() {
        return job.getSession().getTaskName();
    }

    /** @return Task class name. */
    @Order(4)
    public String taskClassName() {
        return job.getSession().getTaskClassName();
    }

    /** @return Comma separated list of cache identifiers or {@code null} for non affinity call. */
    @Order(5)
    public String affinityCacheIds() {
        GridReservable res = job.getPartsReservation();

        if (!(res instanceof GridJobProcessor.PartitionsReservation))
            return null;

        int[] ids = ((GridJobProcessor.PartitionsReservation)res).getCacheIds();

        if (ids == null || ids.length == 0)
            return null;

        StringJoiner joiner = new StringJoiner(",");

        for (int id : ids)
            joiner.add(Integer.toString(id));

        return joiner.toString();
    }

    /** @return Affinity partition id or {@code -1} for non affinity call. */
    @Order(6)
    public int affinityPartitionId() {
        GridReservable res = job.getPartsReservation();

        if (!(res instanceof GridJobProcessor.PartitionsReservation))
            return -1;

        return ((GridJobProcessor.PartitionsReservation)res).getPartId();
    }

    /** @return Create time in milliseconds. */
    @Order(7)
    public long createTime() {
        return job.getCreateTime();
    }

    /** @return Start time in milliseconds. */
    @Order(8)
    public long startTime() {
        return job.getStartTime();
    }

    /** @return Finish time in milliseconds. */
    @Order(9)
    public long finishTime() {
        return job.getFinishTime();
    }

    /** @return {@code True} if job is internal. */
    public boolean isInternal() {
        return job.isInternal();
    }

    /** @return {@code True} if job is finishing. */
    public boolean isFinishing() {
        return job.isFinishing();
    }

    /** @return {@code True} if job is timed out. */
    public boolean isTimedOut() {
        return job.isTimedOut();
    }

    /** @return {@code True} if job started. */
    public boolean isStarted() {
        return job.isStarted();
    }

    /** @return Executor name or {@code null} if not specified. */
    @Nullable public String executorName() {
        return job.executorName();
    }

    /** @return Job state. */
    public ComputeJobState state() {
        return state;
    }
}
