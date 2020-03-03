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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.job.GridJobProcessor;
import org.apache.ignite.internal.processors.job.GridJobWorker;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Compute job representation for a {@link SystemView}.
 */
public class ComputeJobView {
    /** Job. */
    private final GridJobWorker job;

    /**
     * @param job Job.
     */
    public ComputeJobView(GridJobWorker job) {
        this.job = job;
    }

    /** @return Task id. */
    public IgniteUuid id() {
        return job.getJobId();
    }

    /** @return Create time. */
    public long createTime() {
        return job.getCreateTime();
    }

    /** @return Start time. */
    public long startTime() {
        return job.getStartTime();
    }

    /** @return Finish time. */
    public long fininshTime() {
        return job.getFinishTime();
    }

    /** @return Origin node id. */
    public UUID originNodeId() {
        return job.getTaskNode().id();
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

    /** @return {@code True} if ???. */
    public boolean isSysCancelled() {
        return job.isSystemCanceled();
    }

    /** @return {@code True} if ???. */
    public boolean isSysStopping() {
        return job.isSysStopping();
    }

    /** @return {@code True} if job started. */
    public boolean isStarted() {
        return job.isStarted();
    }

    /** @return Executor name. */
    public String executorName() {
        return job.executorName();
    }

    /** @return Job class name. */
    public String taskClassName() {
        return job.getSession().getTaskClassName();
    }

    /** @return Task name. */
    public String taskName() {
        return job.getSession().getTaskName();
    }

    /** @return Affinity cache ids. */
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

    /** @return Affinity partition id. */
    public int affinityPartitionId() {
        GridReservable res = job.getPartsReservation();

        if (!(res instanceof GridJobProcessor.PartitionsReservation))
            return -1;

        return ((GridJobProcessor.PartitionsReservation)res).getPartId();
    }
}
