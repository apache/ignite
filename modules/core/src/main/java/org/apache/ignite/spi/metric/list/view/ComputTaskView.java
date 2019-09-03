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

package org.apache.ignite.spi.metric.list.view;

import java.util.UUID;
import org.apache.ignite.internal.processors.task.GridTaskWorker;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.metric.list.MonitoringRow;

/**
 *
 */
public class ComputTaskView implements MonitoringRow<IgniteUuid> {
    /** */
    private final GridTaskWorker worker;

    /** */
    public ComputTaskView(GridTaskWorker worker) {
        this.worker = worker;
    }

    /** */
    @Override public IgniteUuid monitoringRowId() {
        return worker.getSession().getId();
    }

    /** */
    public boolean internal() {
        return worker.isInternal();
    }

    /** */
    public String taskName() {
        return worker.getSession().getTaskName();
    }

    /** */
    public String taskClassName() {
        return worker.getSession().getTaskClassName();
    }

    /** */
    public long startTime() {
        return worker.getSession().getStartTime();
    }

    /** */
    public long endTime() {
        return worker.getSession().getEndTime();
    }

    /** */
    public UUID taskNodeId() {
        return worker.getSession().getTaskNodeId();
    }

    /** */
    public String execName() {
        return worker.getSession().executorName();
    }

    /** */
    public String affinityCacheName() {
        return worker.affCacheName();
    }

    /** */
    public int affinityPartitionId() {
        return worker.affPartId();
    }

    /** */
    public IgniteUuid jobId() {
        return worker.getSession().getJobId();
    }

    /** */
    public String userVersion() {
        return worker.getSession().getUserVersion();
    }
}
