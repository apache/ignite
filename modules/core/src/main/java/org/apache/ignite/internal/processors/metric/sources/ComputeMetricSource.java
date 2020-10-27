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

package org.apache.ignite.internal.processors.metric.sources;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;

/**
 * Compute grid metric source.
 */
public class ComputeMetricSource extends AbstractMetricSource<ComputeMetricSource.Holder> {
    /** Compute metrics. */
    public static final String COMPUTE_METRICS = "compute";

    /** Total executed tasks metric name. */
    public static final String TOTAL_EXEC_TASKS = "TotalExecutedTasks";

    /** Started jobs metric name. */
    public static final String STARTED = "Started";

    /** Active jobs metric name. */
    public static final String ACTIVE = "Active";

    /** Waiting jobs metric name. */
    public static final String WAITING = "Waiting";

    /** Canceled jobs metric name. */
    public static final String CANCELED = "Canceled";

    /** Rejected jobs metric name. */
    public static final String REJECTED = "Rejected";

    /** Finished jobs metric name. */
    public static final String FINISHED = "Finished";

    /** Total jobs execution time metric name. */
    public static final String EXECUTION_TIME = "ExecutionTime";

    /** Total jobs waiting time metric name. */
    public static final String WAITING_TIME = "WaitingTime";

    /**
     * Creates metric source.
     *
     * @param ctx Kernal context
     */
    public ComputeMetricSource(GridKernalContext ctx) {
        super(COMPUTE_METRICS, ctx);
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        hldr.execTasks = bldr.longAdderMetric(TOTAL_EXEC_TASKS, "Total executed tasks.");

        hldr.startedJobsMetric = bldr.longMetric(STARTED, "Number of started jobs.");

        hldr.activeJobsMetric = bldr.longMetric(ACTIVE, "Number of active jobs currently executing.");

        hldr.waitingJobsMetric = bldr.longMetric(WAITING, "Number of currently queued jobs waiting to be executed.");

        hldr.canceledJobsMetric = bldr.longMetric(CANCELED, "Number of cancelled jobs that are still running.");

        hldr.rejectedJobsMetric = bldr.longMetric(REJECTED,
                "Number of jobs rejected after more recent collision resolution operation.");

        hldr.finishedJobsMetric = bldr.longMetric(FINISHED, "Number of finished jobs.");

        hldr.totalExecutionTimeMetric = bldr.longMetric(EXECUTION_TIME, "Total execution time of jobs.");

        hldr.totalWaitTimeMetric = bldr.longMetric(WAITING_TIME, "Total time jobs spent on waiting queue.");
    }

    /** Increments started jobs counter. */
    public void incrementStartedJobs() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.startedJobsMetric.increment();
    }

    /** Increments canceled jobs counter. */
    public void incrementCanceledJobs() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.canceledJobsMetric.increment();
    }

    /** Increments rejected jobs counter. */
    public void incrementRejectedJobs() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.rejectedJobsMetric.increment();
    }

    /** Increments active jobs counter. */
    public void incrementActiveJobs() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.activeJobsMetric.increment();
    }

    /** Decrements active jobs counter. */
    public void decrementActiveJobs() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.activeJobsMetric.decrement();
    }

    /** Increments waiting jobs counter. */
    public void incrementWaitingJobs() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.waitingJobsMetric.increment();
    }

    /** Decrements waiting jobs counter. */
    public void decrementWaitingJobs() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.waitingJobsMetric.decrement();
    }

    /** Increments finished jobs counter. */
    public void incrementFinishedJobs() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.finishedJobsMetric.increment();
    }

    /**
     * Increases total jobs waiting time.
     *
     * @param val Value which will be added.
     */
    public void increaseTotalWaitTime(long val) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.totalWaitTimeMetric.add(val);
    }

    /**
     * Increases total jobs execution time.
     *
     * @param val Value which will be added.
     */
    public void increaseTotalExecutionTime(long val) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.totalExecutionTimeMetric.add(val);
    }

    /** Increments executed tasks counter. */
    public void incrementExecutedTasks() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.execTasks.increment();
    }

    /** Resets executed tasks counter to zero value. */
    public void resetExecutedTasks() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.execTasks.reset();
    }

    /**
     * Returns value of executed tasks counter.
     *
     * @return Number of executed tasks.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long executedTasks() {
        Holder hldr = holder();

        return hldr != null ? hldr.execTasks.value() : -1;
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** Total executed tasks metric. */
        private LongAdderMetric execTasks;

        /** Number of started jobs. */
        private AtomicLongMetric startedJobsMetric;

        /** Number of active jobs currently executing. */
        private AtomicLongMetric activeJobsMetric;

        /** Number of currently queued jobs waiting to be executed. */
        private AtomicLongMetric waitingJobsMetric;

        /** Number of cancelled jobs that are still running. */
        private AtomicLongMetric canceledJobsMetric;

        /** Number of jobs rejected after more recent collision resolution operation. */
        private AtomicLongMetric rejectedJobsMetric;

        /** Number of finished jobs. */
        private AtomicLongMetric finishedJobsMetric;

        /** Total job execution time. */
        private AtomicLongMetric totalExecutionTimeMetric;

        /** Total time jobs spent on waiting queue. */
        private AtomicLongMetric totalWaitTimeMetric;
    }
}
