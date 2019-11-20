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
import org.apache.ignite.internal.util.StripedExecutor;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.metric.sources.ThreadPoolExecutorMetricSource.THREAD_POOLS;

/**
 * Metric source for striped pool metrics.
 */
public class StripedPoolMetricSource extends AbstractMetricSource<StripedPoolMetricSource.Holder> {
    /** Wrapped striped pool. */
    private final StripedExecutor exec;

    /**
     * Creates metric source for striped pool.
     *
     * @param ctx Kernal context.
     * @param exec Wrapped striped pool.
     */
    public StripedPoolMetricSource(GridKernalContext ctx, StripedExecutor exec) {
        super(metricName(THREAD_POOLS, "StripedExecutor"), ctx);

        this.exec = exec;
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        bldr.register("DetectStarvation",
                exec::detectStarvation,
                "True if possible starvation in striped pool is detected.");

        bldr.register("StripesCount",
                exec::stripesCount,
                "Stripes count.");

        bldr.register("Shutdown",
                exec::isShutdown,
                "True if this executor has been shut down.");

        bldr.register("Terminated",
                exec::isTerminated,
                "True if all tasks have completed following shut down.");

        bldr.register("TotalQueueSize",
                exec::queueSize,
                "Total queue size of all stripes.");

        bldr.register("TotalCompletedTasksCount",
                exec::completedTasks,
                "Completed tasks count of all stripes.");

        bldr.register("StripesCompletedTasksCounts",
                exec::stripesCompletedTasks,
                long[].class,
                "Number of completed tasks per stripe.");

        bldr.register("ActiveCount",
                exec::activeStripesCount,
                "Number of active tasks of all stripes.");

        bldr.register("StripesActiveStatuses",
                exec::stripesActiveStatuses,
                boolean[].class,
                "Number of active tasks per stripe.");

        bldr.register("StripesQueueSizes",
                exec::stripesQueueSizes,
                int[].class,
                "Size of queue per stripe.");
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
    }
}
