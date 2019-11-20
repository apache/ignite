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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Metric source for thread pool executor metrics.
 */
public class ThreadPoolExecutorMetricSource extends AbstractMetricSource<ThreadPoolExecutorMetricSource.Holder> {
    /** Group for a thread pools. */
    public static final String THREAD_POOLS = "threadPools";

    /** */
    public static final String ACTIVE_COUNT_DESC = "Approximate number of threads that are actively executing tasks.";

    /** */
    public static final String COMPLETED_TASK_DESC = "Approximate total number of tasks that have completed execution.";

    /** */
    public static final String CORE_SIZE_DESC = "The core number of threads.";

    /** */
    public static final String LARGEST_SIZE_DESC = "Largest number of threads that have ever simultaneously been in the pool.";

    /** */
    public static final String MAX_SIZE_DESC = "The maximum allowed number of threads.";

    /** */
    public static final String POOL_SIZE_DESC = "Current number of threads in the pool.";

    /** */
    public static final String TASK_COUNT_DESC = "Approximate total number of tasks that have been scheduled for execution.";

    /** */
    public static final String QUEUE_SIZE_DESC = "Current size of the execution queue.";

    /** */
    public static final String KEEP_ALIVE_TIME_DESC = "Thread keep-alive time, which is the amount of time which threads in excess of " +
            "the core pool size may remain idle before being terminated.";

    /** */
    public static final String IS_SHUTDOWN_DESC = "True if this executor has been shut down.";

    /** */
    public static final String IS_TERMINATED_DESC = "True if all tasks have completed following shut down.";

    /** */
    public static final String IS_TERMINATING_DESC = "True if terminating but not yet terminated.";

    /** */
    public static final String REJ_HND_DESC = "Class name of current rejection handler.";

    /** */
    public static final String THRD_FACTORY_DESC = "Class name of thread factory used to create new threads.";

    /** Wrapped executor service. */
    private final ExecutorService execSrvc;

    /**
     * Creates metric source for thread pool.
     *
     * @param name Thread pool name.
     * @param ctx Kernal context.
     * @param execSrvc Wrapped executor service.
     */
    public ThreadPoolExecutorMetricSource(String name, GridKernalContext ctx, ExecutorService execSrvc) {
        super(metricName(THREAD_POOLS, name), ctx);

        this.execSrvc = execSrvc;
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        if (execSrvc instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor exec = (ThreadPoolExecutor)execSrvc;

            bldr.register("ActiveCount", exec::getActiveCount, ACTIVE_COUNT_DESC);
            bldr.register("CompletedTaskCount", exec::getCompletedTaskCount, COMPLETED_TASK_DESC);
            bldr.register("CorePoolSize", exec::getCorePoolSize, CORE_SIZE_DESC);
            bldr.register("LargestPoolSize", exec::getLargestPoolSize, LARGEST_SIZE_DESC);
            bldr.register("MaximumPoolSize", exec::getMaximumPoolSize, MAX_SIZE_DESC);
            bldr.register("PoolSize", exec::getPoolSize, POOL_SIZE_DESC);
            bldr.register("TaskCount", exec::getTaskCount, TASK_COUNT_DESC);
            bldr.register("QueueSize", () -> exec.getQueue().size(), QUEUE_SIZE_DESC);
            bldr.register("KeepAliveTime", () -> exec.getKeepAliveTime(MILLISECONDS), KEEP_ALIVE_TIME_DESC);
            bldr.register("Shutdown", exec::isShutdown, IS_SHUTDOWN_DESC);
            bldr.register("Terminated", exec::isTerminated, IS_TERMINATED_DESC);
            bldr.register("Terminating", exec::isTerminating, IS_TERMINATING_DESC);
            bldr.register("RejectedExecutionHandlerClass", () -> {
                RejectedExecutionHandler hnd = exec.getRejectedExecutionHandler();

                return hnd == null ? "" : hnd.getClass().getName();
            }, String.class, REJ_HND_DESC);
            bldr.register("ThreadFactoryClass", () -> {
                ThreadFactory factory = exec.getThreadFactory();

                return factory == null ? "" : factory.getClass().getName();
            }, String.class, THRD_FACTORY_DESC);
        }
        else {
            bldr.longMetric("ActiveCount", ACTIVE_COUNT_DESC).value(0);
            bldr.longMetric("CompletedTaskCount", COMPLETED_TASK_DESC).value(0);
            bldr.longMetric("CorePoolSize", CORE_SIZE_DESC).value(0);
            bldr.longMetric("LargestPoolSize", LARGEST_SIZE_DESC).value(0);
            bldr.longMetric("MaximumPoolSize", MAX_SIZE_DESC).value(0);
            bldr.longMetric("PoolSize", POOL_SIZE_DESC).value(0);
            bldr.longMetric("TaskCount", TASK_COUNT_DESC);
            bldr.longMetric("QueueSize", QUEUE_SIZE_DESC).value(0);
            bldr.longMetric("KeepAliveTime", KEEP_ALIVE_TIME_DESC).value(0);
            bldr.register("Shutdown", execSrvc::isShutdown, IS_SHUTDOWN_DESC);
            bldr.register("Terminated", execSrvc::isTerminated, IS_TERMINATED_DESC);
            bldr.longMetric("Terminating", IS_TERMINATING_DESC);
            bldr.objectMetric("RejectedExecutionHandlerClass", String.class, REJ_HND_DESC).value("");
            bldr.objectMetric("ThreadFactoryClass", String.class, THRD_FACTORY_DESC).value("");
        }
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
    }
}
