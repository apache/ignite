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

package org.apache.ignite.thread;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.pool.MetricsAwareExecutorService;
import org.apache.ignite.internal.util.GridMutableLong;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * An {@link ExecutorService} that executes submitted tasks using pooled grid threads.
 */
public class IgniteThreadPoolExecutor extends ThreadPoolExecutor implements MetricsAwareExecutorService {
    /** Thread local task start time. */
    private final ThreadLocal<GridMutableLong> taskStartTime = new ThreadLocal<>();

    /** Task execution time metric. */
    private LongConsumer execTimeMetric;

    /**
     * Creates a new service with the given initial parameters.
     *
     * NOTE: There is a known bug. If 'corePoolSize' equals {@code 0},
     * then the pool will degrade to a single-threaded pool.
     *
     * @param threadNamePrefix Will be added at the beginning of all created threads.
     * @param igniteInstanceName Must be the name of the grid.
     * @param corePoolSize The number of threads to keep in the pool, even if they are idle.
     * @param maxPoolSize The maximum number of threads to allow in the pool.
     * @param keepAliveTime When the number of threads is greater than the core, this is the maximum time
     *      that excess idle threads will wait for new tasks before terminating.
     * @param workQ The queue to use for holding tasks before they are executed. This queue will hold only
     *      runnable tasks submitted by the {@link #execute(Runnable)} method.
     */
    public IgniteThreadPoolExecutor(
        String threadNamePrefix,
        String igniteInstanceName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ) {
        this(threadNamePrefix,
            igniteInstanceName,
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            workQ,
            GridIoPolicy.UNDEFINED,
            null);
    }

    /**
     * Creates a new service with the given initial parameters.
     *
     * NOTE: There is a known bug. If 'corePoolSize' equals {@code 0},
     * then the pool will degrade to a single-threaded pool.
     *
     * @param threadNamePrefix Will be added at the beginning of all created threads.
     * @param igniteInstanceName Must be the name of the grid.
     * @param corePoolSize The number of threads to keep in the pool, even if they are idle.
     * @param maxPoolSize The maximum number of threads to allow in the pool.
     * @param keepAliveTime When the number of threads is greater than the core, this is the maximum time
     *      that excess idle threads will wait for new tasks before terminating.
     * @param workQ The queue to use for holding tasks before they are executed. This queue will hold only
     *      runnable tasks submitted by the {@link #execute(Runnable)} method.
     * @param plc {@link GridIoPolicy} for thread pool.
     * @param eHnd Uncaught exception handler for thread pool.
     */
    public IgniteThreadPoolExecutor(
        String threadNamePrefix,
        String igniteInstanceName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        byte plc,
        UncaughtExceptionHandler eHnd) {
        super(
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            TimeUnit.MILLISECONDS,
            workQ,
            new IgniteThreadFactory(igniteInstanceName, threadNamePrefix, plc, eHnd)
        );
    }

    /**
     * Creates a new service with the given initial parameters.
     *
     * NOTE: There is a known bug. If 'corePoolSize' equals {@code 0},
     * then the pool will degrade to a single-threaded pool.
     * *
     * @param corePoolSize The number of threads to keep in the pool, even if they are idle.
     * @param maxPoolSize The maximum number of threads to allow in the pool.
     * @param keepAliveTime When the number of threads is greater than the core, this is the maximum time
     *      that excess idle threads will wait for new tasks before terminating.
     * @param workQ The queue to use for holding tasks before they are executed. This queue will hold only the
     *      runnable tasks submitted by the {@link #execute(Runnable)} method.
     * @param threadFactory Thread factory.
     */
    public IgniteThreadPoolExecutor(
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        ThreadFactory threadFactory) {
        super(
            corePoolSize,
            maxPoolSize,
            keepAliveTime,
            TimeUnit.MILLISECONDS,
            workQ,
            threadFactory,
            new AbortPolicy()
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);

        if (execTimeMetric != null) {
            // Prevent autoboxing if possible.
            GridMutableLong val = taskStartTime.get();

            if (val == null) {
                val = new GridMutableLong();

                taskStartTime.set(val);
            }

            val.set(U.currentTimeMillis());
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterExecute(Runnable r, Throwable t) {
        GridMutableLong val = taskStartTime.get();

        if (val != null)
            execTimeMetric.accept(U.currentTimeMillis() - val.get());

        super.afterExecute(r, t);
    }

    /** {@inheritDoc} */
    @Override public void registerMetrics(MetricRegistry mreg) {
        mreg.register("ActiveCount", this::getActiveCount, ACTIVE_COUNT_DESC);
        mreg.register("CompletedTaskCount", this::getCompletedTaskCount, COMPLETED_TASK_DESC);
        mreg.register("CorePoolSize", this::getCorePoolSize, CORE_SIZE_DESC);
        mreg.register("LargestPoolSize", this::getLargestPoolSize, LARGEST_SIZE_DESC);
        mreg.register("MaximumPoolSize", this::getMaximumPoolSize, MAX_SIZE_DESC);
        mreg.register("PoolSize", this::getPoolSize, POOL_SIZE_DESC);
        mreg.register("TaskCount", this::getTaskCount, TASK_COUNT_DESC);
        mreg.register("QueueSize", () -> getQueue().size(), QUEUE_SIZE_DESC);
        mreg.register("KeepAliveTime", () -> getKeepAliveTime(TimeUnit.MILLISECONDS), KEEP_ALIVE_TIME_DESC);
        mreg.register("Shutdown", this::isShutdown, IS_SHUTDOWN_DESC);
        mreg.register("Terminated", this::isTerminated, IS_TERMINATED_DESC);
        mreg.register("Terminating", this::isTerminating, IS_TERMINATING_DESC);

        mreg.register("RejectedExecutionHandlerClass", () -> {
            RejectedExecutionHandler hnd = getRejectedExecutionHandler();

            return hnd == null ? "" : hnd.getClass().getName();
        }, String.class, REJ_HND_DESC);

        mreg.register("ThreadFactoryClass", () -> {
            ThreadFactory factory = getThreadFactory();

            return factory == null ? "" : factory.getClass().getName();
        }, String.class, THRD_FACTORY_DESC);

        execTimeMetric = mreg.histogram(TASK_EXEC_TIME_NAME, TASK_EXEC_TIME_HISTOGRAM_BUCKETS, TASK_EXEC_TIME_DESC);
    }
}
