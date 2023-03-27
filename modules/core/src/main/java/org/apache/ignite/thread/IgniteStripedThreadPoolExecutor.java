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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.pool.MetricsAwareExecutorService;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.ACTIVE_COUNT_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.COMPLETED_TASK_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.CORE_SIZE_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.IS_SHUTDOWN_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.IS_TERMINATED_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.IS_TERMINATING_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.KEEP_ALIVE_TIME_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.LARGEST_SIZE_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.MAX_SIZE_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.POOL_SIZE_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.QUEUE_SIZE_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.REJ_HND_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.TASK_COUNT_DESC;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.TASK_EXEC_TIME;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.TASK_EXEC_TIME_HISTOGRAM_BUCKETS;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.THRD_FACTORY_DESC;

/**
 * An {@link ExecutorService} that executes submitted tasks using pooled grid threads.
 */
public class IgniteStripedThreadPoolExecutor implements ExecutorService, MetricsAwareExecutorService {
    /** Stripe pools. */
    private final IgniteThreadPoolExecutor[] execs;

    /** Task execution time metric. */
    @GridToStringExclude
    private volatile HistogramMetricImpl execTime;

    /**
     * Create striped thread pool.
     *
     * @param concurrentLvl Concurrency level.
     * @param igniteInstanceName Node name.
     * @param threadNamePrefix Thread name prefix.
     * @param allowCoreThreadTimeOut Sets the policy governing whether core threads may time out and
     * terminate if no tasks arrive within the keep-alive time.
     * @param keepAliveTime When the number of threads is greater than the core, this is the maximum time
     * that excess idle threads will wait for new tasks before terminating.
     * @param eHnd Uncaught exception handler.
     */
    public IgniteStripedThreadPoolExecutor(
        int concurrentLvl,
        String igniteInstanceName,
        String threadNamePrefix,
        UncaughtExceptionHandler eHnd,
        boolean allowCoreThreadTimeOut,
        long keepAliveTime) {
        execs = new IgniteThreadPoolExecutor[concurrentLvl];
        execTime = new HistogramMetricImpl(TASK_EXEC_TIME, TASK_COUNT_DESC, TASK_EXEC_TIME_HISTOGRAM_BUCKETS);

        ThreadFactory factory = new IgniteThreadFactory(igniteInstanceName, threadNamePrefix, eHnd);

        for (int i = 0; i < concurrentLvl; i++) {
            IgniteThreadPoolExecutor executor = new IgniteThreadPoolExecutor(
                1,
                1,
                keepAliveTime,
                new LinkedBlockingQueue<>(),
                factory,
                execTime);

            executor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);

            execs[i] = executor;
        }
    }

    /**
     * Executes the given command at some time in the future. The command with the same {@code index}
     * will be executed in the same thread.
     *
     * @param task the runnable task
     * @param idx Striped index.
     * @throws RejectedExecutionException if this task cannot be
     * accepted for execution.
     * @throws NullPointerException If command is null
     */
    public void execute(Runnable task, int idx) {
        execs[threadId(idx)].execute(task);
    }

    /**
     * @param idx Index.
     * @return Stripped thread ID.
     */
    public int threadId(int idx) {
        return idx < execs.length ? idx : idx % execs.length;
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        for (ExecutorService exec : execs)
            exec.shutdown();
    }

    /** {@inheritDoc} */
    @Override public List<Runnable> shutdownNow() {
        if (execs.length == 0)
            return Collections.emptyList();

        List<Runnable> res = new ArrayList<>(execs.length);

        for (ExecutorService exec : execs) {
            for (Runnable r : exec.shutdownNow())
                res.add(r);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
        for (ExecutorService exec : execs) {
            if (!exec.isShutdown())
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
        for (ExecutorService exec : execs) {
            if (!exec.isTerminated())
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean res = true;

        for (ExecutorService exec : execs)
            res &= exec.awaitTermination(timeout, unit);

        return res;
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> Future<T> submit(Callable<T> task) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> Future<T> submit(Runnable task, T res) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @NotNull @Override public Future<?> submit(Runnable task) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
        long timeout,
        TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void execute(Runnable cmd) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void registerMetrics(MetricRegistry mreg) {
        mreg.register("ActiveCount", this::activeCount, ACTIVE_COUNT_DESC);
        mreg.register("CompletedTaskCount", this::completedTaskCount, COMPLETED_TASK_DESC);
        mreg.intMetric("CorePoolSize", CORE_SIZE_DESC).value(execs.length);
        mreg.register("LargestPoolSize", this::largestPoolSize, LARGEST_SIZE_DESC);
        mreg.intMetric("MaximumPoolSize", MAX_SIZE_DESC).value(execs.length);
        mreg.register("PoolSize", this::poolSize, POOL_SIZE_DESC);
        mreg.register("TaskCount", this::taskCount, TASK_COUNT_DESC);
        mreg.register("QueueSize", this::queueSize, QUEUE_SIZE_DESC);
        mreg.longMetric("KeepAliveTime", KEEP_ALIVE_TIME_DESC).value(execs[0].getKeepAliveTime(TimeUnit.MILLISECONDS));
        mreg.register("Shutdown", this::isShutdown, IS_SHUTDOWN_DESC);
        mreg.register("Terminated", this::isTerminated, IS_TERMINATED_DESC);
        mreg.register("Terminating", this::terminating, IS_TERMINATING_DESC);
        mreg.objectMetric("RejectedExecutionHandlerClass", String.class, REJ_HND_DESC)
            .value(execs[0].getRejectedExecutionHandler().getClass().getName());
        mreg.objectMetric("ThreadFactoryClass", String.class, THRD_FACTORY_DESC)
            .value(execs[0].getThreadFactory().getClass().getName());

        HistogramMetricImpl execTime0 = execTime;

        execTime = new HistogramMetricImpl(metricName(mreg.name(), TASK_EXEC_TIME), execTime0);

        mreg.register(execTime);

        for (IgniteThreadPoolExecutor exec : execs)
            exec.executionTimeMetric(execTime);
    }

    /**
     * @return Returns true if this executor is in the process of terminating after shutdown or shutdownNow but has not
     * completely terminated.
     */
    private boolean terminating() {
        for (IgniteThreadPoolExecutor exec : execs) {
            if (!exec.isTerminating())
                return false;
        }

        return true;
    }

    /**
     * @return Size of the task queue used by executor.
     */
    private int queueSize() {
        int queueSize = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            queueSize += exec.getQueue().size();

        return queueSize;
    }

    /**
     * @return Approximate total number of tasks that have ever been scheduled for execution. Because the states of
     * tasks and threads may change dynamically during computation, the returned value is only an approximation.
     */
    private long taskCount() {
        long taskCnt = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            taskCnt += exec.getTaskCount();

        return taskCnt;
    }

    /**
     * @return Approximate total number of tasks that have completed execution. Because the states of tasks and threads
     * may change dynamically during computation, the returned value is only an approximation, but one that does not
     * ever decrease across successive calls.
     */
    private long completedTaskCount() {
        long completedTaskCnt = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            completedTaskCnt += exec.getCompletedTaskCount();

        return completedTaskCnt;
    }

    /**
     * @return Approximate number of threads that are actively executing tasks.
     */
    private int activeCount() {
        int activeCnt = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            activeCnt += exec.getActiveCount();

        return activeCnt;
    }

    /**
     * @return current number of threads in the pool.
     */
    private int poolSize() {
        int poolSize = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            poolSize += exec.getPoolSize();

        return poolSize;
    }

    /**
     * @return Largest number of threads that have ever simultaneously been in the pool.
     */
    private int largestPoolSize() {
        int largestPoolSize = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            largestPoolSize += exec.getLargestPoolSize();

        return largestPoolSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteStripedThreadPoolExecutor.class, this);
    }
}
