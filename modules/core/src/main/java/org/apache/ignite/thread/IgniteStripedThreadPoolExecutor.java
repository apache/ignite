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
import org.apache.ignite.internal.processors.pool.MetricsAwareExecutorService;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * An {@link ExecutorService} that executes submitted tasks using pooled grid threads.
 */
public class IgniteStripedThreadPoolExecutor implements ExecutorService, MetricsAwareExecutorService {
    /** Number of threads to keep in each stripe-pool. */
    private static final int CORE_THREADS_PER_POOL = 1;

    /** Maximum number of threads to allow in each stripe-pool. */
    private static final int MAX_THREADS_PER_POOL = 1;

    /** Thread keep-alive time. */
    private final long keepAliveTime;

    /** Number of threads to keep in the pool. */
    private final int corePoolSize;

    /** Maximum number of threads to allow in the pool. */
    private final int maxPoolSize;

    /** Stripe pools. */
    private final IgniteThreadPoolExecutor[] execs;

    /** Handler name that is used when execution is blocked because the thread bounds and queue capacities are reached. */
    private final String rejectedExecHndClsName;

    /** Class name of the thread factory. */
    private final String threadFactoryClsName;

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

        ThreadFactory factory = new IgniteThreadFactory(igniteInstanceName, threadNamePrefix, eHnd);

        for (int i = 0; i < concurrentLvl; i++) {
            IgniteThreadPoolExecutor executor = new IgniteThreadPoolExecutor(
                CORE_THREADS_PER_POOL,
                MAX_THREADS_PER_POOL,
                keepAliveTime,
                new LinkedBlockingQueue<>(),
                factory);

            executor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);

            execs[i] = executor;
        }

        this.keepAliveTime = keepAliveTime;

        corePoolSize = concurrentLvl * CORE_THREADS_PER_POOL;
        maxPoolSize = concurrentLvl * MAX_THREADS_PER_POOL;
        rejectedExecHndClsName = execs[0].getRejectedExecutionHandler().getClass().getName();
        threadFactoryClsName = factory.getClass().getName();
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

    /**
     * @return Returns true if this executor is in the process of terminating after shutdown or shutdownNow but has not
     * completely terminated.
     */
    private boolean isTerminating() {
        for (IgniteThreadPoolExecutor exec : execs) {
            if (!exec.isTerminating())
                return false;
        }

        return true;
    }

    /**
     * @return Size of the task queue used by executor.
     */
    private int getQueueSize() {
        int queueSize = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            queueSize += exec.getQueue().size();

        return queueSize;
    }

    /**
     * @return Approximate total number of tasks that have ever been scheduled for execution. Because the states of
     * tasks and threads may change dynamically during computation, the returned value is only an approximation.
     */
    private long getTaskCount() {
        long taskCount = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            taskCount += exec.getTaskCount();

        return taskCount;
    }

    /**
     * @return Approximate total number of tasks that have completed execution. Because the states of tasks and threads
     * may change dynamically during computation, the returned value is only an approximation, but one that does not
     * ever decrease across successive calls.
     */
    private long getCompletedTaskCount() {
        long completedTaskCnt = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            completedTaskCnt += exec.getCompletedTaskCount();

        return completedTaskCnt;
    }

    /**
     * @return Approximate number of threads that are actively executing tasks.
     */
    private int getActiveCount() {
        int activeCnt = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            activeCnt += exec.getActiveCount();

        return activeCnt;
    }

    /**
     * @return current number of threads in the pool.
     */
    private int getPoolSize() {
        int poolSize = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            poolSize += exec.getPoolSize();

        return poolSize;
    }

    /**
     * @return Largest number of threads that have ever simultaneously been in the pool.
     */
    private int getLargestPoolSize() {
        int largestPoolSize = 0;

        for (IgniteThreadPoolExecutor exec : execs)
            largestPoolSize += exec.getLargestPoolSize();

        return largestPoolSize;
    }

    /** {@inheritDoc} */
    @Override public void registerMetrics(MetricRegistry mreg) {
        mreg.register("ActiveCount", this::getActiveCount, ACTIVE_COUNT_DESC);
        mreg.register("CompletedTaskCount", this::getCompletedTaskCount, COMPLETED_TASK_DESC);
        mreg.intMetric("CorePoolSize", CORE_SIZE_DESC).value(corePoolSize);
        mreg.register("LargestPoolSize", this::getLargestPoolSize, LARGEST_SIZE_DESC);
        mreg.intMetric("MaximumPoolSize", MAX_SIZE_DESC).value(maxPoolSize);
        mreg.register("PoolSize", this::getPoolSize, POOL_SIZE_DESC);
        mreg.register("TaskCount", this::getTaskCount, TASK_COUNT_DESC);
        mreg.register("QueueSize", this::getQueueSize, QUEUE_SIZE_DESC);
        mreg.longMetric("KeepAliveTime", KEEP_ALIVE_TIME_DESC).value(keepAliveTime);
        mreg.register("Shutdown", this::isShutdown, IS_SHUTDOWN_DESC);
        mreg.register("Terminated", this::isTerminated, IS_TERMINATED_DESC);
        mreg.register("Terminating", this::isTerminating, IS_TERMINATING_DESC);
        mreg.register("RejectedExecutionHandlerClass", () -> rejectedExecHndClsName, String.class, REJ_HND_DESC);
        mreg.register("ThreadFactoryClass", () -> threadFactoryClsName, String.class, THRD_FACTORY_DESC);

        for (IgniteThreadPoolExecutor exec : execs)
            exec.initializeExecTimeMetric(mreg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteStripedThreadPoolExecutor.class, this);
    }
}
