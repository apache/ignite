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
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;

/**
 * An {@link ExecutorService} that executes submitted tasks using pooled grid threads.
 *
 * In addition to what it allows to track all enqueued tasks completion.
 */
public class IgniteTrackingThreadPoolExecutor extends ThreadPoolExecutor {
    /** */
    private final LongAdder pendingTaskCnt = new LongAdder();

    /** */
    private final LongAdder completedTaskCnt = new LongAdder();

    /** */
    private volatile boolean initialized;

    /** */
    private Queue<Throwable> exceptions = new ConcurrentLinkedQueue<>();

    /**
     * Creates a new service with the given initial parameters.
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
    public IgniteTrackingThreadPoolExecutor(
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
    public IgniteTrackingThreadPoolExecutor(
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
     * @param corePoolSize The number of threads to keep in the pool, even if they are idle.
     * @param maxPoolSize The maximum number of threads to allow in the pool.
     * @param keepAliveTime When the number of threads is greater than the core, this is the maximum time
     *      that excess idle threads will wait for new tasks before terminating.
     * @param workQ The queue to use for holding tasks before they are executed. This queue will hold only the
     *      runnable tasks submitted by the {@link #execute(Runnable)} method.
     * @param threadFactory Thread factory.
     */
    public IgniteTrackingThreadPoolExecutor(
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
    @Override public void execute(Runnable cmd) {
        pendingTaskCnt.add(1);

        super.execute(cmd);
    }

    /** {@inheritDoc} */
    @Override protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);

        completedTaskCnt.add(1);

        if (t != null)
            exceptions.add(t);

        if (initialized && pendingTaskCnt.sum() == completedTaskCnt.sum()) {
            synchronized (this) {
                notify();
            }
        }
    }

    /**
     * Mark this executor as initialized. This method should be called when all task are enqueued for execution.
     */
    public void markInitialized() {
        initialized = true;
    }

    /**
     *
     * @return {@code True} when all enqueued task are completed.
     */
    public boolean isDone() {
        return initialized && completedTaskCnt.sum() == pendingTaskCnt.sum();
    }

    /**
     * Wait synchronously until all tasks are completed and reset executor state for subsequent reuse.
     *
     * @throws IgniteCheckedException if some tasks execution result in error.
     */
    public synchronized void awaitDone() throws IgniteCheckedException {
        while(!(initialized && completedTaskCnt.sum() == pendingTaskCnt.sum())) {
            try {
                wait();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (!exceptions.isEmpty()) {
            IgniteCheckedException ex = new IgniteCheckedException("An execution resulted in exception");

            for (Throwable exception : exceptions)
                ex.addSuppressed(exception);

            throw ex;
        }
    }

    /**
     * Reset task completion tracking context. The method should be called before adding tasks to the executor.
     */
    public void reset() {
        initialized = false;
        completedTaskCnt.reset();
        pendingTaskCnt.reset();
        exceptions.clear();
    }
}
