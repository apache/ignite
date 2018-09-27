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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;

/**
 * An {@link ExecutorService} that executes submitted tasks using pooled grid threads.
 *
 * In addition to what it allows to track all enqueued tasks completion or failure during execution.
 */
public class IgniteTaskTrackingThreadPoolExecutor extends IgniteThreadPoolExecutor {
    /** */
    private final LongAdder pendingTaskCnt = new LongAdder();

    /** */
    private final LongAdder completedTaskCnt = new LongAdder();

    /** */
    private volatile boolean initialized;

    /** */
    private volatile AtomicReference<Throwable> err = new AtomicReference<>();

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
    public IgniteTaskTrackingThreadPoolExecutor(String threadNamePrefix, String igniteInstanceName, int corePoolSize,
        int maxPoolSize, long keepAliveTime, BlockingQueue<Runnable> workQ) {
        super(threadNamePrefix, igniteInstanceName, corePoolSize, maxPoolSize, keepAliveTime, workQ);
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
    public IgniteTaskTrackingThreadPoolExecutor(String threadNamePrefix, String igniteInstanceName, int corePoolSize,
        int maxPoolSize, long keepAliveTime, BlockingQueue<Runnable> workQ, byte plc,
        UncaughtExceptionHandler eHnd) {
        super(threadNamePrefix, igniteInstanceName, corePoolSize, maxPoolSize, keepAliveTime, workQ, plc, eHnd);
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
    public IgniteTaskTrackingThreadPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime,
        BlockingQueue<Runnable> workQ, ThreadFactory threadFactory) {
        super(corePoolSize, maxPoolSize, keepAliveTime, workQ, threadFactory);
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

        if (t != null && err.compareAndSet(null, t) || isDone()) {
            synchronized (this) {
                notifyAll();
            }
        }
    }

    /**
     * Mark this executor as initialized.
     * This method should be called when all required tasks are enqueued for execution.
     */
    public final void markInitialized() {
        initialized = true;
    }

    /**
     * Check error status.
     *
     * @return {@code True} if any task execution resulted in error.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public final boolean isError() {
        return err.get() != null;
    }

    /**
     * Check done status.
     *
     * @return {@code True} when all enqueued task are completed.
     */
    public final boolean isDone() {
        return initialized && completedTaskCnt.sum() == pendingTaskCnt.sum();
    }

    /**
     * Wait synchronously until all tasks are completed or error has occurred.
     *
     * @throws IgniteCheckedException if task execution resulted in error.
     */
    public final synchronized void awaitDone() throws IgniteCheckedException {
        // There are no guarantee what all enqueued tasks will be finished if an error has occurred.
        while(!isError() && !isDone()) {
            try {
                wait();
            }
            catch (InterruptedException e) {
                err.set(e);

                Thread.currentThread().interrupt();
            }
        }

        if (isError())
            throw new IgniteCheckedException("Task execution resulted in error", err.get());
    }

    /**
     * Reset tasks tracking context.
     * The method should be called before adding new tasks to the executor.
     */
    public final void reset() {
        initialized = false;
        completedTaskCnt.reset();
        pendingTaskCnt.reset();
        err.set(null);
    }
}
