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

    /** {@inheritDoc} */
    public IgniteTaskTrackingThreadPoolExecutor(String threadNamePrefix, String igniteInstanceName, int corePoolSize,
        int maxPoolSize, long keepAliveTime, BlockingQueue<Runnable> workQ) {
        super(threadNamePrefix, igniteInstanceName, corePoolSize, maxPoolSize, keepAliveTime, workQ);
    }

    /** {@inheritDoc} */
    public IgniteTaskTrackingThreadPoolExecutor(String threadNamePrefix, String igniteInstanceName, int corePoolSize,
        int maxPoolSize, long keepAliveTime, BlockingQueue<Runnable> workQ, byte plc,
        UncaughtExceptionHandler eHnd) {
        super(threadNamePrefix, igniteInstanceName, corePoolSize, maxPoolSize, keepAliveTime, workQ, plc, eHnd);
    }

    /** {@inheritDoc} */
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
     *
     * @return {@code True} if any task execution resulted in error.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public final boolean isError() {
        return err.get() != null;
    }

    /**
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
