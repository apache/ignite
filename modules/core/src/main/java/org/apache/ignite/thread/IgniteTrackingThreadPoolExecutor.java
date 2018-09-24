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
public class IgniteTrackingThreadPoolExecutor extends IgniteThreadPoolExecutor {
    /** */
    private final LongAdder pendingTaskCnt = new LongAdder();

    /** */
    private final LongAdder completedTaskCnt = new LongAdder();

    /** */
    private volatile boolean initialized;

    /** */
    private Queue<Throwable> exceptions = new ConcurrentLinkedQueue<>();

    /** {@inheritDoc} */
    public IgniteTrackingThreadPoolExecutor(String threadNamePrefix, String igniteInstanceName, int corePoolSize,
        int maxPoolSize, long keepAliveTime, BlockingQueue<Runnable> workQ) {
        super(threadNamePrefix, igniteInstanceName, corePoolSize, maxPoolSize, keepAliveTime, workQ);
    }

    /** {@inheritDoc} */
    public IgniteTrackingThreadPoolExecutor(String threadNamePrefix, String igniteInstanceName, int corePoolSize,
        int maxPoolSize, long keepAliveTime, BlockingQueue<Runnable> workQ, byte plc,
        UncaughtExceptionHandler eHnd) {
        super(threadNamePrefix, igniteInstanceName, corePoolSize, maxPoolSize, keepAliveTime, workQ, plc, eHnd);
    }

    /** {@inheritDoc} */
    public IgniteTrackingThreadPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime,
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
