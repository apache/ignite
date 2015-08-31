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

package org.apache.ignite.testframework;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Threads factory for safe test-threads management.
 */
public final class GridTestSafeThreadFactory implements ThreadFactory {
    /** Collection to hold all started threads across the JVM. */
    private static final BlockingQueue<Thread> startedThreads = new LinkedBlockingQueue<>();

    /* Lock protection of the started across the JVM threads collection. */
    private static final GridBusyLock startedThreadsLock = new GridBusyLock();

    /** Threads name prefix. */
    private final String threadName;

    /** Flag to interrupt all factory threads if any thread fails with unexpected exception. */
    private final boolean interruptAll;

    /** Created threads counter. */
    private final AtomicLong cnt = new AtomicLong();

    /** Collection of ALL created threads in this factory. */
    private final Collection<GridTestThread> threads = new ArrayList<>();

    /** The first thrown error during threads from this factory execution. */
    private final BlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();

    /**
     * Constructs threads factory for safe test-threads management.
     *
     * @param threadName threads name prefix.
     */
    public GridTestSafeThreadFactory(String threadName) {
        this(threadName, true);
    }

    /**
     * Constructs threads factory for safe test-threads management.
     *
     * @param threadName Threads name prefix.
     * @param interruptAll Interrupt all threads in factory if any thread fails with unexpected exception.
     */
    public GridTestSafeThreadFactory(String threadName, boolean interruptAll) {
        this.threadName = threadName;
        this.interruptAll = interruptAll;
    }

    /**
     * Create new thread around callable task.
     *
     * @param c Callable task to execute in the thread.
     * @return New thread around callable task.
     * @see GridTestThread
     */
    public Thread newThread(final Callable<?> c) {
        // Create new thread around the task.
        GridTestThread thread = new GridTestThread(c, threadName + '-' + cnt.incrementAndGet()) {
            @Override protected void onError(Throwable err) {
                // Save the exception.
                errors.add(err);

                // Interrupt execution of all other threads in this factory.
                if (interruptAll)
                    for (Thread t : threads)
                        t.interrupt();
            }

            @Override protected void onFinished() {
                super.onFinished();

                // No need to acquire lock here since it is a concurrent collection.
                startedThreads.remove(this);
            }
        };

        // Add this thread into the collection of managed threads.
        startedThreadsLock.enterBusy();

        try {
            startedThreads.add(thread);
        }
        finally {
            startedThreadsLock.leaveBusy();
        }

        // Register new thread in this factory.
        threads.add(thread);

        return thread;
    }

    /**
     * Create new thread around runnable task.
     *
     * @param r Runnable task to execute in the thread.
     * @return New thread around runnable task.
     * @see GridTestThread
     */
    @Override public Thread newThread(final Runnable r) {
        return newThread(GridTestUtils.makeCallable(r, null));
    }

    /**
     * Check and throws an exception if happens during this factory threads execution.
     *
     * @throws Exception If there is error.
     */
    public void checkError() throws Exception {
        Throwable err = errors.peek();
        if (err != null) {
            if (err instanceof Error)
                throw (Error)err;

            throw (Exception)err;
        }

        for (GridTestThread thread : threads) {
            thread.checkError();
        }
    }

    /**
     * Interrupts all threads, created by this thread factory.
     */
    public void interruptAllThreads() {
        for (Thread t : threads)
            U.interrupt(t);

        try {
            for (Thread t : threads)
                U.join(t);
        }
        catch (IgniteInterruptedCheckedException ignored) {
            // No-op.
        }
    }

    /**
     * Interrupts and waits for termination of all the threads started
     * so far by current test.
     *
     * @param log Logger.
     */
    static void stopAllThreads(IgniteLogger log) {
        startedThreadsLock.block();

        List<Thread> all;

        try {
            all = new ArrayList<>(startedThreads.size());
            startedThreads.drainTo(all);
        }
        finally {
            startedThreadsLock.unblock();
        }

        boolean aliveThreads = F.forAny(
            all,
            new P1<Thread>() {
                @Override public boolean apply(Thread t) {
                    return t.isAlive();
                }
            }
        );

        if (!aliveThreads)
            return;

        U.warn(log, "Interrupting threads started so far: " + all.size());

        U.interrupt(all);

        U.joinThreads(all, log);

        Iterator<Thread> it = all.iterator();

        for (Thread thread = it.next(); it.hasNext(); thread = it.next())
            if (!thread.isAlive())
                it.remove();

        if (all.isEmpty())
            U.warn(log, "Finished interrupting threads.");
        else
            U.error(log, "Finished interrupting threads, but some threads are still alive" +
                " [size=" + all.size() + ", threads=" + all + "]");
    }
}