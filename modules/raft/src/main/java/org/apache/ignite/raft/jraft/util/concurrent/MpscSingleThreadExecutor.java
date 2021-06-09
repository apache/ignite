/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util.concurrent;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.raft.jraft.util.Mpsc;
import org.apache.ignite.raft.jraft.util.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MpscSingleThreadExecutor implements SingleThreadExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(MpscSingleThreadExecutor.class);

    private static final AtomicIntegerFieldUpdater<MpscSingleThreadExecutor> STATE_UPDATER = AtomicIntegerFieldUpdater
        .newUpdater(MpscSingleThreadExecutor.class, "state");

    private static final long DEFAULT_SHUTDOWN_TIMEOUT = 15;

    private static final int ST_NOT_STARTED = 1;
    private static final int ST_STARTED = 2;
    private static final int ST_SHUTDOWN = 3;
    private static final int ST_TERMINATED = 4;

    private static final Runnable WAKEUP_TASK = () -> {
    };

    private final Queue<Runnable> taskQueue;
    private final Executor executor;
    private final RejectedExecutionHandler rejectedExecutionHandler;
    private final Set<Runnable> shutdownHooks = new LinkedHashSet<>();
    private final Semaphore threadLock = new Semaphore(0);

    private volatile int state = ST_NOT_STARTED;
    private volatile Worker worker;

    public MpscSingleThreadExecutor(int maxPendingTasks, ThreadFactory threadFactory) {
        this(maxPendingTasks, threadFactory, RejectedExecutionHandlers.reject());
    }

    public MpscSingleThreadExecutor(int maxPendingTasks, ThreadFactory threadFactory,
        RejectedExecutionHandler rejectedExecutionHandler) {
        this.taskQueue = newTaskQueue(maxPendingTasks);
        this.executor = new ThreadPerTaskExecutor(threadFactory);
        this.rejectedExecutionHandler = rejectedExecutionHandler;
    }

    @Override
    public boolean shutdownGracefully() {
        return shutdownGracefully(DEFAULT_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
    }

    @Override
    public boolean shutdownGracefully(final long timeout, final TimeUnit unit) {
        Requires.requireNonNull(unit, "unit");
        if (isShutdown()) {
            return awaitTermination(timeout, unit);
        }

        boolean wakeup;
        int oldState;
        for (; ; ) {
            if (isShutdown()) {
                return awaitTermination(timeout, unit);
            }
            int newState;
            wakeup = true;
            oldState = this.state;
            switch (oldState) {
                case ST_NOT_STARTED:
                case ST_STARTED:
                    newState = ST_SHUTDOWN;
                    break;
                default:
                    newState = oldState;
                    wakeup = false;
            }
            if (STATE_UPDATER.compareAndSet(this, oldState, newState)) {
                break;
            }
        }

        if (oldState == ST_NOT_STARTED) {
            try {
                doStartWorker();
            }
            catch (final Throwable t) {
                this.state = ST_TERMINATED;

                if (!(t instanceof Exception)) {
                    // Also rethrow as it may be an OOME for example
                    throw new RuntimeException(t);
                }
                return true;
            }
        }

        if (wakeup) {
            wakeupAndStopWorker();
        }

        return awaitTermination(timeout, unit);
    }

    @Override
    public void execute(final Runnable task) {
        Requires.requireNonNull(task, "task");

        addTask(task);
        startWorker();
        wakeupForTask();
    }

    /**
     * Add a {@link Runnable} which will be executed on shutdown of this instance.
     */
    public void addShutdownHook(final Runnable task) {
        execute(() -> MpscSingleThreadExecutor.this.shutdownHooks.add(task));
    }

    /**
     * Remove a previous added {@link Runnable} as a shutdown hook.
     */
    public void removeShutdownHook(final Runnable task) {
        execute(() -> MpscSingleThreadExecutor.this.shutdownHooks.remove(task));
    }

    private boolean runShutdownHooks() {
        boolean ran = false;
        // Note shutdown hooks can add / remove shutdown hooks.
        while (!this.shutdownHooks.isEmpty()) {
            final List<Runnable> copy = new ArrayList<>(this.shutdownHooks);
            this.shutdownHooks.clear();
            for (final Runnable task : copy) {
                try {
                    task.run();
                }
                catch (final Throwable t) {
                    LOG.warn("Shutdown hook raised an exception.", t);
                }
                finally {
                    ran = true;
                }
            }
        }
        return ran;
    }

    public boolean isShutdown() {
        return this.state >= ST_SHUTDOWN;
    }

    public boolean isTerminated() {
        return this.state == ST_TERMINATED;
    }

    public boolean inWorkerThread(final Thread thread) {
        final Worker worker = this.worker;
        return worker != null && worker.thread == thread;
    }

    public boolean awaitTermination(final long timeout, final TimeUnit unit) {
        Requires.requireNonNull(unit, "unit");

        try {
            if (this.threadLock.tryAcquire(timeout, unit)) {
                this.threadLock.release();
            }
        }
        catch (final InterruptedException ignored) {
            // ignored
        }

        return isTerminated();
    }

    protected Queue<Runnable> newTaskQueue(final int maxPendingTasks) {
        return maxPendingTasks == Integer.MAX_VALUE ? Mpsc.newMpscQueue() : Mpsc.newMpscQueue(maxPendingTasks);
    }

    /**
     * Add a task to the task queue or throws a {@link RejectedExecutionException} if this instance was shutdown
     * before.
     */
    protected void addTask(final Runnable task) {
        if (!offerTask(task)) {
            reject(task);
        }
    }

    protected final boolean offerTask(final Runnable task) {
        if (isShutdown()) {
            reject();
        }
        return this.taskQueue.offer(task);
    }

    private void wakeupForTask() {
        final Worker worker = this.worker;
        if (worker != null) {
            worker.notifyIfNeeded();
        }
    }

    private void wakeupAndStopWorker() {
        // Maybe the worker has not initialized yet and cant't be notify, so we
        // add a wakeup_task first, it may prevent the worker be blocked.
        this.taskQueue.offer(WAKEUP_TASK);
        final Worker worker = this.worker;
        if (worker != null) {
            worker.notifyAndStop();
        }
    }

    private void startWorker() {
        if (this.state != ST_NOT_STARTED) {
            // avoid CAS if not needed
            return;
        }
        if (STATE_UPDATER.compareAndSet(this, ST_NOT_STARTED, ST_STARTED)) {
            try {
                doStartWorker();
            }
            catch (final Throwable t) {
                this.state = ST_NOT_STARTED;
                throw new RuntimeException("Fail to start executor", t);
            }
        }
    }

    private void doStartWorker() {
        this.executor.execute(() -> {
            MpscSingleThreadExecutor.this.worker = new Worker(Thread.currentThread());

            try {
                MpscSingleThreadExecutor.this.worker.run();
            }
            catch (final Throwable t) {
                LOG.warn("Unexpected exception from executor: ", t);
            }
            finally {
                for (; ; ) {
                    int oldState = MpscSingleThreadExecutor.this.state;
                    if (oldState >= ST_SHUTDOWN || STATE_UPDATER.compareAndSet(MpscSingleThreadExecutor.this, oldState, ST_SHUTDOWN)) {
                        break;
                    }
                }

                runShutdownHooks();

                MpscSingleThreadExecutor.this.state = ST_TERMINATED;
                MpscSingleThreadExecutor.this.threadLock.release();
            }
        });
    }

    /**
     * Offers the task to the associated {@link RejectedExecutionHandler}.
     *
     * @param task to reject.
     */
    protected final void reject(final Runnable task) {
        this.rejectedExecutionHandler.rejected(task, this);
    }

    protected static void reject() {
        throw new RejectedExecutionException("Executor terminated");
    }

    private static final AtomicIntegerFieldUpdater<Worker> NOTIFY_UPDATER = AtomicIntegerFieldUpdater.newUpdater(
        Worker.class, "notifyNeeded");
    private static final int NOT_NEEDED = 0;
    private static final int NEEDED = 1;

    private class Worker implements Runnable {

        final Thread thread;
        volatile int notifyNeeded = NOT_NEEDED;
        boolean stop = false;

        private Worker(Thread thread) {
            this.thread = thread;
        }

        @Override
        public void run() {
            for (; ; ) {
                final Runnable task = pollTask();
                if (task == null) {
                    // wait task
                    synchronized (this) {
                        if (this.stop) {
                            break;
                        }
                        this.notifyNeeded = NEEDED;
                        try {
                            // Maybe the outer layer calls shutdown when the worker has not initialized yet,
                            // so we only wait a little while to recheck the conditions.
                            wait(1000, 10);

                            if (this.stop || isShutdown()) {
                                break;
                            }
                        }
                        catch (final InterruptedException ignored) {
                            // ignored
                        }
                    }
                    continue;
                }

                runTask(task);

                if (isShutdown()) {
                    break;
                }
            }

            runAllTasks();
        }

        private Runnable pollTask() {
            return MpscSingleThreadExecutor.this.taskQueue.poll();
        }

        private void runTask(final Runnable task) {
            try {
                task.run();
            }
            catch (final Throwable t) {
                LOG.warn("Caught an unknown error while executing a task", t);
            }
        }

        private void runAllTasks() {
            Runnable task;
            while ((task = pollTask()) != null) {
                runTask(task);
            }
        }

        private boolean isShuttingDown() {
            return MpscSingleThreadExecutor.this.state != ST_STARTED;
        }

        private void notifyIfNeeded() {
            if (this.notifyNeeded == NOT_NEEDED) {
                return;
            }
            if (NOTIFY_UPDATER.getAndSet(this, NOT_NEEDED) == NEEDED) {
                synchronized (this) {
                    notifyAll();
                }
            }
        }

        private void notifyAndStop() {
            synchronized (this) {
                this.stop = true;
                notifyAll();
            }
        }
    }

    private static class ThreadPerTaskExecutor implements Executor {

        private final ThreadFactory threadFactory;

        ThreadPerTaskExecutor(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
        }

        @Override
        public void execute(final Runnable task) {
            this.threadFactory.newThread(task).start();
        }
    }
}
