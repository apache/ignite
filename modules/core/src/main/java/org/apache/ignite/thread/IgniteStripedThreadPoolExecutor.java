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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * An {@link ExecutorService} that executes submitted tasks using pooled grid threads.
 */
public class IgniteStripedThreadPoolExecutor implements ExecutorService {
    /** */
    private final ExecutorService[] execs;

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
     */
    public IgniteStripedThreadPoolExecutor(
        int concurrentLvl,
        String igniteInstanceName,
        String threadNamePrefix,
        UncaughtExceptionHandler eHnd,
        boolean allowCoreThreadTimeOut,
        long keepAliveTime) {
        execs = new ExecutorService[concurrentLvl];

        ThreadFactory factory = new IgniteThreadFactory(igniteInstanceName, threadNamePrefix, eHnd);

        for (int i = 0; i < concurrentLvl; i++) {
            IgniteThreadPoolExecutor executor = new IgniteThreadPoolExecutor(
                1,
                1,
                keepAliveTime,
                new LinkedBlockingQueue<>(),
                factory);

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
    @Override public String toString() {
        return S.toString(IgniteStripedThreadPoolExecutor.class, this);
    }
}
