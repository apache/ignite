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

package org.apache.ignite.internal.processors.security.thread;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;

/**
 * Extends {@link ThreadPoolExecutor} with the ability to execute tasks in security context that was actual when task was
 * added to executor's queue.
 */
public class SecurityAwareThreadPoolExecutor extends IgniteThreadPoolExecutor {
    /** */
    private final IgniteSecurity security;

    /** */
    public SecurityAwareThreadPoolExecutor(
        IgniteSecurity security,
        String threadNamePrefix,
        String igniteInstanceName,
        int corePoolSize,
        int maxPoolSize,
        long keepAliveTime,
        BlockingQueue<Runnable> workQ,
        byte plc,
        Thread.UncaughtExceptionHandler eHnd
    ) {
        super(threadNamePrefix, igniteInstanceName, corePoolSize, maxPoolSize, keepAliveTime, workQ, plc, eHnd);

        this.security = security;
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> Future<T> submit(@NotNull Callable<T> task) {
        return super.submit(SecurityAwareCallable.of(security, task));
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> Future<T> submit(@NotNull Runnable task, T res) {
        return super.submit(SecurityAwareRunnable.of(security, task), res);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Future<?> submit(@NotNull Runnable task) {
        return super.submit(SecurityAwareRunnable.of(security, task));
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> List<Future<T>> invokeAll(
        @NotNull Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return super.invokeAll(SecurityAwareCallable.of(security, tasks));
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks,
        long timeout, @NotNull TimeUnit unit) throws InterruptedException {
        return super.invokeAll(SecurityAwareCallable.of(security, tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
        return super.invokeAny(SecurityAwareCallable.of(security, tasks));
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks,
        long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return super.invokeAny(SecurityAwareCallable.of(security, tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public void execute(@NotNull Runnable cmd) {
        super.execute(SecurityAwareRunnable.of(security, cmd));
    }
}
