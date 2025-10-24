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

package org.apache.ignite.internal.thread;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.thread.context.concurrent.ThreadContextAwareScheduledExecutorService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UNDEFINED;
import static org.apache.ignite.internal.thread.context.function.ThreadContextAwareCallable.wrapIfActiveAttributesPresent;
import static org.apache.ignite.internal.thread.context.function.ThreadContextAwareRunnable.wrapIfActiveAttributesPresent;

/** */
public class IgniteScheduledThreadPoolExecutor extends ThreadContextAwareScheduledExecutorService {
    /**
     * @param threadNamePrefix Pool thread name prefix.
     * @param igniteInstanceName Ignite instance name.
     * @param poolSize Pool size.
     */
    public IgniteScheduledThreadPoolExecutor(String threadNamePrefix, String igniteInstanceName, int poolSize) {
        super(new ScheduledThreadPoolExecutor(
            poolSize,
            new IgniteThreadFactory(igniteInstanceName, threadNamePrefix, UNDEFINED, null))
        );
    }

    /** {@inheritDoc} */
    @NotNull @Override public ScheduledFuture<?> schedule(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
        return super.schedule(wrapIfActiveAttributesPresent(command), delay, unit);
    }

    /** {@inheritDoc} */
    @NotNull @Override public <V> ScheduledFuture<V> schedule(@NotNull Callable<V> callable, long delay, @NotNull TimeUnit unit) {
        return super.schedule(wrapIfActiveAttributesPresent(callable), delay, unit);
    }

    /** {@inheritDoc} */
    @NotNull @Override public ScheduledFuture<?> scheduleAtFixedRate(
        @NotNull Runnable command,
        long initialDelay,
        long period,
        @NotNull TimeUnit unit
    ) {
        return super.scheduleAtFixedRate(wrapIfActiveAttributesPresent(command), initialDelay, period, unit);
    }

    /** {@inheritDoc} */
    @NotNull @Override public ScheduledFuture<?> scheduleWithFixedDelay(
        @NotNull Runnable command,
        long initialDelay,
        long delay,
        @NotNull TimeUnit unit
    ) {
        return super.scheduleWithFixedDelay(wrapIfActiveAttributesPresent(command), initialDelay, delay, unit);
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> Future<T> submit(@NotNull Callable<T> task) {
        return super.submit(wrapIfActiveAttributesPresent(task));
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> Future<T> submit(@NotNull Runnable task, T result) {
        return super.submit(wrapIfActiveAttributesPresent(task), result);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Future<?> submit(@NotNull Runnable task) {
        return super.submit(wrapIfActiveAttributesPresent(task));
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return super.invokeAll(wrapIfActiveAttributesPresent(tasks));
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> List<Future<T>> invokeAll(
        @NotNull Collection<? extends Callable<T>> tasks,
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException {
        return super.invokeAll(wrapIfActiveAttributesPresent(tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> T invokeAny(
        @NotNull Collection<? extends Callable<T>> tasks
    ) throws InterruptedException, ExecutionException {
        return super.invokeAny(wrapIfActiveAttributesPresent(tasks));
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(
        @NotNull Collection<? extends Callable<T>> tasks,
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException, ExecutionException, TimeoutException {
        return super.invokeAny(wrapIfActiveAttributesPresent(tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public void execute(@NotNull Runnable command) {
        super.execute(wrapIfActiveAttributesPresent(command));
    }

    /**
     * @param threadNamePrefix Pool thread name prefix.
     * @param igniteInstanceName Ignite instance name.
     * @return Thread pool instance.
     */
    public static ScheduledExecutorService newSingleThreadScheduledExecutor(String threadNamePrefix, @Nullable String igniteInstanceName) {
        return new IgniteScheduledThreadPoolExecutor(threadNamePrefix, igniteInstanceName, 1);
    }
}
