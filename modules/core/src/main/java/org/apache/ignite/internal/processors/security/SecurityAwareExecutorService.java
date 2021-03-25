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

package org.apache.ignite.internal.processors.security;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.GridKernalContext;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.security.SecurityAwareCallable.convertToSecurityAware;

/**
 * Executes tasks with a security context that was in force when executor's method was called.
 */
public class SecurityAwareExecutorService implements ExecutorService {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final ExecutorService original;

    /** */
    public SecurityAwareExecutorService(GridKernalContext ctx, ExecutorService original) {
        this.ctx = ctx;
        this.original = original;
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        original.shutdown();
    }

    /** {@inheritDoc} */
    @NotNull @Override public List<Runnable> shutdownNow() {
        return original.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
        return original.isShutdown();
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
        return original.isTerminated();
    }

    /** {@inheritDoc} */
    @Override public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
        return original.awaitTermination(timeout, unit);
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> Future<T> submit(@NotNull Callable<T> task) {
        return original.submit(new SecurityAwareCallable<>(ctx, task));
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> Future<T> submit(@NotNull Runnable task, T res) {
        return original.submit(new SecurityAwareRunnable(ctx, task), res);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Future<?> submit(@NotNull Runnable task) {
        return original.submit(new SecurityAwareRunnable(ctx, task));
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> List<Future<T>> invokeAll(
        @NotNull Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return original.invokeAll(convertToSecurityAware(ctx, tasks));
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks,
        long timeout, @NotNull TimeUnit unit) throws InterruptedException {
        return original.invokeAll(convertToSecurityAware(ctx, tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @NotNull @Override public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
        return original.invokeAny(convertToSecurityAware(ctx, tasks));
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks,
        long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return original.invokeAny(convertToSecurityAware(ctx, tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public void execute(@NotNull Runnable cmd) {
        original.execute(new SecurityAwareRunnable(ctx, cmd));
    }
}
