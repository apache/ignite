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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class SecurityAwareIgniteStripedThreadPoolExecutor extends IgniteStripedThreadPoolExecutor {
    /** */
    public static SecurityAwareHolder<IgniteStripedThreadPoolExecutor> holder(GridKernalContext ctx,
        IgniteStripedThreadPoolExecutor original) {
        return new SecurityAwareHolder<IgniteStripedThreadPoolExecutor>(ctx, original) {
            @Override protected IgniteStripedThreadPoolExecutor createSecurityAwareInstance() {
                return new SecurityAwareIgniteStripedThreadPoolExecutor(ctx, original);
            }
        };
    }

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteStripedThreadPoolExecutor original;

    /** */
    public SecurityAwareIgniteStripedThreadPoolExecutor(GridKernalContext ctx,
        IgniteStripedThreadPoolExecutor original) {
        this.ctx = ctx;
        this.original = original;
    }

    /** */
    private Runnable convertToSecurityAware(Runnable cmd) {
        return SecurityUtils.isAuthentificated(ctx) ? new SecurityAwareRunnable(ctx.security(), cmd) : cmd;
    }

    /** */
    private <T> Callable<T> convertToSecurityAware(Callable<T> tsk) {
        return SecurityUtils.isAuthentificated(ctx) ? new SecurityAwareCallable<>(ctx.security(), tsk) : tsk;
    }

    /** */
    private <T> Collection<? extends Callable<T>> convertToSecurityAware(Collection<? extends Callable<T>> tasks) {
        return SecurityUtils.isAuthentificated(ctx)
            ? tasks.stream().map(t -> new SecurityAwareCallable<>(ctx.security(), t)).collect(Collectors.toList())
            : tasks;
    }

    /** {@inheritDoc} */
    @Override public void execute(Runnable task, int idx) {
        original.execute(convertToSecurityAware(task), idx);
    }

    /** {@inheritDoc} */
    @Override public int threadId(int idx) {
        return original.threadId(idx);
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        original.shutdown();
    }

    /** {@inheritDoc} */
    @Override public List<Runnable> shutdownNow() {
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
    @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return original.awaitTermination(timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T> Future<T> submit(Callable<T> task) {
        return original.submit(convertToSecurityAware(task));
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T> Future<T> submit(Runnable task, T res) {
        return original.submit(convertToSecurityAware(task), res);
    }

    /** {@inheritDoc} */
    @Override public @NotNull Future<?> submit(Runnable task) {
        return original.submit(convertToSecurityAware(task));
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
        return original.invokeAll(convertToSecurityAware(tasks));
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
        TimeUnit unit) {
        return original.invokeAll(convertToSecurityAware(tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public <T> @NotNull T invokeAny(Collection<? extends Callable<T>> tasks) {
        return original.invokeAny(convertToSecurityAware(tasks));
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout,
        TimeUnit unit) {
        return original.invokeAny(convertToSecurityAware(tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public void execute(Runnable cmd) {
        original.execute(convertToSecurityAware(cmd));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return original.toString();
    }
}
