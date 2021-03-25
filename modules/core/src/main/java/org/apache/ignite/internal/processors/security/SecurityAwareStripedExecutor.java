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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.StripedExecutor;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.security.SecurityAwareCallable.convertToSecurityAware;

/**
 *
 */
public class SecurityAwareStripedExecutor extends StripedExecutor {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final StripedExecutor original;

    /** */
    public SecurityAwareStripedExecutor(GridKernalContext ctx, StripedExecutor original) {
        this.ctx = ctx;
        this.original = original;
    }

    /** {@inheritDoc} */
    @Override public void execute(int idx, Runnable cmd) {
        original.execute(idx, new SecurityAwareRunnable(ctx, cmd));
    }

    /** {@inheritDoc} */
    @Override public void execute(@NotNull Runnable cmd) {
        original.execute(new SecurityAwareRunnable(ctx, cmd));
    }

    /** {@inheritDoc} */
    @Override public boolean detectStarvation() {
        return original.detectStarvation();
    }

    /** {@inheritDoc} */
    @Override public int stripesCount() {
        return original.stripesCount();
    }

    /** {@inheritDoc} */
    @Override public Stripe[] stripes() {
        return original.stripes();
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        original.shutdown();
    }

    /** {@inheritDoc} */
    @Override public @NotNull List<Runnable> shutdownNow() {
        return original.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
        return original.awaitTermination(timeout, unit);
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
    @Override public void stop() {
        original.stop();
    }

    /** {@inheritDoc} */
    @Override public int queueSize() {
        return original.queueSize();
    }

    /** {@inheritDoc} */
    @Override public int queueStripeSize(int idx) {
        return original.queueStripeSize(idx);
    }

    /** {@inheritDoc} */
    @Override public long completedTasks() {
        return original.completedTasks();
    }

    /** {@inheritDoc} */
    @Override public long[] stripesCompletedTasks() {
        return original.stripesCompletedTasks();
    }

    /** {@inheritDoc} */
    @Override public boolean[] stripesActiveStatuses() {
        return original.stripesActiveStatuses();
    }

    /** {@inheritDoc} */
    @Override public int activeStripesCount() {
        return original.activeStripesCount();
    }

    /** {@inheritDoc} */
    @Override public int[] stripesQueueSizes() {
        return original.stripesQueueSizes();
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T> Future<T> submit(@NotNull Runnable task, T res) {
        return original.submit(new SecurityAwareRunnable(ctx, task), res);
    }

    /** {@inheritDoc} */
    @Override public @NotNull Future<?> submit(@NotNull Runnable task) {
        return original.submit(new SecurityAwareRunnable(ctx, task));
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T> Future<T> submit(@NotNull Callable<T> task) {
        return original.submit(new SecurityAwareCallable<>(ctx, task));
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T> List<Future<T>> invokeAll(
        @NotNull Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return original.invokeAll(convertToSecurityAware(ctx, tasks));
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks,
        long timeout, @NotNull TimeUnit unit) throws InterruptedException {
        return original.invokeAll(convertToSecurityAware(ctx, tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public <T> @NotNull T invokeAny(
        @NotNull Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return original.invokeAny(convertToSecurityAware(ctx, tasks));
    }

    /** {@inheritDoc} */
    @Override public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks, long timeout,
        @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return original.invokeAny(convertToSecurityAware(ctx, tasks), timeout, unit);
    }

    /** {@inheritDoc} */
    @Override public void awaitComplete(int... stripes) throws InterruptedException {
        original.awaitComplete(stripes);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return original.toString();
    }
}
