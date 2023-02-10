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

package org.apache.ignite.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.platform.compute.PlatformCompute;
import org.apache.ignite.internal.processors.task.GridTaskProcessor;
import org.apache.ignite.internal.processors.task.TaskExecutionOptions;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridClosureCallMode.BALANCE;
import static org.apache.ignite.internal.GridClosureCallMode.BROADCAST;

/**
 * Represents a general facade that delegates Ignite compute requests to {@link GridTaskProcessor} and
 * {@link GridClosureProcessor} internal processors. It also provides a built-in mechanism for accumulating execution
 * options that are automatically applied to computations.
 *
 * @see TaskExecutionOptions
 * @see IgniteComputeImpl
 * @see PlatformCompute
 */
public class IgniteComputeHandler {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Task execution options. */
    private final ThreadLocal<TaskExecutionOptions> opts;

    /**
     * Creates copy of specified computation handler. The result instance will inherit all options set for the original
     * handler so far with {@code optsTransformer} applied to them.
     *
     * @param other Ignite computation handler.
     * @param optsTransformer Function for initial execution options customization.
     */ 
    public IgniteComputeHandler(
        IgniteComputeHandler other,
        Function<TaskExecutionOptions, TaskExecutionOptions> optsTransformer
    ) {
        this(other.ctx, optsTransformer);

        TaskExecutionOptions copy = TaskExecutionOptions.options(other.opts.get());

        opts.set(optsTransformer.apply(copy));
    }

    /**
     * @param ctx Kernal context.
     * @param optsTransformer Function for initial execution options customization.
     */
    public IgniteComputeHandler(
        GridKernalContext ctx,
        Function<TaskExecutionOptions, TaskExecutionOptions> optsTransformer
    ) {
        this.ctx = ctx;

        opts = ThreadLocal.withInitial(() -> optsTransformer.apply(TaskExecutionOptions.options()));
    }

    /**
     * Affinity run implementation.
     *
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    public IgniteInternalFuture<?> affinityRunAsync(String cacheName, Object affKey, IgniteRunnable job) {
        guard();

        try {
            A.notNull(affKey, "affKey");
            A.notNull(job, "job");
            CU.validateCacheName(cacheName);

            return ctx.closure().affinityRun(Collections.singletonList(cacheName), affKey, job, opts.get());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Affinity run implementation.
     *
     * @param cacheNames Cache names collection.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    public IgniteInternalFuture<?> affinityRunAsync(@NotNull Collection<String> cacheNames, Object affKey, IgniteRunnable job) {
        guard();

        try {
            A.notNull(affKey, "affKey");
            A.notNull(job, "job");
            A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");
            CU.validateCacheNames(cacheNames);

            return ctx.closure().affinityRun(cacheNames, affKey, job, opts.get());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Affinity run implementation.
     *
     * @param cacheNames Cache names collection.
     * @param partId partition ID.
     * @param job Job.
     * @return Internal future.
     */
    public IgniteInternalFuture<?> affinityRunAsync(@NotNull Collection<String> cacheNames, int partId, IgniteRunnable job) {
        guard();

        try {
            A.ensure(partId >= 0, "partId = " + partId);
            A.notNull(job, "job");
            A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");
            CU.validateCacheNames(cacheNames);

            return ctx.closure().affinityRun(cacheNames, partId, job, opts.get());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Affinity call implementation.

     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    public <R> IgniteInternalFuture<R> affinityCallAsync(String cacheName, Object affKey, IgniteCallable<R> job) {
        guard();

        try {
            A.notNull(affKey, "affKey");
            A.notNull(job, "job");
            CU.validateCacheName(cacheName);

            return ctx.closure().affinityCall(Collections.singletonList(cacheName), affKey, job, opts.get());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Affinity call implementation.

     * @param cacheNames Cache names collection.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    public <R> IgniteInternalFuture<R> affinityCallAsync(@NotNull Collection<String> cacheNames, Object affKey, IgniteCallable<R> job) {
        guard();

        try {
            A.notNull(affKey, "affKey");
            A.notNull(job, "job");
            A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");
            CU.validateCacheNames(cacheNames);

            return ctx.closure().affinityCall(cacheNames, affKey, job, opts.get());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Affinity call implementation.

     * @param cacheNames Cache names collection.
     * @param partId Partition ID.
     * @param job Job.
     * @return Internal future.
     */
    public <R> IgniteInternalFuture<R> affinityCallAsync(@NotNull Collection<String> cacheNames, int partId, IgniteCallable<R> job) {
        guard();

        try {
            A.ensure(partId >= 0, "partId = " + partId);
            A.notNull(job, "job");
            A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");
            CU.validateCacheNames(cacheNames);

            return ctx.closure().affinityCall(cacheNames, partId, job, opts.get());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Execute implementation.
     *
     * @param taskName Task name.
     * @param arg Argument.
     * @return Internal future.
     */
    public <T, R> IgniteInternalFuture<R> executeAsync(String taskName, @Nullable T arg) {
        guard();

        try {
            A.notNull(taskName, "taskName");

            return ctx.task().execute(taskName, arg, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Execute implementation.
     *
     * @param taskCls Task class.
     * @param arg Argument.
     * @return Internal future.
     */
    public <T, R> IgniteInternalFuture<R> executeAsync(Class<? extends ComputeTask<T, R>> taskCls, @Nullable T arg) {
        guard();

        try {
            A.notNull(taskCls, "taskCls");

            return ctx.task().execute(taskCls, arg, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Execute implementation.
     *
     * @param task Task.
     * @param arg Task argument.
     * @return Task future.
     */
    public <T, R> ComputeTaskInternalFuture<R> executeAsync(ComputeTask<T, R> task, @Nullable T arg) {
        guard();

        try {
            A.notNull(task, "task");

            return ctx.task().execute(task, arg, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Broadcast implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    public IgniteInternalFuture<?> broadcastAsync(IgniteRunnable job) {
        guard();

        try {
            A.notNull(job, "job");

            return ctx.closure().runAsync(BROADCAST, job, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Broadcast implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    public <R> IgniteInternalFuture<Collection<R>> broadcastAsync(IgniteCallable<R> job) {
        guard();

        try {
            A.notNull(job, "job");

            return ctx.closure().callAsync(BROADCAST, Collections.singletonList(job), opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Broadcast implementation.
     *
     * @param job Job.
     * @param arg Argument.
     * @return Internal future.
     */
    public <R, T> IgniteInternalFuture<Collection<R>> broadcastAsync(IgniteClosure<T, R> job, @Nullable T arg) {
        guard();

        try {
            A.notNull(job, "job");

            return ctx.closure().broadcast(job, arg, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Run implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    public IgniteInternalFuture<?> runAsync(IgniteRunnable job) {
        guard();

        try {
            A.notNull(job, "job");

            return ctx.closure().runAsync(BALANCE, job, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Run implementation.
     *
     * @param jobs Jobs.
     * @return Internal future.
     */
    public IgniteInternalFuture<?> runAsync(Collection<? extends IgniteRunnable> jobs) {
        guard();

        try {
            A.notEmpty(jobs, "jobs");

            return ctx.closure().runAsync(BALANCE, jobs, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Apply implementation.
     *
     * @param job Job.
     * @param arg Argument.
     * @return Internal future.
     */
    public <R, T> IgniteInternalFuture<R> applyAsync(IgniteClosure<T, R> job, @Nullable T arg) {
        guard();

        try {
            A.notNull(job, "job");

            return ctx.closure().callAsync(job, arg, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Call implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    public <R> IgniteInternalFuture<R> callAsync(IgniteCallable<R> job) {
        guard();

        try {
            A.notNull(job, "job");

            return ctx.closure().callAsync(BALANCE, job, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Call implementation.
     *
     * @param jobs Jobs.
     * @return Internal future.
     */
    public <R> IgniteInternalFuture<Collection<R>> callAsync(Collection<? extends IgniteCallable<R>> jobs) {
        guard();

        try {
            A.notEmpty(jobs, "jobs");

            return ctx.closure().callAsync(BALANCE, jobs, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Apply implementation.
     *
     * @param job Job.
     * @param args Arguments/
     * @return Internal future.
     */
    public <T, R> IgniteInternalFuture<Collection<R>> applyAsync(final IgniteClosure<T, R> job,
        @Nullable Collection<? extends T> args) {
        guard();

        try {
            A.notNull(job, "job");
            A.notNull(args, "args");

            return ctx.closure().callAsync(job, args, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Call with reducer implementation.
     *
     * @param jobs Jobs.
     * @param rdc Reducer.
     * @return Internal future.
     */
    public <R1, R2> IgniteInternalFuture<R2> callAsync(Collection<? extends IgniteCallable<R1>> jobs,
        IgniteReducer<R1, R2> rdc) {
        guard();

        try {
            A.notEmpty(jobs, "jobs");
            A.notNull(rdc, "rdc");

            return ctx.closure().forkjoinAsync(BALANCE, jobs, rdc, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /**
     * Apply with reducer implementation.
     *
     * @param job Job
     * @param args Arguments.
     * @param rdc Reducer.
     * @return Internal future.
     */
    public <R1, R2, T> IgniteInternalFuture<R2> applyAsync(
        IgniteClosure<T, R1> job,
        Collection<? extends T> args,
        IgniteReducer<R1, R2> rdc
    ) {
        guard();

        try {
            A.notNull(job, "job");
            A.notNull(rdc, "rdc");
            A.notNull(args, "args");

            return ctx.closure().callAsync(job, args, rdc, opts.get());
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /** */
    public IgniteComputeHandler withName(String taskName) {
        A.notNull(taskName, "taskName");

        guard();

        try {
            opts.get().withName(taskName);
        }
        finally {
            unguard();
        }

        return this;
    }

    /** */
    public IgniteComputeHandler withTimeout(long timeout) {
        A.ensure(timeout >= 0, "timeout >= 0");

        guard();

        try {
            opts.get().withTimeout(timeout);
        }
        finally {
            unguard();
        }

        return this;
    }

    /** */
    public IgniteComputeHandler withNoFailover() {
        guard();

        try {
            opts.get().withFailoverDisabled();
        }
        finally {
            unguard();
        }

        return this;
    }

    /** */
    public IgniteComputeHandler withProjection(Collection<ClusterNode> prj) {
        A.notEmpty(prj, "projection");

        guard();

        try {
            opts.get().withProjection(prj);
        }
        finally {
            unguard();
        }

        return this;
    }

    /** */
    public IgniteComputeHandler withNoResultCache() {
        guard();

        try {
            opts.get().withResultCacheDisabled();
        }
        finally {
            unguard();
        }

        return this;
    }

    /** */
    public IgniteComputeHandler withExecutor(String execName) {
        A.notEmpty(execName, "executor name");

        guard();

        try {
            opts.get().withExecutor(execName);
        }
        finally {
            unguard();
        }

        return this;
    }

    /** */
    public IgniteComputeHandler withProjectionPredicate(IgnitePredicate<ClusterNode> predicate) {
        guard();

        try {
            opts.get().withProjectionPredicate(predicate);
        }
        finally {
            unguard();
        }

        return this;
    }

    /**
     * <tt>ctx.gateway().readLock()</tt>
     */
    private void guard() {
        ctx.gateway().readLock();
    }

    /**
     * <tt>ctx.gateway().readUnlock()</tt>
     */
    private void unguard() {
        ctx.gateway().readUnlock();
    }
}
