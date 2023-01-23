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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDeploymentException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.processors.task.TaskExecutionOptions;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridClosureCallMode.BALANCE;
import static org.apache.ignite.internal.GridClosureCallMode.BROADCAST;

/**
 * {@link IgniteCompute} implementation.
 */
public class IgniteComputeImpl extends AsyncSupportAdapter<IgniteCompute>
    implements IgniteCompute, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridKernalContext ctx;

    /** */
    private ClusterGroupAdapter prj;

    /** Custom executor name. */
    private String execName;

    /** Default task execution options. */
    private final ThreadLocal<TaskExecutionOptions> opts = ThreadLocal.withInitial(() ->
        TaskExecutionOptions.options(prj.nodes()).withExecutor(execName)
    );

    /**
     * Required by {@link Externalizable}.
     */
    public IgniteComputeImpl() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     * @param prj Projection.
     */
    public IgniteComputeImpl(GridKernalContext ctx, ClusterGroupAdapter prj) {
        this(ctx, prj, false);
    }

    /**
     * @param ctx Kernal context.
     * @param prj Projection.
     * @param async Async support flag.
     */
    private IgniteComputeImpl(GridKernalContext ctx, ClusterGroupAdapter prj, boolean async) {
        super(async);

        this.ctx = ctx;
        this.prj = prj;
    }

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param prj Projection.
     * @param async Async support flag.
     * @param execName Custom executor name.
     */
    private IgniteComputeImpl(GridKernalContext ctx, ClusterGroupAdapter prj, boolean async,
        String execName) {
        super(async);

        this.ctx = ctx;
        this.prj = prj;
        this.execName = execName;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCompute createAsyncInstance() {
        return new IgniteComputeImpl(ctx, prj, true);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup clusterGroup() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(String cacheName, Object affKey, IgniteRunnable job) {
        try {
            saveOrGet(affinityRunAsync0(cacheName, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> affinityRunAsync(String cacheName, Object affKey,
        IgniteRunnable job) throws IgniteException {

        return (IgniteFuture<Void>)createFuture(affinityRunAsync0(cacheName, affKey, job));
    }

    /**
     * Affinity run implementation.
     *
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> affinityRunAsync0(String cacheName, Object affKey, IgniteRunnable job) {
        guard();

        try {
            A.notNull(affKey, "affKey");
            A.notNull(job, "job");
            CU.validateCacheName(cacheName);

            // In case cache key is passed instead of affinity key.
            final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);
            int partId = ctx.affinity().partition(cacheName, affKey0);

            if (partId < 0)
                throw new IgniteCheckedException("Failed map key to partition: [cache=" + cacheName + " key="
                    + affKey + ']');

            return ctx.closure().affinityRun(Collections.singletonList(cacheName), partId, job, opts.get());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(@NotNull Collection<String> cacheNames, Object affKey, IgniteRunnable job) {
        try {
            saveOrGet(affinityRunAsync0(cacheNames, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> affinityRunAsync(@NotNull Collection<String> cacheNames, Object affKey,
        IgniteRunnable job) throws IgniteException {
        return (IgniteFuture<Void>)createFuture(affinityRunAsync0(cacheNames, affKey, job));
    }

    /**
     * Affinity run implementation.
     *
     * @param cacheNames Cache names collection.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> affinityRunAsync0(@NotNull Collection<String> cacheNames, Object affKey,
        IgniteRunnable job) {
        guard();

        try {
            A.notNull(affKey, "affKey");
            A.notNull(job, "job");
            A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");
            CU.validateCacheNames(cacheNames);

            final String cacheName = F.first(cacheNames);

            // In case cache key is passed instead of affinity key.
            final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);
            int partId = ctx.affinity().partition(cacheName, affKey0);

            if (partId < 0)
                throw new IgniteCheckedException("Failed map key to partition: [cache=" + cacheName + " key="
                    + affKey + ']');

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

    /** {@inheritDoc} */
    @Override public void affinityRun(@NotNull Collection<String> cacheNames, int partId, IgniteRunnable job) {
        try {
            saveOrGet(affinityRunAsync0(cacheNames, partId, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> affinityRunAsync(@NotNull Collection<String> cacheNames, int partId,
        IgniteRunnable job) throws IgniteException {
        return (IgniteFuture<Void>)createFuture(affinityRunAsync0(cacheNames, partId, job));
    }

    /**
     * Affinity run implementation.
     *
     * @param cacheNames Cache names collection.
     * @param partId partition ID.
     * @param job Job.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> affinityRunAsync0(@NotNull Collection<String> cacheNames, int partId,
        IgniteRunnable job) {
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

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(String cacheName, Object affKey, IgniteCallable<R> job) {
        try {
            return saveOrGet(affinityCallAsync0(cacheName, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> affinityCallAsync(String cacheName, Object affKey,
        IgniteCallable<R> job) throws IgniteException {
        return createFuture(affinityCallAsync0(cacheName, affKey, job));
    }

    /**
     * Affinity call implementation.

     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<R> affinityCallAsync0(String cacheName, Object affKey,
        IgniteCallable<R> job) {
        guard();

        try {
            A.notNull(affKey, "affKey");
            A.notNull(job, "job");
            CU.validateCacheName(cacheName);

            // In case cache key is passed instead of affinity key.
            final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);
            int partId = ctx.affinity().partition(cacheName, affKey0);

            if (partId < 0)
                throw new IgniteCheckedException("Failed map key to partition: [cache=" + cacheName + " key="
                    + affKey + ']');

            return ctx.closure().affinityCall(Collections.singletonList(cacheName), partId, job, opts.get());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(@NotNull Collection<String> cacheNames, Object affKey, IgniteCallable<R> job) {
        try {
            return saveOrGet(affinityCallAsync0(cacheNames, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> affinityCallAsync(@NotNull Collection<String> cacheNames, Object affKey,
        IgniteCallable<R> job) throws IgniteException {
        return createFuture(affinityCallAsync0(cacheNames, affKey, job));
    }

    /**
     * Affinity call implementation.

     * @param cacheNames Cache names collection.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<R> affinityCallAsync0(@NotNull Collection<String> cacheNames, Object affKey,
        IgniteCallable<R> job) {
        guard();

        try {
            A.notNull(affKey, "affKey");
            A.notNull(job, "job");
            A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");
            CU.validateCacheNames(cacheNames);

            final String cacheName = F.first(cacheNames);

            // In case cache key is passed instead of affinity key.
            final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);
            int partId = ctx.affinity().partition(cacheName, affKey0);

            if (partId < 0)
                throw new IgniteCheckedException("Failed map key to partition: [cache=" + cacheName + " key="
                    + affKey + ']');

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

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(@NotNull Collection<String> cacheNames, int partId, IgniteCallable<R> job) {
        try {
            return saveOrGet(affinityCallAsync0(cacheNames, partId, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> affinityCallAsync(@NotNull Collection<String> cacheNames, int partId,
        IgniteCallable<R> job) throws IgniteException {
        return createFuture(affinityCallAsync0(cacheNames, partId, job));
    }

    /**
     * Affinity call implementation.

     * @param cacheNames Cache names collection.
     * @param partId Partition ID.
     * @param job Job.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<R> affinityCallAsync0(@NotNull Collection<String> cacheNames, int partId,
        IgniteCallable<R> job) {
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

    /** {@inheritDoc} */
    @Override public <T, R> R execute(String taskName, @Nullable T arg) {
        try {
            return saveOrGet(executeAsync0(taskName, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> ComputeTaskFuture<R> executeAsync(String taskName, @Nullable T arg) throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(executeAsync0(taskName, arg));
    }

    /**
     * Execute implementation.
     *
     * @param taskName Task name.
     * @param arg Argument.
     * @return Internal future.
     */
    private <T, R> IgniteInternalFuture<R> executeAsync0(String taskName, @Nullable T arg) {
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

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends ComputeTask<T, R>> taskCls, @Nullable T arg) {
        try {
            return saveOrGet(executeAsync0(taskCls, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> ComputeTaskFuture<R> executeAsync(Class<? extends ComputeTask<T, R>> taskCls,
        @Nullable T arg) throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(executeAsync0(taskCls, arg));
    }

    /**
     * Execute implementation.
     *
     * @param taskCls Task class.
     * @param arg Argument.
     * @return Internal future.
     */
    private <T, R> IgniteInternalFuture<R> executeAsync0(Class<? extends ComputeTask<T, R>> taskCls, @Nullable T arg) {
        guard();

        try {
            A.notNull(taskCls, "taskCls");

            return ctx.task().execute(taskCls, arg, opts.get().withProjectionPredicate(prj.predicate()));
        }
        finally {
            opts.remove();
            
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(ComputeTask<T, R> task, @Nullable T arg) {
        try {
            return saveOrGet(executeAsync0(task, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> ComputeTaskFuture<R> executeAsync(ComputeTask<T, R> task, @Nullable T arg)
        throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(executeAsync0(task, arg));
    }

    /**
     * Execute implementation.
     *
     * @param task Task.
     * @param arg Task argument.
     * @return Task future.
     */
    public <T, R> ComputeTaskInternalFuture<R> executeAsync0(ComputeTask<T, R> task, @Nullable T arg) {
        guard();

        try {
            A.notNull(task, "task");

            return ctx.task().execute(task, arg, opts.get().withProjectionPredicate(prj.predicate()));
        }
        finally {
            opts.remove();

            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void broadcast(IgniteRunnable job) {
        try {
            saveOrGet(broadcastAsync0(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> broadcastAsync(IgniteRunnable job) throws IgniteException {
        return (IgniteFuture<Void>)createFuture(broadcastAsync0(job));
    }

    /**
     * Broadcast implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> broadcastAsync0(IgniteRunnable job) {
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

    /** {@inheritDoc} */
    @Override public <R> Collection<R> broadcast(IgniteCallable<R> job) {
        try {
            return saveOrGet(broadcastAsync0(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<Collection<R>> broadcastAsync(IgniteCallable<R> job) throws IgniteException {
        return createFuture(broadcastAsync0(job));
    }

    /**
     * Broadcast implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<Collection<R>> broadcastAsync0(IgniteCallable<R> job) {
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

    /** {@inheritDoc} */
    @Override public <R, T> Collection<R> broadcast(IgniteClosure<T, R> job, @Nullable T arg) {
        try {
            return saveOrGet(broadcastAsync0(job, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> IgniteFuture<Collection<R>> broadcastAsync(IgniteClosure<T, R> job,
        @Nullable T arg) throws IgniteException {
        return createFuture(broadcastAsync0(job, arg));
    }

    /**
     * Broadcast implementation.
     *
     * @param job Job.
     * @param arg Argument.
     * @return Internal future.
     */
    private <R, T> IgniteInternalFuture<Collection<R>> broadcastAsync0(IgniteClosure<T, R> job, @Nullable T arg) {
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

    /** {@inheritDoc} */
    @Override public void run(IgniteRunnable job) {
        try {
            saveOrGet(runAsync0(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> runAsync(IgniteRunnable job) throws IgniteException {
        return (IgniteFuture<Void>)createFuture(runAsync0(job));
    }

    /**
     * Run implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> runAsync0(IgniteRunnable job) {
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

    /** {@inheritDoc} */
    @Override public void run(Collection<? extends IgniteRunnable> jobs) {
        try {
            saveOrGet(runAsync0(jobs));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> runAsync(Collection<? extends IgniteRunnable> jobs)
        throws IgniteException {
        return (IgniteFuture<Void>)createFuture(runAsync0(jobs));
    }

    /**
     * Run implementation.
     *
     * @param jobs Jobs.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> runAsync0(Collection<? extends IgniteRunnable> jobs) {
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

    /** {@inheritDoc} */
    @Override public <R, T> R apply(IgniteClosure<T, R> job, @Nullable T arg) {
        try {
            return saveOrGet(applyAsync0(job, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> IgniteFuture<R> applyAsync(IgniteClosure<T, R> job, @Nullable T arg)
        throws IgniteException {
        return createFuture(applyAsync0(job, arg));
    }

    /**
     * Apply implementation.
     *
     * @param job Job.
     * @param arg Argument.
     * @return Internal future.
     */
    private <R, T> IgniteInternalFuture<R> applyAsync0(IgniteClosure<T, R> job, @Nullable T arg) {
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

    /** {@inheritDoc} */
    @Override public <R> R call(IgniteCallable<R> job) {
        try {
            return saveOrGet(callAsync0(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> callAsync(IgniteCallable<R> job) throws IgniteException {
        return createFuture(callAsync0(job));
    }

    /**
     * Call implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<R> callAsync0(IgniteCallable<R> job) {
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

    /** {@inheritDoc} */
    @Override public <R> Collection<R> call(Collection<? extends IgniteCallable<R>> jobs) {
        try {
            return saveOrGet(callAsync0(jobs));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<Collection<R>> callAsync(
        Collection<? extends IgniteCallable<R>> jobs) throws IgniteException {
        return createFuture(callAsync0(jobs));
    }

    /**
     * Call implementation.
     *
     * @param jobs Jobs.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<Collection<R>> callAsync0(Collection<? extends IgniteCallable<R>> jobs) {
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

    /** {@inheritDoc} */
    @Override public <T, R> Collection<R> apply(final IgniteClosure<T, R> job, @Nullable Collection<? extends T> args) {
        try {
            return saveOrGet(applyAsync0(job, args));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> IgniteFuture<Collection<R>> applyAsync(IgniteClosure<T, R> job,
        Collection<? extends T> args) throws IgniteException {
        return createFuture(applyAsync0(job, args));
    }

    /**
     * Apply implementation.
     *
     * @param job Job.
     * @param args Arguments/
     * @return Internal future.
     */
    private <T, R> IgniteInternalFuture<Collection<R>> applyAsync0(final IgniteClosure<T, R> job,
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

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 call(Collection<? extends IgniteCallable<R1>> jobs, IgniteReducer<R1, R2> rdc) {
        try {
            return saveOrGet(callAsync0(jobs, rdc));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> IgniteFuture<R2> callAsync(Collection<? extends IgniteCallable<R1>> jobs,
        IgniteReducer<R1, R2> rdc) throws IgniteException {
        return createFuture(callAsync0(jobs, rdc));
    }

    /**
     * Call with reducer implementation.
     *
     * @param jobs Jobs.
     * @param rdc Reducer.
     * @return Internal future.
     */
    private <R1, R2> IgniteInternalFuture<R2> callAsync0(Collection<? extends IgniteCallable<R1>> jobs,
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

    /** {@inheritDoc} */
    @Override public <R1, R2, T> R2 apply(IgniteClosure<T, R1> job, Collection<? extends T> args,
        IgniteReducer<R1, R2> rdc) {
        try {
            return saveOrGet(applyAsync0(job, args, rdc));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T> IgniteFuture<R2> applyAsync(IgniteClosure<T, R1> job,
        Collection<? extends T> args, IgniteReducer<R1, R2> rdc) throws IgniteException {
        return createFuture(applyAsync0(job, args, rdc));
    }

    /**
     * Apply with reducer implementation.
     *
     * @param job Job
     * @param args Arguments.
     * @param rdc Reducer.
     * @return Internal future.
     */
    private <R1, R2, T> IgniteInternalFuture<R2> applyAsync0(IgniteClosure<T, R1> job, Collection<? extends T> args,
        IgniteReducer<R1, R2> rdc) {
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

    /** {@inheritDoc} */
    @Override public <R> Map<IgniteUuid, ComputeTaskFuture<R>> activeTaskFutures() {
        guard();

        try {
            return ctx.task().taskFutures();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute withName(String taskName) {
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

    /** {@inheritDoc} */
    @Override public IgniteCompute withTimeout(long timeout) {
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

    /** {@inheritDoc} */
    @Override public IgniteCompute withNoFailover() {
        guard();

        try {
            opts.get().withFailoverDisabled();
        }
        finally {
            unguard();
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute withNoResultCache() {
        guard();

        try {
            opts.get().withResultCacheDisabled();
        }
        finally {
            unguard();
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public void localDeployTask(Class<? extends ComputeTask> taskCls, ClassLoader clsLdr) {
        A.notNull(taskCls, "taskCls", clsLdr, "clsLdr");

        guard();

        try {
            GridDeployment dep = ctx.deploy().deploy(taskCls, clsLdr);

            if (dep == null)
                throw new IgniteDeploymentException("Failed to deploy task (was task (re|un)deployed?): " + taskCls);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, Class<? extends ComputeTask<?, ?>>> localTasks() {
        guard();

        try {
            return ctx.deploy().findAllTasks();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void undeployTask(String taskName) {
        A.notNull(taskName, "taskName");

        guard();

        try {
            ctx.deploy().undeployTask(taskName, prj.node(ctx.localNodeId()) != null,
                prj.forRemotes().nodes());
        }
        finally {
            unguard();
        }
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

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(prj);
        out.writeObject(execName);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        prj = (ClusterGroupAdapter)in.readObject();
        execName = (String)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        return execName == null ? prj.compute() : prj.compute().withExecutor(execName);
    }

    /** {@inheritDoc} */
    @Override protected <R> IgniteFuture<R> createFuture(IgniteInternalFuture<R> fut) {
        assert fut instanceof ComputeTaskInternalFuture : fut;

        return ((ComputeTaskInternalFuture<R>)fut).publicFuture();
    }

    /** {@inheritDoc} */
    @Override public <R> ComputeTaskFuture<R> future() {
        return (ComputeTaskFuture<R>)super.future();
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute withExecutor(@NotNull String name) {
        return new IgniteComputeImpl(ctx, prj, isAsync(), name);
    }
}
