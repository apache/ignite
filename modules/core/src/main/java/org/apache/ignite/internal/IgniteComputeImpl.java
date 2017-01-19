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
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDeploymentException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
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
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_NO_FAILOVER;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SUBGRID;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SUBJ_ID;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_TASK_NAME;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_TIMEOUT;

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

    /** */
    private UUID subjId;

    /**
     * Required by {@link Externalizable}.
     */
    public IgniteComputeImpl() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     * @param prj Projection.
     * @param subjId Subject ID.
     */
    public IgniteComputeImpl(GridKernalContext ctx, ClusterGroupAdapter prj, UUID subjId) {
        this(ctx, prj, subjId, false);
    }

    /**
     * @param ctx Kernal context.
     * @param prj Projection.
     * @param subjId Subject ID.
     * @param async Async support flag.
     */
    private IgniteComputeImpl(GridKernalContext ctx, ClusterGroupAdapter prj, UUID subjId, boolean async) {
        super(async);

        this.ctx = ctx;
        this.prj = prj;
        this.subjId = subjId;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCompute createAsyncInstance() {
        return new IgniteComputeImpl(ctx, prj, subjId, true);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup clusterGroup() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(@Nullable String cacheName, Object affKey, IgniteRunnable job) {
        try {
            saveOrGet(affinityRun0(cacheName, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskFuture<Void> affinityRunAsync(@Nullable String cacheName, Object affKey,
        IgniteRunnable job) throws IgniteException {
        return (ComputeTaskFuture<Void>)createFuture(affinityRun0(cacheName, affKey, job));
    }

    /**
     * Affinity run implementation.
     *
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> affinityRun0(@Nullable String cacheName, Object affKey, IgniteRunnable job) {
        A.notNull(affKey, "affKey");
        A.notNull(job, "job");

        guard();

        try {
            // In case cache key is passed instead of affinity key.
            final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);
            int partId = ctx.affinity().partition(cacheName, affKey0);

            if (partId < 0)
                throw new IgniteCheckedException("Failed map key to partition: [cache=" + cacheName + " key="
                    + affKey + ']');

            return ctx.closure().affinityRun(Collections.singletonList(cacheName), partId, affKey,
                job, prj.nodes());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(@NotNull Collection<String> cacheNames, Object affKey, IgniteRunnable job) {
        try {
            saveOrGet(affinityRun0(cacheNames, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskFuture<Void> affinityRunAsync(@NotNull Collection<String> cacheNames, Object affKey,
        IgniteRunnable job) throws IgniteException {
        return (ComputeTaskFuture<Void>)createFuture(affinityRun0(cacheNames, affKey, job));
    }

    /**
     * Affinity run implementation.
     *
     * @param cacheNames Cache names collection.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> affinityRun0(@NotNull Collection<String> cacheNames, Object affKey,
        IgniteRunnable job) {
        A.notNull(affKey, "affKey");
        A.notNull(job, "job");
        A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");

        guard();

        try {
            final String cacheName = F.first(cacheNames);

            // In case cache key is passed instead of affinity key.
            final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);
            int partId = ctx.affinity().partition(cacheName, affKey0);

            if (partId < 0)
                throw new IgniteCheckedException("Failed map key to partition: [cache=" + cacheName + " key="
                    + affKey + ']');

            return ctx.closure().affinityRun(cacheNames, partId, affKey, job, prj.nodes());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(@NotNull Collection<String> cacheNames, int partId, IgniteRunnable job) {
        try {
            saveOrGet(affinityRun0(cacheNames, partId, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskFuture<Void> affinityRunAsync(@NotNull Collection<String> cacheNames, int partId,
        IgniteRunnable job) throws IgniteException {
        return (ComputeTaskFuture<Void>)createFuture(affinityRun0(cacheNames, partId, job));
    }

    /**
     * Affinity run implementation.
     *
     * @param cacheNames Cache names collection.
     * @param partId partition ID.
     * @param job Job.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> affinityRun0(@NotNull Collection<String> cacheNames, int partId,
        IgniteRunnable job) {
        A.ensure(partId >= 0, "partId = " + partId);
        A.notNull(job, "job");
        A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");

        guard();

        try {
            return ctx.closure().affinityRun(cacheNames, partId, null, job, prj.nodes());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(@Nullable String cacheName, Object affKey, IgniteCallable<R> job) {
        try {
            return saveOrGet(affinityCall0(cacheName, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> ComputeTaskFuture<R> affinityCallAsync(@Nullable String cacheName, Object affKey,
        IgniteCallable<R> job) throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(affinityCall0(cacheName, affKey, job));
    }

    /**
     * Affinity call implementation.

     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<R> affinityCall0(@Nullable String cacheName, Object affKey,
        IgniteCallable<R> job) {
        A.notNull(affKey, "affKey");
        A.notNull(job, "job");

        guard();

        try {
            // In case cache key is passed instead of affinity key.
            final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);
            int partId = ctx.affinity().partition(cacheName, affKey0);

            if (partId < 0)
                throw new IgniteCheckedException("Failed map key to partition: [cache=" + cacheName + " key="
                    + affKey + ']');

            return ctx.closure().affinityCall(Collections.singletonList(cacheName), partId, affKey, job,
                prj.nodes());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(@NotNull Collection<String> cacheNames, Object affKey, IgniteCallable<R> job) {
        try {
            return saveOrGet(affinityCall0(cacheNames, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> ComputeTaskFuture<R> affinityCallAsync(@NotNull Collection<String> cacheNames, Object affKey,
        IgniteCallable<R> job) throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(affinityCall0(cacheNames, affKey, job));
    }

    /**
     * Affinity call implementation.

     * @param cacheNames Cache names collection.
     * @param affKey Affinity key.
     * @param job Job.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<R> affinityCall0(@NotNull Collection<String> cacheNames, Object affKey,
        IgniteCallable<R> job) {
        A.notNull(affKey, "affKey");
        A.notNull(job, "job");
        A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");

        guard();

        try {
            final String cacheName = F.first(cacheNames);

            // In case cache key is passed instead of affinity key.
            final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);
            int partId = ctx.affinity().partition(cacheName, affKey0);

            if (partId < 0)
                throw new IgniteCheckedException("Failed map key to partition: [cache=" + cacheName + " key="
                    + affKey + ']');

            return ctx.closure().affinityCall(cacheNames, partId, affKey, job, prj.nodes());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(@NotNull Collection<String> cacheNames, int partId, IgniteCallable<R> job) {
        try {
            return saveOrGet(affinityCall0(cacheNames, partId, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> ComputeTaskFuture<R> affinityCallAsync(@NotNull Collection<String> cacheNames, int partId,
        IgniteCallable<R> job) throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(affinityCall0(cacheNames, partId, job));
    }

    /**
     * Affinity call implementation.

     * @param cacheNames Cache names collection.
     * @param partId Partition ID.
     * @param job Job.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<R> affinityCall0(@NotNull Collection<String> cacheNames, int partId,
        IgniteCallable<R> job) {
        A.ensure(partId >= 0, "partId = " + partId);
        A.notNull(job, "job");
        A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");

        guard();

        try {
            return ctx.closure().affinityCall(cacheNames, partId, null, job, prj.nodes());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T, R> R execute(String taskName, @Nullable T arg) {
        try {
            return (R)saveOrGet(execute0(taskName, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> ComputeTaskFuture<R> executeAsync(String taskName, @Nullable T arg) throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(execute0(taskName, arg));
    }

    /**
     * Execute implementation.
     *
     * @param taskName Task name.
     * @param arg Argument.
     * @return Internal future.
     */
    @SuppressWarnings("unchecked")
    private <T, R> IgniteInternalFuture<R> execute0(String taskName, @Nullable T arg) {
        A.notNull(taskName, "taskName");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_SUBJ_ID, subjId);

            return ctx.task().execute(taskName, arg);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends ComputeTask<T, R>> taskCls, @Nullable T arg) {
        try {
            return (R)saveOrGet(execute0(taskCls, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> ComputeTaskFuture<R> executeAsync(Class<? extends ComputeTask<T, R>> taskCls,
        @Nullable T arg) throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(execute0(taskCls, arg));
    }

    /**
     * Execute implementation.
     *
     * @param taskCls Task class.
     * @param arg Argument.
     * @return Internal future.
     */
    @SuppressWarnings("unchecked")
    private <T, R> IgniteInternalFuture<R> execute0(Class<? extends ComputeTask<T, R>> taskCls, @Nullable T arg) {
        A.notNull(taskCls, "taskCls");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_SUBJ_ID, subjId);

            return ctx.task().execute(taskCls, arg);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(ComputeTask<T, R> task, @Nullable T arg) {
        try {
            return (R)saveOrGet(executeAsyncInternal(task, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> ComputeTaskFuture<R> executeAsync(ComputeTask<T, R> task, @Nullable T arg)
        throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(executeAsyncInternal(task, arg));
    }

    /**
     * Execute implementation.
     *
     * @param task Task.
     * @param arg Task argument.
     * @return Task future.
     */
    public <T, R> ComputeTaskInternalFuture<R> executeAsyncInternal(ComputeTask<T, R> task, @Nullable T arg) {
        A.notNull(task, "task");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_SUBJ_ID, subjId);

            return ctx.task().execute(task, arg);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void broadcast(IgniteRunnable job) {
        try {
            saveOrGet(broadcast0(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskFuture<Void> broadcastAsync(IgniteRunnable job) throws IgniteException {
        return (ComputeTaskFuture<Void>)createFuture(broadcast0(job));
    }

    /**
     * Broadcast implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> broadcast0(IgniteRunnable job) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.closure().runAsync(BROADCAST, job, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> broadcast(IgniteCallable<R> job) {
        try {
            return saveOrGet(broadcast0(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> ComputeTaskFuture<Collection<R>> broadcastAsync(IgniteCallable<R> job) throws IgniteException {
        return (ComputeTaskFuture<Collection<R>>)createFuture(broadcast0(job));
    }

    /**
     * Broadcast implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<Collection<R>> broadcast0(IgniteCallable<R> job) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.closure().callAsync(BROADCAST, Collections.singletonList(job), prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> Collection<R> broadcast(IgniteClosure<T, R> job, @Nullable T arg) {
        try {
            return saveOrGet(broadcast0(job, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> ComputeTaskFuture<Collection<R>> broadcastAsync(IgniteClosure<T, R> job,
        @Nullable T arg) throws IgniteException {
        return (ComputeTaskFuture<Collection<R>>)createFuture(broadcast0(job, arg));
    }

    /**
     * Broadcast implementation.
     *
     * @param job Job.
     * @param arg Argument.
     * @return Internal future.
     */
    private <R, T> IgniteInternalFuture<Collection<R>> broadcast0(IgniteClosure<T, R> job, @Nullable T arg) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.closure().broadcast(job, arg, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void run(IgniteRunnable job) {
        try {
            saveOrGet(run0(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskFuture<Void> runAsync(IgniteRunnable job) throws IgniteException {
        return (ComputeTaskFuture<Void>)createFuture(run0(job));
    }

    /**
     * Run implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> run0(IgniteRunnable job) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.closure().runAsync(BALANCE, job, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void run(Collection<? extends IgniteRunnable> jobs) {
        try {
            saveOrGet(run0(jobs));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskFuture<Void> runAsync(Collection<? extends IgniteRunnable> jobs)
        throws IgniteException {
        return (ComputeTaskFuture<Void>)createFuture(run0(jobs));
    }

    /**
     * Run implementation.
     *
     * @param jobs Jobs.
     * @return Internal future.
     */
    private IgniteInternalFuture<?> run0(Collection<? extends IgniteRunnable> jobs) {
        A.notEmpty(jobs, "jobs");

        guard();

        try {
            return ctx.closure().runAsync(BALANCE, jobs, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> R apply(IgniteClosure<T, R> job, @Nullable T arg) {
        try {
            return saveOrGet(apply0(job, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> ComputeTaskFuture<R> applyAsync(IgniteClosure<T, R> job, @Nullable T arg)
        throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(apply0(job, arg));
    }

    /**
     * Apply implementation.
     *
     * @param job Job.
     * @param arg Argument.
     * @return Internal future.
     */
    private <R, T> IgniteInternalFuture<R> apply0(IgniteClosure<T, R> job, @Nullable T arg) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.closure().callAsync(job, arg, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> R call(IgniteCallable<R> job) {
        try {
            return saveOrGet(call0(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> ComputeTaskFuture<R> callAsync(IgniteCallable<R> job) throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(call0(job));
    }

    /**
     * Call implementation.
     *
     * @param job Job.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<R> call0(IgniteCallable<R> job) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.closure().callAsync(BALANCE, job, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> call(Collection<? extends IgniteCallable<R>> jobs) {
        try {
            return saveOrGet(call0(jobs));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> ComputeTaskFuture<Collection<R>> callAsync(
        Collection<? extends IgniteCallable<R>> jobs) throws IgniteException {
        return (ComputeTaskFuture<Collection<R>>)createFuture(call0(jobs));
    }

    /**
     * Call implementation.
     *
     * @param jobs Jobs.
     * @return Internal future.
     */
    private <R> IgniteInternalFuture<Collection<R>> call0(Collection<? extends IgniteCallable<R>> jobs) {
        A.notEmpty(jobs, "jobs");

        guard();

        try {
            return ctx.closure().callAsync(BALANCE, (Collection<? extends Callable<R>>)jobs, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> Collection<R> apply(final IgniteClosure<T, R> job, @Nullable Collection<? extends T> args) {
        try {
            return saveOrGet(apply0(job, args));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> ComputeTaskFuture<Collection<R>> applyAsync(IgniteClosure<T, R> job,
        Collection<? extends T> args) throws IgniteException {
        return (ComputeTaskFuture<Collection<R>>)createFuture(apply0(job, args));
    }

    /**
     * Apply implementation.
     *
     * @param job Job.
     * @param args Arguments/
     * @return Internal future.
     */
    private <T, R> IgniteInternalFuture<Collection<R>> apply0(final IgniteClosure<T, R> job,
        @Nullable Collection<? extends T> args) {
        A.notNull(job, "job");
        A.notNull(args, "args");

        guard();

        try {
            return ctx.closure().callAsync(job, args, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 call(Collection<? extends IgniteCallable<R1>> jobs, IgniteReducer<R1, R2> rdc) {
        try {
            return saveOrGet(call0(jobs, rdc));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> ComputeTaskFuture<R2> callAsync(Collection<? extends IgniteCallable<R1>> jobs,
        IgniteReducer<R1, R2> rdc) throws IgniteException {
        return (ComputeTaskFuture<R2>)createFuture(call0(jobs, rdc));
    }

    /**
     * Call with reducer implementation.
     *
     * @param jobs Jobs.
     * @param rdc Reducer.
     * @return Internal future.
     */
    private <R1, R2> IgniteInternalFuture<R2> call0(Collection<? extends IgniteCallable<R1>> jobs,
        IgniteReducer<R1, R2> rdc) {
        A.notEmpty(jobs, "jobs");
        A.notNull(rdc, "rdc");

        guard();

        try {
            return ctx.closure().forkjoinAsync(BALANCE, jobs, rdc, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T> R2 apply(IgniteClosure<T, R1> job, Collection<? extends T> args,
        IgniteReducer<R1, R2> rdc) {
        try {
            return saveOrGet(apply0(job, args, rdc));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T> ComputeTaskFuture<R2> applyAsync(IgniteClosure<T, R1> job,
        Collection<? extends T> args, IgniteReducer<R1, R2> rdc) throws IgniteException {
        return (ComputeTaskFuture<R2>)createFuture(apply0(job, args, rdc));
    }

    /**
     * Apply with reducer implementation.
     *
     * @param job Job
     * @param args Arguments.
     * @param rdc Reducer.
     * @return Internal future.
     */
    private <R1, R2, T> IgniteInternalFuture<R2> apply0(IgniteClosure<T, R1> job, Collection<? extends T> args,
        IgniteReducer<R1, R2> rdc) {
        A.notNull(job, "job");
        A.notNull(rdc, "rdc");
        A.notNull(args, "args");

        guard();

        try {
            return ctx.closure().callAsync(job, args, rdc, prj.nodes());
        }
        finally {
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
            ctx.task().setThreadContext(TC_TASK_NAME, taskName);
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
            ctx.task().setThreadContext(TC_TIMEOUT, timeout);
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
            ctx.task().setThreadContext(TC_NO_FAILOVER, true);
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
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        prj = (ClusterGroupAdapter)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        return prj.compute();
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
}