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

    /** */
    private IgniteComputeHandler compute;

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
        this(ctx, prj, false, null, null);
    }

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param prj Projection.
     * @param async Async support flag.
     * @param execName Custom executor name.
     * @param compute Optional compute handler from which the initial task execution options will be copied.
     */
    private IgniteComputeImpl(
        GridKernalContext ctx,
        ClusterGroupAdapter prj,
        boolean async,
        String execName,
        @Nullable IgniteComputeHandler compute
    ) {
        super(async);

        this.ctx = ctx;
        this.prj = prj;
        this.execName = execName;

        this.compute = compute == null
            ? new IgniteComputeHandler(ctx, this::enrichOptions)
            : new IgniteComputeHandler(compute, this::enrichOptions);
    }

    /** {@inheritDoc} */
    @Override protected IgniteCompute createAsyncInstance() {
        return new IgniteComputeImpl(ctx, prj, true, execName, compute);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup clusterGroup() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(String cacheName, Object affKey, IgniteRunnable job) {
        try {
            saveOrGet(compute.affinityRunAsync(cacheName, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> affinityRunAsync(String cacheName, Object affKey,
        IgniteRunnable job) throws IgniteException {

        return (IgniteFuture<Void>)createFuture(compute.affinityRunAsync(cacheName, affKey, job));
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(@NotNull Collection<String> cacheNames, Object affKey, IgniteRunnable job) {
        try {
            saveOrGet(compute.affinityRunAsync(cacheNames, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> affinityRunAsync(@NotNull Collection<String> cacheNames, Object affKey,
        IgniteRunnable job) throws IgniteException {
        return (IgniteFuture<Void>)createFuture(compute.affinityRunAsync(cacheNames, affKey, job));
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(@NotNull Collection<String> cacheNames, int partId, IgniteRunnable job) {
        try {
            saveOrGet(compute.affinityRunAsync(cacheNames, partId, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> affinityRunAsync(@NotNull Collection<String> cacheNames, int partId,
        IgniteRunnable job) throws IgniteException {
        return (IgniteFuture<Void>)createFuture(compute.affinityRunAsync(cacheNames, partId, job));
    }

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(String cacheName, Object affKey, IgniteCallable<R> job) {
        try {
            return saveOrGet(compute.affinityCallAsync(cacheName, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> affinityCallAsync(String cacheName, Object affKey,
        IgniteCallable<R> job) throws IgniteException {
        return createFuture(compute.affinityCallAsync(cacheName, affKey, job));
    }

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(@NotNull Collection<String> cacheNames, Object affKey, IgniteCallable<R> job) {
        try {
            return saveOrGet(compute.affinityCallAsync(cacheNames, affKey, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> affinityCallAsync(@NotNull Collection<String> cacheNames, Object affKey,
        IgniteCallable<R> job) throws IgniteException {
        return createFuture(compute.affinityCallAsync(cacheNames, affKey, job));
    }

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(@NotNull Collection<String> cacheNames, int partId, IgniteCallable<R> job) {
        try {
            return saveOrGet(compute.affinityCallAsync(cacheNames, partId, job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> affinityCallAsync(@NotNull Collection<String> cacheNames, int partId,
        IgniteCallable<R> job) throws IgniteException {
        return createFuture(compute.affinityCallAsync(cacheNames, partId, job));
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(String taskName, @Nullable T arg) {
        try {
            return saveOrGet(compute.executeAsync(taskName, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> ComputeTaskFuture<R> executeAsync(String taskName, @Nullable T arg) throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(compute.executeAsync(taskName, arg));
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends ComputeTask<T, R>> taskCls, @Nullable T arg) {
        try {
            return saveOrGet(compute.withProjectionPredicate(prj.predicate()).executeAsync(taskCls, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> ComputeTaskFuture<R> executeAsync(Class<? extends ComputeTask<T, R>> taskCls,
        @Nullable T arg) throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(compute.withProjectionPredicate(prj.predicate()).executeAsync(taskCls, arg));
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(ComputeTask<T, R> task, @Nullable T arg) {
        try {
            return saveOrGet(compute.withProjectionPredicate(prj.predicate()).executeAsync(task, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> ComputeTaskFuture<R> executeAsync(ComputeTask<T, R> task, @Nullable T arg)
        throws IgniteException {
        return (ComputeTaskFuture<R>)createFuture(compute.withProjectionPredicate(prj.predicate()).executeAsync(task, arg));
    }

    /** {@inheritDoc} */
    @Override public void broadcast(IgniteRunnable job) {
        try {
            saveOrGet(compute.broadcastAsync(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> broadcastAsync(IgniteRunnable job) throws IgniteException {
        return (IgniteFuture<Void>)createFuture(compute.broadcastAsync(job));
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> broadcast(IgniteCallable<R> job) {
        try {
            return saveOrGet(compute.broadcastAsync(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<Collection<R>> broadcastAsync(IgniteCallable<R> job) throws IgniteException {
        return createFuture(compute.broadcastAsync(job));
    }

    /** {@inheritDoc} */
    @Override public <R, T> Collection<R> broadcast(IgniteClosure<T, R> job, @Nullable T arg) {
        try {
            return saveOrGet(compute.broadcastAsync(job, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> IgniteFuture<Collection<R>> broadcastAsync(IgniteClosure<T, R> job,
        @Nullable T arg) throws IgniteException {
        return createFuture(compute.broadcastAsync(job, arg));
    }

    /** {@inheritDoc} */
    @Override public void run(IgniteRunnable job) {
        try {
            saveOrGet(compute.runAsync(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> runAsync(IgniteRunnable job) throws IgniteException {
        return (IgniteFuture<Void>)createFuture(compute.runAsync(job));
    }

    /** {@inheritDoc} */
    @Override public void run(Collection<? extends IgniteRunnable> jobs) {
        try {
            saveOrGet(compute.runAsync(jobs));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> runAsync(Collection<? extends IgniteRunnable> jobs)
        throws IgniteException {
        return (IgniteFuture<Void>)createFuture(compute.runAsync(jobs));
    }

    /** {@inheritDoc} */
    @Override public <R, T> R apply(IgniteClosure<T, R> job, @Nullable T arg) {
        try {
            return saveOrGet(compute.applyAsync(job, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> IgniteFuture<R> applyAsync(IgniteClosure<T, R> job, @Nullable T arg)
        throws IgniteException {
        return createFuture(compute.applyAsync(job, arg));
    }

    /** {@inheritDoc} */
    @Override public <R> R call(IgniteCallable<R> job) {
        try {
            return saveOrGet(compute.callAsync(job));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> callAsync(IgniteCallable<R> job) throws IgniteException {
        return createFuture(compute.callAsync(job));
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> call(Collection<? extends IgniteCallable<R>> jobs) {
        try {
            return saveOrGet(compute.callAsync(jobs));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<Collection<R>> callAsync(
        Collection<? extends IgniteCallable<R>> jobs) throws IgniteException {
        return createFuture(compute.callAsync(jobs));
    }

    /** {@inheritDoc} */
    @Override public <T, R> Collection<R> apply(final IgniteClosure<T, R> job, @Nullable Collection<? extends T> args) {
        try {
            return saveOrGet(compute.applyAsync(job, args));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> IgniteFuture<Collection<R>> applyAsync(IgniteClosure<T, R> job,
        Collection<? extends T> args) throws IgniteException {
        return createFuture(compute.applyAsync(job, args));
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 call(Collection<? extends IgniteCallable<R1>> jobs, IgniteReducer<R1, R2> rdc) {
        try {
            return saveOrGet(compute.callAsync(jobs, rdc));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> IgniteFuture<R2> callAsync(Collection<? extends IgniteCallable<R1>> jobs,
        IgniteReducer<R1, R2> rdc) throws IgniteException {
        return createFuture(compute.callAsync(jobs, rdc));
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T> R2 apply(IgniteClosure<T, R1> job, Collection<? extends T> args,
        IgniteReducer<R1, R2> rdc) {
        try {
            return saveOrGet(compute.applyAsync(job, args, rdc));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T> IgniteFuture<R2> applyAsync(IgniteClosure<T, R1> job,
        Collection<? extends T> args, IgniteReducer<R1, R2> rdc) throws IgniteException {
        return createFuture(compute.applyAsync(job, args, rdc));
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
        compute.withName(taskName);

        return this;
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute withTimeout(long timeout) {
        compute.withTimeout(timeout);

        return this;
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute withNoFailover() {
        compute.withNoFailover();

        return this;
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute withNoResultCache() {
        compute.withNoResultCache();

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
        return new IgniteComputeImpl(ctx, prj, isAsync(), name, compute);
    }

    /** Enriches specified task execution options with those that are bounded to the current compute instance. */
    private TaskExecutionOptions enrichOptions(TaskExecutionOptions opts) {
        if (execName != null)
            opts.withExecutor(execName);

        return opts.asPublicRequest().withProjection(prj.nodes());
    }
}
