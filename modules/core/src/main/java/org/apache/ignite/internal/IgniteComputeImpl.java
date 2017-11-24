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
     * @param async Async support flag.
     */
    public IgniteComputeImpl(GridKernalContext ctx, ClusterGroupAdapter prj, UUID subjId, boolean async) {
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

            saveOrGet(ctx.closure().affinityRun(Collections.singletonList(cacheName), partId, affKey,
                job, prj.nodes()));
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

            saveOrGet(ctx.closure().affinityRun(cacheNames, partId, affKey, job, prj.nodes()));
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
        A.ensure(partId >= 0, "partId = " + partId);
        A.notNull(job, "job");
        A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");

        guard();

        try {
            saveOrGet(ctx.closure().affinityRun(cacheNames, partId, null, job, prj.nodes()));
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

            return saveOrGet(ctx.closure().affinityCall(Collections.singletonList(cacheName), partId, affKey, job,
                prj.nodes()));
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

            return saveOrGet(ctx.closure().affinityCall(cacheNames, partId, affKey, job, prj.nodes()));
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
        A.ensure(partId >= 0, "partId = " + partId);
        A.notNull(job, "job");
        A.ensure(!cacheNames.isEmpty(), "cachesNames mustn't be empty");

        guard();

        try {
            return saveOrGet(ctx.closure().affinityCall(cacheNames, partId, null, job, prj.nodes()));
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
        A.notNull(taskName, "taskName");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_SUBJ_ID, subjId);

            return (R)saveOrGet(ctx.task().execute(taskName, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends ComputeTask<T, R>> taskCls, @Nullable T arg) {
        A.notNull(taskCls, "taskCls");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_SUBJ_ID, subjId);

            return saveOrGet(ctx.task().execute(taskCls, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(ComputeTask<T, R> task, @Nullable T arg) {
        A.notNull(task, "task");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_SUBJ_ID, subjId);

            return saveOrGet(ctx.task().execute(task, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /**
     * @param task Task.
     * @param arg Task argument.
     * @return Task future.
     */
    public <T, R> ComputeTaskInternalFuture<R> executeAsync(ComputeTask<T, R> task, @Nullable T arg) {
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

    /**
     * @param taskName Task name.
     * @param arg Task argument.
     * @return Task future.
     */
    public <T, R> ComputeTaskInternalFuture<R> executeAsync(String taskName, @Nullable T arg) {
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
    @Override public void broadcast(IgniteRunnable job) {
        A.notNull(job, "job");

        guard();

        try {
            saveOrGet(ctx.closure().runAsync(BROADCAST, job, prj.nodes()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> broadcast(IgniteCallable<R> job) {
        A.notNull(job, "job");

        guard();

        try {
            return saveOrGet(ctx.closure().callAsync(BROADCAST, Collections.singletonList(job), prj.nodes()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> Collection<R> broadcast(IgniteClosure<T, R> job, @Nullable T arg) {
        A.notNull(job, "job");

        guard();

        try {
            return saveOrGet(ctx.closure().broadcast(job, arg, prj.nodes()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void run(IgniteRunnable job) {
        A.notNull(job, "job");

        guard();

        try {
            saveOrGet(ctx.closure().runAsync(BALANCE, job, prj.nodes()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void run(Collection<? extends IgniteRunnable> jobs) {
        A.notEmpty(jobs, "jobs");

        guard();

        try {
            saveOrGet(ctx.closure().runAsync(BALANCE, jobs, prj.nodes()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> R apply(IgniteClosure<T, R> job, @Nullable T arg) {
        A.notNull(job, "job");

        guard();

        try {
            return saveOrGet(ctx.closure().callAsync(job, arg, prj.nodes()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> R call(IgniteCallable<R> job) {
        A.notNull(job, "job");

        guard();

        try {
            return saveOrGet(ctx.closure().callAsync(BALANCE, job, prj.nodes()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> call(Collection<? extends IgniteCallable<R>> jobs) {
        A.notEmpty(jobs, "jobs");

        guard();

        try {
            return saveOrGet(ctx.closure().callAsync(BALANCE, (Collection<? extends Callable<R>>)jobs, prj.nodes()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> Collection<R> apply(final IgniteClosure<T, R> job, @Nullable Collection<? extends T> args) {
        A.notNull(job, "job");
        A.notNull(args, "args");

        guard();

        try {
            return saveOrGet(ctx.closure().callAsync(job, args, prj.nodes()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 call(Collection<? extends IgniteCallable<R1>> jobs, IgniteReducer<R1, R2> rdc) {
        A.notEmpty(jobs, "jobs");
        A.notNull(rdc, "rdc");

        guard();

        try {
            return saveOrGet(ctx.closure().forkjoinAsync(BALANCE, jobs, rdc, prj.nodes()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T> R2 apply(IgniteClosure<T, R1> job, Collection<? extends T> args,
        IgniteReducer<R1, R2> rdc) {
        A.notNull(job, "job");
        A.notNull(rdc, "rdc");
        A.notNull(args, "args");

        guard();

        try {
            return saveOrGet(ctx.closure().callAsync(job, args, rdc, prj.nodes()));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
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
            ctx.deploy().undeployTask(taskName, prj.node(ctx.localNodeId()) != null, prj.forRemotes().nodes());
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