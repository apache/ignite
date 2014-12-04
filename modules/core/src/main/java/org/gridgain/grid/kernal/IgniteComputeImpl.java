/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.GridClosureCallMode.*;
import static org.gridgain.grid.kernal.processors.task.GridTaskThreadContextKey.*;

/**
 * {@link org.apache.ignite.IgniteCompute} implementation.
 */
public class IgniteComputeImpl implements IgniteCompute, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridKernalContext ctx;

    /** */
    private ClusterGroupAdapter prj;

    /** */
    private UUID subjId;

    /** */
    private IgniteAsyncSupportAdapter asyncSup;

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
        this.ctx = ctx;
        this.prj = prj;
        this.subjId = subjId;

        asyncSup = new IgniteAsyncSupportAdapter(async);
    }

    /** {@inheritDoc} */
    @Override public <R> GridComputeTaskFuture<R> future() {
        return (GridComputeTaskFuture<R>)asyncSup.future();
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute enableAsync() {
        if (asyncSup.isAsync())
            return this;

        return new IgniteComputeImpl(ctx, prj, subjId, true);
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return asyncSup.isAsync();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup projection() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public void affinityRun(@Nullable String cacheName, Object affKey, Runnable job) throws GridException {
        A.notNull(affKey, "affKey");
        A.notNull(job, "job");

        guard();

        try {
            asyncSup.saveOrGet(ctx.closure().affinityRun(cacheName, affKey, job, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> R affinityCall(@Nullable String cacheName, Object affKey, Callable<R> job)
        throws GridException {
        A.notNull(affKey, "affKey");
        A.notNull(job, "job");

        guard();

        try {
            return asyncSup.saveOrGet(ctx.closure().affinityCall(cacheName, affKey, job, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(String taskName, @Nullable T arg) throws GridException {
        A.notNull(taskName, "taskName");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_SUBJ_ID, subjId);

            return (R)asyncSup.saveOrGet(ctx.task().execute(taskName, arg));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends GridComputeTask<T, R>> taskCls,
        @Nullable T arg) throws GridException {
        A.notNull(taskCls, "taskCls");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_SUBJ_ID, subjId);

            return asyncSup.saveOrGet(ctx.task().execute(taskCls, arg));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(GridComputeTask<T, R> task, @Nullable T arg) throws GridException {
        A.notNull(task, "task");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_SUBJ_ID, subjId);

            return asyncSup.saveOrGet(ctx.task().execute(task, arg));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void broadcast(Runnable job) throws GridException {
        A.notNull(job, "job");

        guard();

        try {
            asyncSup.saveOrGet(ctx.closure().runAsync(BROADCAST, job, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> broadcast(Callable<R> job) throws GridException {
        A.notNull(job, "job");

        guard();

        try {
            return asyncSup.saveOrGet(ctx.closure().callAsync(BROADCAST, Arrays.asList(job), prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> Collection<R> broadcast(IgniteClosure<T, R> job, @Nullable T arg) throws GridException {
        A.notNull(job, "job");

        guard();

        try {
            return asyncSup.saveOrGet(ctx.closure().broadcast(job, arg, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void run(Runnable job) throws GridException {
        A.notNull(job, "job");

        guard();

        try {
            asyncSup.saveOrGet(ctx.closure().runAsync(BALANCE, job, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void run(Collection<? extends Runnable> jobs) throws GridException {
        A.notEmpty(jobs, "jobs");

        guard();

        try {
            asyncSup.saveOrGet(ctx.closure().runAsync(BALANCE, jobs, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> R apply(IgniteClosure<T, R> job, @Nullable T arg) throws GridException {
        A.notNull(job, "job");

        guard();

        try {
            return asyncSup.saveOrGet(ctx.closure().callAsync(job, arg, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> R call(Callable<R> job) throws GridException {
        A.notNull(job, "job");

        guard();

        try {
            return asyncSup.saveOrGet(ctx.closure().callAsync(BALANCE, job, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> Collection<R> call(Collection<? extends Callable<R>> jobs) throws GridException {
        A.notEmpty(jobs, "jobs");

        guard();

        try {
            return asyncSup.saveOrGet(ctx.closure().callAsync(BALANCE, jobs, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> Collection<R> apply(final IgniteClosure<T, R> job,
        @Nullable Collection<? extends T> args) throws GridException {
        A.notNull(job, "job");
        A.notNull(args, "args");

        guard();

        try {
            return asyncSup.saveOrGet(ctx.closure().callAsync(job, args, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> R2 call(Collection<? extends Callable<R1>> jobs, IgniteReducer<R1, R2> rdc)
        throws GridException {
        A.notEmpty(jobs, "jobs");
        A.notNull(rdc, "rdc");

        guard();

        try {
            return asyncSup.saveOrGet(ctx.closure().forkjoinAsync(BALANCE, jobs, rdc, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T> R2 apply(IgniteClosure<T, R1> job, Collection<? extends T> args,
        IgniteReducer<R1, R2> rdc) throws GridException {
        A.notNull(job, "job");
        A.notNull(rdc, "rdc");
        A.notNull(args, "args");

        guard();

        try {
            return asyncSup.saveOrGet(ctx.closure().callAsync(job, args, rdc, prj.nodes()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> Map<IgniteUuid, GridComputeTaskFuture<R>> activeTaskFutures() {
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
    @Override public void localDeployTask(Class<? extends GridComputeTask> taskCls, ClassLoader clsLdr) throws GridException {
        A.notNull(taskCls, "taskCls", clsLdr, "clsLdr");

        guard();

        try {
            GridDeployment dep = ctx.deploy().deploy(taskCls, clsLdr);

            if (dep == null)
                throw new GridDeploymentException("Failed to deploy task (was task (re|un)deployed?): " + taskCls);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, Class<? extends GridComputeTask<?, ?>>> localTasks() {
        guard();

        try {
            return ctx.deploy().findAllTasks();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void undeployTask(String taskName) throws GridException {
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
}
