// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.executor.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.kernal.GridClosureCallMode.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;
import static org.gridgain.grid.kernal.processors.task.GridTaskThreadContextKey.*;

/**
 * {@link GridCompute} implementation.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridComputeImpl implements GridCompute {
    /** */
    private GridKernalContext ctx;

    /** */
    private GridProjection prj;

    /**
     * @param ctx Kernal context.
     * @param prj Projection.
     */
    public GridComputeImpl(GridKernalContext ctx, GridProjection prj) {
        this.ctx = ctx;
        this.prj = prj;
    }

    /** {@inheritDoc} */
    @Override public GridProjection projection() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> affinityRun(@Nullable String cacheName, Object affKey, Runnable job) {
        A.notNull(affKey, "affKey");
        A.notNull(job, "job");

        guard();

        try {
            return ctx.closure().runAsync(BALANCE, wrapRun(cacheName, affKey, job), prj.nodes());
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx, e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> GridFuture<R> affinityCall(@Nullable String cacheName, Object affKey, Callable<R> job) {
        A.notNull(affKey, "affKey");
        A.notNull(job, "job");

        guard();

        try {
            return ctx.closure().callAsync(BALANCE, wrapCall(cacheName, affKey, job), prj.nodes());
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx, e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> GridComputeTaskFuture<R> execute(String taskName, @Nullable T arg) {
        A.notNull(taskName, "taskName");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());

            return ctx.task().execute(taskName, arg);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> GridComputeTaskFuture<R> execute(Class<? extends GridComputeTask<T, R>> taskCls,
        @Nullable T arg) {
        A.notNull(taskCls, "taskCls");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());

            return ctx.task().execute(taskCls, arg);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> GridComputeTaskFuture<R> execute(GridComputeTask<T, R> task, @Nullable T arg) {
        A.notNull(task, "task");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());

            return ctx.task().execute(task, arg);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> broadcast(Runnable job) {
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
    @Override public <R> GridFuture<Collection<R>> broadcast(Callable<R> job) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.closure().callAsync(BROADCAST, Arrays.asList(job), prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> GridFuture<Collection<R>> broadcast(GridClosure<T, R> job, @Nullable T arg) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.closure().callAsync(BALANCE, Arrays.asList(F.curry(job, arg)), prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> run(Runnable job) {
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
    @Override public GridFuture<?> run(Collection<? extends Runnable> jobs) {
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
    @Override public <R, T> GridFuture<R> apply(GridClosure<T, R> job, @Nullable T arg) {
        A.notNull(job, "job");

        guard();

        try {
            return ctx.closure().callAsync(BALANCE, F.curry(job, arg), prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> GridFuture<R> call(Callable<R> job) {
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
    @Override public <R> GridFuture<Collection<R>> call(Collection<? extends Callable<R>> jobs) {
        A.notEmpty(jobs, "jobs");

        guard();

        try {
            return ctx.closure().callAsync(BALANCE, jobs, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        guard();

        try {
            return new GridExecutorService(prj, ctx.log());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> GridFuture<Collection<R>> apply(final GridClosure<T, R> job,
        @Nullable Collection<? extends T> args) {
        A.notNull(job, "job");

        guard();

        try {
            return job == null ? new GridFinishedFuture<Collection<R>>(ctx) :
                ctx.closure().callAsync(BALANCE,
                    F.transform(args, new C1<T, Callable<R>>() {
                        @Override public Callable<R> apply(T arg) {
                            return F.curry(job, arg);
                        }
                    }),
                    prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridFuture<R2> call(Collection<? extends Callable<R1>> jobs, GridReducer<R1, R2> rdc) {
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
    @Override public <R1, R2, T> GridFuture<R2> apply(final GridClosure<T, R1> job,
        @Nullable Collection<? extends T> args, GridReducer<R1, R2> rdc) {
        A.notNull(job, "job");
        A.notNull(rdc, "rdc");

        guard();

        try {
            return job == null ? new GridFinishedFuture<R2>(ctx) :
                ctx.closure().forkjoinAsync(BALANCE,
                    F.transform(args, new C1<T, Callable<R1>>() {
                        @Override public Callable<R1> apply(T arg) {
                            return F.curry(job, arg);
                        }
                    }),
                    rdc, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public <R> GridComputeTaskFuture<R> taskFuture(GridUuid sesId) {
        A.notNull(sesId, "sesId");

        guard();

        try {
            return ctx.task().taskFuture(sesId);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void cancelTask(GridUuid sesId) throws GridException {
        A.notNull(sesId, "sesId");

        guard();

        try {
            GridComputeTaskFuture<Object> task = ctx.task().taskFuture(sesId);

            if (task != null) {
                boolean loc = F.nodeIds(prj.nodes()).contains(ctx.localNodeId());

                if (loc)
                    task.cancel();
            }
            else
                ctx.io().send(
                    prj.forRemotes().nodes(),
                    TOPIC_TASK_CANCEL,
                    new GridTaskCancelRequest(sesId),
                    SYSTEM_POOL
                );
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void cancelJob(GridUuid jobId) throws GridException {
        A.notNull(jobId, "jobId");

        guard();

        try {
            boolean loc = F.nodeIds(prj.nodes()).contains(ctx.localNodeId());

            if (loc)
                ctx.job().cancelJob(null, jobId, false);

            ctx.io().send(
                prj.forRemotes().nodes(),
                TOPIC_JOB_CANCEL,
                new GridJobCancelRequest(null, jobId, false),
                SYSTEM_POOL
            );
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCompute withName(String taskName) {
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
    @Override public GridCompute withTimeout(long timeout) {
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
    @Override public GridCompute withNoFailover() {
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
     *
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param run Run to wrap.
     * @return Wrapped call.
     * @throws GridException In case of error.
     */
    @SuppressWarnings("UnusedDeclaration")
    private Runnable wrapRun(@Nullable final String cacheName, Object affKey, final Runnable run) throws GridException {
        // In case cache key is passed instead of affinity key.
        final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);

        return new GridRunnable() {
            @GridCacheName
            private final String cn = cacheName;

            @GridCacheAffinityKeyMapped
            private final Object ak = affKey0;

            @Override public void run() {
                run.run();
            }
        };
    }

    /**
     *
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param call Call to wrap.
     * @param <R> Type of the {@code call} return value.
     * @return Wrapped call.
     * @throws GridException In case of error.
     */
    @SuppressWarnings("UnusedDeclaration")
    private <R> Callable<R> wrapCall(@Nullable final String cacheName, Object affKey, final Callable<R> call)
        throws GridException {
        // In case cache key is passed instead of affinity key.
        final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);

        return new GridCallable<R>() {
            @GridCacheName
            private final String cn = cacheName;

            @GridCacheAffinityKeyMapped
            private final Object ak = affKey0;

            @Override public R call() {
                try {
                    return call.call();
                }
                catch (Exception e) {
                    throw F.wrap(e);
                }
            }
        };
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
