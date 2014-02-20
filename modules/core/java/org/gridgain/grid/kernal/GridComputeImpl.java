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
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.GridClosureCallMode.*;
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
        guard();

        try {
            return (affKey == null || job == null) ?
                new GridFinishedFuture(ctx) :
                ctx.closure().runAsync(BALANCE, wrapRun(cacheName, affKey, job), prj.nodes());
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx, e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> affinityRun(@Nullable final String cacheName, Collection<?> affKeys,
        final GridOutClosure<Runnable> jobFactory) {
        guard();

        try {
            return (affKeys == null || affKeys.isEmpty() || jobFactory == null) ?
                new GridFinishedFuture(ctx) :
                ctx.closure().runAsync(
                    BALANCE,
                    F.transform(
                        affKeys,
                        new CX1<Object, CA>() {
                            @Override
                            public CA applyx(Object affKey) throws GridException {
                                return wrapRun(cacheName, affKey, jobFactory.apply());
                            }
                        }
                    ),
                    prj.nodes()
                );
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> GridFuture<R> affinityCall(@Nullable String cacheName, Object affKey, Callable<R> job) {
        guard();

        try {
            return (affKey == null || job == null) ?
                new GridFinishedFuture<R>(ctx) :
                ctx.closure().callAsync(BALANCE, wrapCall(cacheName, affKey, job), prj.nodes());
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx, e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> GridFuture<Collection<R>> affinityCall(@Nullable final String cacheName, Collection<?> affKeys,
        final GridOutClosure<Callable<R>> jobFactory) {
        guard();

        try {
            return (affKeys == null || affKeys.isEmpty() || jobFactory == null) ?
                new GridFinishedFuture<Collection<R>>(ctx) :
                ctx.closure().callAsync(
                    BALANCE,
                    F.transform(
                        affKeys,
                        new CX1<Object, CO<R>>() {
                            @Override public CO<R> applyx(Object affKey) throws GridException {
                                return wrapCall(cacheName, affKey, jobFactory.apply());
                            }
                        }
                    ),
                    prj.nodes()
                );
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> GridComputeTaskFuture<R> execute(String taskName, @Nullable T arg, long timeout) {
        A.notNull(taskName, "taskName");
        A.ensure(timeout >= 0, "timeout >= 0");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_PREDICATE, prj.predicate());

            return ctx.task().execute(taskName, arg, timeout);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> GridComputeTaskFuture<R> execute(Class<? extends GridComputeTask<T, R>> taskCls, @Nullable T arg,
        long timeout) {
        A.notNull(taskCls, "taskCls");
        A.ensure(timeout >= 0, "timeout >= 0");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_PREDICATE, prj.predicate());

            return ctx.task().execute(taskCls, arg, timeout);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> GridComputeTaskFuture<R> execute(GridComputeTask<T, R> task, @Nullable T arg, long timeout) {
        A.notNull(task, "task");
        A.ensure(timeout >= 0, "timeout >= 0");

        guard();

        try {
            ctx.task().setThreadContextIfNotNull(TC_SUBGRID, prj.nodes());
            ctx.task().setThreadContextIfNotNull(TC_PREDICATE, prj.predicate());


            return ctx.task().execute(task, arg, timeout);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> run(GridClosureCallMode mode, Runnable job) {
        A.notNull(mode, "mode");

        guard();

        try {
            return ctx.closure().runAsync(mode, job, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T> GridFuture<?> run(GridClosureCallMode mode, GridInClosure<? super T> job, @Nullable T arg) {
        A.notNull(mode, "mode");

        guard();

        try {
            return job == null ? new GridFinishedFuture<T>(ctx) : ctx.closure().runAsync(mode, F.curry(job, arg),
                prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> run(GridClosureCallMode mode, Collection<? extends Runnable> jobs) {
        A.notNull(mode, "mode");

        guard();

        try {
            return ctx.closure().runAsync(mode, jobs, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R, T> GridFuture<R> call(GridClosureCallMode mode, GridClosure<? super T, R> job,
        @Nullable T arg) {
        A.notNull(mode, "mode", job, "job");

        guard();

        try {
            return job == null ? new GridFinishedFuture<R>(ctx) : ctx.closure().callAsync(mode, F.curry(job, arg),
                prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> GridFuture<R> call(GridClosureCallMode mode, @Nullable Callable<R> job) {
        A.notNull(mode, "mode");

        guard();

        try {
            return ctx.closure().callAsync(mode, job, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R> GridFuture<Collection<R>> call(GridClosureCallMode mode,
        Collection<? extends Callable<R>> jobs) {
        A.notNull(mode, "mode");

        guard();

        try {
            return ctx.closure().callAsync(mode, jobs, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executor() {
        guard();

        try {
            return new GridExecutorService(prj, ctx.log());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> GridFuture<Collection<R>> call(GridClosureCallMode mode,
        Collection<? extends GridClosure<? super T, R>> jobs, @Nullable Collection<? extends T> args) {
        A.notNull(mode, "mode");

        guard();

        try {
            return ctx.closure().callAsync(mode, F.curry(jobs, args), prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> GridFuture<Collection<R>> call(GridClosureCallMode mode,
        final GridOutClosure<GridClosure<? super T, R>> jobFactory, @Nullable Collection<? extends T> args) {
        A.notNull(mode, "mode");

        guard();

        try {
            return jobFactory == null ? new GridFinishedFuture<Collection<R>>(ctx) :
                ctx.closure().callAsync(mode,
                    F.transform(args, new C1<T, Callable<R>>() {
                        @Override public Callable<R> apply(T arg) {
                            return F.curry(jobFactory.apply(), arg);
                        }
                    }),
                    prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T extends Callable<R1>> GridFuture<R2> mapreduce(
        @Nullable GridMapper<T, GridNode> mapper, @Nullable Collection<T> jobs, @Nullable GridReducer<R1, R2> rdc) {
        guard();

        try {
            return ctx.closure().forkjoinAsync(mapper, jobs, rdc, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T> GridFuture<R2> mapreduce(GridMapper<GridOutClosure<R1>, GridNode> mapper,
        Collection<? extends GridClosure<? super T, R1>> jobs, @Nullable Collection<? extends T> args,
        GridReducer<R1, R2> rdc) {
        guard();

        try {
            return ctx.closure().forkjoinAsync(mapper, F.curry(jobs, args), rdc, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T> GridFuture<R2> mapreduce(GridMapper<GridOutClosure<R1>, GridNode> mapper,
        final GridOutClosure<GridClosure<? super T, R1>> jobFactory, @Nullable Collection<? extends T> args,
        GridReducer<R1, R2> rdc) {
        guard();

        try {
            return jobFactory == null ? new GridFinishedFuture<R2>(ctx) :
                ctx.closure().forkjoinAsync(mapper,
                    F.transform(args, new C1<T, GridOutClosure<R1>>() {
                        @Override public GridOutClosure<R1> apply(T arg) {
                            return F.curry(jobFactory.apply(), arg);
                        }
                    }),
                    rdc, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridFuture<R2> reduce(GridClosureCallMode mode,
        @Nullable Collection<? extends Callable<R1>> jobs, @Nullable GridReducer<R1, R2> rdc) {
        A.notNull(mode, "mode");

        guard();

        try {
            return ctx.closure().forkjoinAsync(mode, jobs, rdc, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T> GridFuture<R2> reduce(GridClosureCallMode mode,
        @Nullable Collection<? extends GridClosure<? super T, R1>> jobs, @Nullable Collection<? extends T> args,
        @Nullable GridReducer<R1, R2> rdc) {
        A.notNull(mode, "mode");

        guard();

        try {
            return ctx.closure().forkjoinAsync(mode, F.curry(jobs, args), rdc, prj.nodes());
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2, T> GridFuture<R2> reduce(GridClosureCallMode mode,
        @Nullable final GridOutClosure<GridClosure<? super T, R1>> jobFactory,
        @Nullable Collection<? extends T> args, @Nullable GridReducer<R1, R2> rdc) {
        A.notNull(mode, "mode");

        guard();

        try {
            return jobFactory == null ? new GridFinishedFuture<R2>(ctx) :
                ctx.closure().forkjoinAsync(mode,
                    F.transform(args, new C1<T, Callable<R1>>() {
                        @Override public Callable<R1> apply(T arg) {
                            return F.curry(jobFactory.apply(), arg);
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
    @Override public void cancelTask(@Nullable GridUuid sesId) throws GridException {
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
    @Override public GridCompute withName(@Nullable String taskName) {
        if (taskName != null) {
            guard();

            try {
                ctx.task().setThreadContext(TC_TASK_NAME, taskName);
            }
            finally {
                unguard();
            }
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
    @Override public GridCompute withResultClosure(@Nullable GridBiClosure<GridComputeJobResult, List<GridComputeJobResult>,
            GridComputeJobResultPolicy> res) {
        if (res != null) {
            guard();

            try {
                ctx.task().setThreadContext(TC_RESULT, res);
            }
            finally {
                unguard();
            }
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public GridCompute withFailoverSpi(@Nullable String spiName) {
        if (spiName != null) {
            guard();

            try {
                ctx.task().setThreadContext(TC_FAILOVER_SPI, spiName);
            }
            finally {
                unguard();
            }
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public GridCompute withCheckpointSpi(@Nullable String spiName) {
        if (spiName != null) {
            guard();

            try {
                ctx.task().setThreadContext(TC_CHECKPOINT_SPI, spiName);
            }
            finally {
                unguard();
            }
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public GridCompute withLoadBalancingSpi(@Nullable String spiName) {
        if (spiName != null) {
            guard();

            try {
                ctx.task().setThreadContext(TC_LOAD_BALANCING_SPI, spiName);
            }
            finally {
                unguard();
            }
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public GridCompute withTopologySpi(@Nullable String spiName) {
        if (spiName != null) {
            guard();

            try {
                ctx.task().setThreadContext(TC_TOPOLOGY_SPI, spiName);
            }
            finally {
                unguard();
            }
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override public GridCompute withSessionFullSupport() {
        guard();

        try {
            ctx.task().setThreadContext(TC_SES_FULL_SUPPORT, true);
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
            ctx.deploy().undeployTask(taskName);
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
    private CA wrapRun(@Nullable final String cacheName, Object affKey, final Runnable run) throws GridException {
        // In case cache key is passed instead of affinity key.
        final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);

        return new CA() {
            @GridCacheName
            private final String cn = cacheName;

            @GridCacheAffinityMapped
            private final Object ak = affKey0;

            @Override public void apply() {
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
    private <R> CO<R> wrapCall(@Nullable final String cacheName, Object affKey, final Callable<R> call)
        throws GridException {
        // In case cache key is passed instead of affinity key.
        final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);

        return new CO<R>() {
            @GridCacheName
            private final String cn = cacheName;

            @GridCacheAffinityMapped
            private final Object ak = affKey0;

            @Override public R apply() {
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
