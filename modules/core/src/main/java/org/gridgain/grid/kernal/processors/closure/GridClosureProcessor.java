/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.closure;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.compute.ComputeJobResultPolicy.*;
import static org.gridgain.grid.kernal.processors.task.GridTaskThreadContextKey.*;

/**
 *
 */
public class GridClosureProcessor extends GridProcessorAdapter {
    /** */
    private final Executor sysPool;

    /** */
    private final Executor pubPool;

    /** */
    private final Executor ggfsPool;

    /** Lock to control execution after stop. */
    private final GridSpinReadWriteLock busyLock = new GridSpinReadWriteLock();

    /** Workers count. */
    private final LongAdder workersCnt = new LongAdder();

    /**
     * @param ctx Kernal context.
     */
    public GridClosureProcessor(GridKernalContext ctx) {
        super(ctx);

        sysPool = ctx.config().getSystemExecutorService();
        pubPool = ctx.config().getExecutorService();
        ggfsPool = ctx.config().getGgfsExecutorService();
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (log.isDebugEnabled())
            log.debug("Started closure processor.");
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public void onKernalStop(boolean cancel) {
        busyLock.writeLock();

        boolean interrupted = Thread.interrupted();

        while (workersCnt.sum() != 0) {
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();

        if (log.isDebugEnabled())
            log.debug("Stopped closure processor.");
    }

    /**
     * @throws IllegalStateException If grid is stopped.
     */
    private void enterBusy() throws IllegalStateException {
        if (!busyLock.tryReadLock())
            throw new IllegalStateException("Closure processor cannot be used on stopped grid: " + ctx.gridName());
    }

    /**
     * Unlocks busy lock.
     */
    private void leaveBusy() {
        busyLock.readUnlock();
    }

    /**
     * @param mode Distribution mode.
     * @param jobs Closures to execute.
     * @param nodes Grid nodes.
     * @return Task execution future.
     */
    public IgniteFuture<?> runAsync(GridClosureCallMode mode, @Nullable Collection<? extends Runnable> jobs,
        @Nullable Collection<ClusterNode> nodes) {
        return runAsync(mode, jobs, nodes, false);
    }

    /**
     * @param mode Distribution mode.
     * @param jobs Closures to execute.
     * @param nodes Grid nodes.
     * @param sys If {@code true}, then system pool will be used.
     * @return Task execution future.
     */
    public IgniteFuture<?> runAsync(GridClosureCallMode mode, @Nullable Collection<? extends Runnable> jobs,
        @Nullable Collection<ClusterNode> nodes, boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (F.isEmpty(jobs))
                return new GridFinishedFuture(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T1(mode, jobs), null, sys);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param mode Distribution mode.
     * @param job Closure to execute.
     * @param nodes Grid nodes.
     * @return Task execution future.
     */
    public IgniteFuture<?> runAsync(GridClosureCallMode mode, @Nullable Runnable job,
        @Nullable Collection<ClusterNode> nodes) {
        return runAsync(mode, job, nodes, false);
    }

    /**
     * @param mode Distribution mode.
     * @param job Closure to execute.
     * @param nodes Grid nodes.
     * @param sys If {@code true}, then system pool will be used.
     * @return Task execution future.
     */
    public IgniteFuture<?> runAsync(GridClosureCallMode mode, @Nullable Runnable job,
        @Nullable Collection<ClusterNode> nodes, boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (job == null)
                return new GridFinishedFuture(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T2(mode, job), null, sys);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Maps {@link Runnable} jobs to specified nodes based on distribution mode.
     *
     * @param mode Distribution mode.
     * @param jobs Closures to map.
     * @param nodes Grid nodes.
     * @param lb Load balancer.
     * @throws GridException Thrown in case of any errors.
     * @return Mapping.
     */
    private Map<ComputeJob, ClusterNode> absMap(GridClosureCallMode mode, Collection<? extends Runnable> jobs,
        Collection<ClusterNode> nodes, ComputeLoadBalancer lb) throws GridException {
        assert mode != null;
        assert jobs != null;
        assert nodes != null;
        assert lb != null;

        if (!F.isEmpty(jobs) && !F.isEmpty(nodes)) {
            Map<ComputeJob, ClusterNode> map = new HashMap<>(jobs.size(), 1);

            JobMapper mapper = new JobMapper(map);

            switch (mode) {
                case BROADCAST: {
                    for (ClusterNode n : nodes)
                        for (Runnable r : jobs)
                            mapper.map(job(r), n);

                    break;
                }

                case BALANCE: {
                    for (Runnable r : jobs) {
                        ComputeJob job = job(r);

                        mapper.map(job, lb.getBalancedNode(job, null));
                    }

                    break;
                }
            }

            return map;
        }
        else
            return Collections.emptyMap();
    }

    /**
     * Maps {@link Callable} jobs to specified nodes based on distribution mode.
     *
     * @param mode Distribution mode.
     * @param jobs Closures to map.
     * @param nodes Grid nodes.
     * @param lb Load balancer.
     * @throws GridException Thrown in case of any errors.
     * @return Mapping.
     */
    private <R> Map<ComputeJob, ClusterNode> outMap(GridClosureCallMode mode,
        Collection<? extends Callable<R>> jobs, Collection<ClusterNode> nodes, ComputeLoadBalancer lb)
        throws GridException {
        assert mode != null;
        assert jobs != null;
        assert nodes != null;
        assert lb != null;

        if (!F.isEmpty(jobs) && !F.isEmpty(nodes)) {
            Map<ComputeJob, ClusterNode> map = new HashMap<>(jobs.size(), 1);

            JobMapper mapper = new JobMapper(map);

            switch (mode) {
                case BROADCAST: {
                    for (ClusterNode n : nodes)
                        for (Callable<R> c : jobs)
                            mapper.map(job(c), n);

                    break;
                }

                case BALANCE: {
                    for (Callable<R> c : jobs) {
                        ComputeJob job = job(c);

                        mapper.map(job, lb.getBalancedNode(job, null));
                    }

                    break;
                }
            }

            return map;
        }
        else
            return Collections.emptyMap();
    }

    /**
     * @param mode Distribution mode.
     * @param jobs Closures to execute.
     * @param rdc Reducer.
     * @param nodes Grid nodes.
     * @param <R1> Type.
     * @param <R2> Type.
     * @return Reduced result.
     */
    public <R1, R2> IgniteFuture<R2> forkjoinAsync(GridClosureCallMode mode,
        @Nullable Collection<? extends Callable<R1>> jobs,
        @Nullable IgniteReducer<R1, R2> rdc, @Nullable Collection<ClusterNode> nodes) {
        assert mode != null;

        enterBusy();

        try {
            if (F.isEmpty(jobs) || rdc == null)
                return new GridFinishedFuture<>(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T3<>(mode, jobs, rdc), null);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param mode Distribution mode.
     * @param jobs Closures to execute.
     * @param nodes Grid nodes.
     * @param <R> Type.
     * @return Grid future for collection of closure results.
     */
    public <R> IgniteFuture<Collection<R>> callAsync(
        GridClosureCallMode mode,
        @Nullable Collection<? extends Callable<R>> jobs,
        @Nullable Collection<ClusterNode> nodes) {
        return callAsync(mode, jobs, nodes, false);
    }

    /**
     * @param mode Distribution mode.
     * @param jobs Closures to execute.
     * @param nodes Grid nodes.
     * @param sys If {@code true}, then system pool will be used.
     * @param <R> Type.
     * @return Grid future for collection of closure results.
     */
    public <R> IgniteFuture<Collection<R>> callAsync(GridClosureCallMode mode,
        @Nullable Collection<? extends Callable<R>> jobs, @Nullable Collection<ClusterNode> nodes,
        boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (F.isEmpty(jobs))
                return new GridFinishedFuture<>(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T6<>(mode, jobs), null, sys);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     *
     * @param mode Distribution mode.
     * @param job Closure to execute.
     * @param nodes Grid nodes.
     * @param <R> Type.
     * @return Grid future for collection of closure results.
     */
    public <R> IgniteFuture<R> callAsync(GridClosureCallMode mode,
        @Nullable Callable<R> job, @Nullable Collection<ClusterNode> nodes) {
        return callAsync(mode, job, nodes, false);
    }

    /**
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param job Job.
     * @param nodes Grid nodes.
     * @return Job future.
     */
    public <R> IgniteFuture<R> affinityCall(@Nullable String cacheName, Object affKey, Callable<R> job,
        @Nullable Collection<ClusterNode> nodes) {
        enterBusy();

        try {
            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            // In case cache key is passed instead of affinity key.
            final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T5<>(cacheName, affKey0, job), null, false);
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx, e);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param job Job.
     * @param nodes Grid nodes.
     * @return Job future.
     */
    public IgniteFuture<?> affinityRun(@Nullable String cacheName, Object affKey, Runnable job,
        @Nullable Collection<ClusterNode> nodes) {
        enterBusy();

        try {
            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            // In case cache key is passed instead of affinity key.
            final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T4(cacheName, affKey0, job), null, false);
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx, e);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param mode Distribution mode.
     * @param job Closure to execute.
     * @param nodes Grid nodes.
     * @param sys If {@code true}, then system pool will be used.
     * @param <R> Type.
     * @return Grid future for collection of closure results.
     */
    public <R> IgniteFuture<R> callAsyncNoFailover(GridClosureCallMode mode, @Nullable Callable<R> job,
        @Nullable Collection<ClusterNode> nodes, boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (job == null)
                return new GridFinishedFuture<>(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_NO_FAILOVER, true);
            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T7<>(mode, job), null, sys);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param mode Distribution mode.
     * @param jobs Closures to execute.
     * @param nodes Grid nodes.
     * @param sys If {@code true}, then system pool will be used.
     * @param <R> Type.
     * @return Grid future for collection of closure results.
     */
    public <R> IgniteFuture<Collection<R>> callAsyncNoFailover(GridClosureCallMode mode,
        @Nullable Collection<? extends Callable<R>> jobs, @Nullable Collection<ClusterNode> nodes,
        boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (F.isEmpty(jobs))
                return new GridFinishedFuture<>(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_NO_FAILOVER, true);
            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T6<>(mode, jobs), null, sys);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param mode Distribution mode.
     * @param job Closure to execute.
     * @param nodes Grid nodes.
     * @param sys If {@code true}, then system pool will be used.
     * @param <R> Type.
     * @return Grid future for collection of closure results.
     */
    public <R> IgniteFuture<R> callAsync(GridClosureCallMode mode,
        @Nullable Callable<R> job, @Nullable Collection<ClusterNode> nodes, boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (job == null)
                return new GridFinishedFuture<>(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T7<>(mode, job), null, sys);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param job Job closure.
     * @param arg Optional job argument.
     * @param nodes Grid nodes.
     * @return Grid future for execution result.
     */
    public <T, R> IgniteFuture<R> callAsync(IgniteClosure<T, R> job, @Nullable T arg,
        @Nullable Collection<ClusterNode> nodes) {
        enterBusy();

        try {
            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T8<>(job, arg), null, false);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param job Job closure.
     * @param arg Optional job argument.
     * @param nodes Grid nodes.
     * @return Grid future for execution result.
     */
    public <T, R> IgniteFuture<Collection<R>> broadcast(IgniteClosure<T, R> job, @Nullable T arg,
        @Nullable Collection<ClusterNode> nodes) {
        enterBusy();

        try {
            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T11<>(job, arg, nodes), null, false);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param job Job closure.
     * @param arg Optional job argument.
     * @param nodes Grid nodes.
     * @return Grid future for execution result.
     */
    public <T, R> IgniteFuture<Collection<R>> broadcastNoFailover(IgniteClosure<T, R> job, @Nullable T arg,
        @Nullable Collection<ClusterNode> nodes) {
        enterBusy();

        try {
            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);
            ctx.task().setThreadContext(TC_NO_FAILOVER, true);

            return ctx.task().execute(new T11<>(job, arg, nodes), null, false);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param job Job closure.
     * @param args Job arguments.
     * @param nodes Grid nodes.
     * @return Grid future for execution result.
     */
    public <T, R> IgniteFuture<Collection<R>> callAsync(IgniteClosure<T, R> job, @Nullable Collection<? extends T> args,
        @Nullable Collection<ClusterNode> nodes) {
        enterBusy();

        try {
            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T9<>(job, args), null, false);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param job Job closure.
     * @param args Job arguments.
     * @param rdc Reducer.
     * @param nodes Grid nodes.
     * @return Grid future for execution result.
     */
    public <T, R1, R2> IgniteFuture<R2> callAsync(IgniteClosure<T, R1> job,
        Collection<? extends T> args, IgniteReducer<R1, R2> rdc, @Nullable Collection<ClusterNode> nodes) {
        enterBusy();

        try {
            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, U.emptyTopologyException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T10<>(job, args, rdc), null, false);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Gets pool by execution policy.
     *
     * @param plc Whether to get system or public pool.
     * @return Requested worker pool.
     */
    private Executor pool(GridClosurePolicy plc) {
        switch (plc) {
            case PUBLIC_POOL:
                return pubPool;

            case SYSTEM_POOL:
                return sysPool;

            case GGFS_POOL:
                return ggfsPool;

            default:
                throw new IllegalArgumentException("Invalid closure execution policy: " + plc);
        }
    }

    /**
     * Gets pool name by execution policy.
     *
     * @param plc Policy to choose executor pool.
     * @return Pool name.
     */
    private String poolName(GridClosurePolicy plc) {
        switch (plc) {
            case PUBLIC_POOL:
                return "public";

            case SYSTEM_POOL:
                return "system";

            case GGFS_POOL:
                return "ggfs";

            default:
                throw new IllegalArgumentException("Invalid closure execution policy: " + plc);
        }
    }

    /**
     * @param c Closure to execute.
     * @param sys If {@code true}, then system pool will be used, otherwise public pool will be used.
     * @return Future.
     * @throws GridException Thrown in case of any errors.
     */
    private IgniteFuture<?> runLocal(@Nullable final Runnable c, boolean sys) throws GridException {
        return runLocal(c, sys ? GridClosurePolicy.SYSTEM_POOL : GridClosurePolicy.PUBLIC_POOL);
    }

    /**
     * @param c Closure to execute.
     * @param plc Whether to run on system or public pool.
     * @return Future.
     * @throws GridException Thrown in case of any errors.
     */
    private IgniteFuture<?> runLocal(@Nullable final Runnable c, GridClosurePolicy plc) throws GridException {
        if (c == null)
            return new GridFinishedFuture(ctx);

        enterBusy();

        try {
            // Inject only if needed.
            if (!(c instanceof GridPlainRunnable))
                ctx.resource().inject(ctx.deploy().getDeployment(c.getClass().getName()), c.getClass(), c);

            final ClassLoader ldr = Thread.currentThread().getContextClassLoader();

            final GridWorkerFuture fut = new GridWorkerFuture(ctx);

            workersCnt.increment();

            GridWorker w = new GridWorker(ctx.gridName(), "closure-proc-worker", log) {
                @Override protected void body() {
                    try {
                        if (ldr != null)
                            U.wrapThreadLoader(ldr, c);
                        else
                            c.run();

                        fut.onDone();
                    }
                    catch (Throwable e) {
                        if (e instanceof Error)
                            U.error(log, "Closure execution failed with error.", e);

                        fut.onDone(U.cast(e));
                    }
                    finally {
                        workersCnt.decrement();
                    }
                }
            };

            fut.setWorker(w);

            try {
                pool(plc).execute(w);
            }
            catch (RejectedExecutionException e) {
                U.error(log, "Failed to execute worker due to execution rejection " +
                    "(increase upper bound on " + poolName(plc) + " executor service).", e);

                w.run();
            }

            return fut;
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Executes closure on system pool. Companion to {@link #runLocal(Runnable, boolean)} but
     * in case of rejected execution re-runs the closure in the current thread (blocking).
     *
     * @param c Closure to execute.
     * @return Future.
     */
    public IgniteFuture<?> runLocalSafe(Runnable c) {
        return runLocalSafe(c, true);
    }

    /**
     * Companion to {@link #runLocal(Runnable, boolean)} but in case of rejected execution re-runs
     * the closure in the current thread (blocking).
     *
     * @param c Closure to execute.
     * @param sys If {@code true}, then system pool will be used, otherwise public pool will be used.
     * @return Future.
     */
    public IgniteFuture<?> runLocalSafe(Runnable c, boolean sys) {
        return runLocalSafe(c, sys ? GridClosurePolicy.SYSTEM_POOL : GridClosurePolicy.PUBLIC_POOL);
    }

    /**
     * Companion to {@link #runLocal(Runnable, boolean)} but in case of rejected execution re-runs
     * the closure in the current thread (blocking).
     *
     * @param c Closure to execute.
     * @param plc Policy to choose executor pool.
     * @return Future.
     */
    public IgniteFuture<?> runLocalSafe(Runnable c, GridClosurePolicy plc) {
        try {
            return runLocal(c, plc);
        }
        catch (Throwable e) {
            if (e instanceof Error)
                U.error(log, "Closure execution failed with error.", e);

            // If execution was rejected - rerun locally.
            if (e.getCause() instanceof RejectedExecutionException) {
                U.warn(log, "Closure execution has been rejected (will execute in the same thread) [plc=" + plc +
                    ", closure=" + c + ']');

                try {
                    c.run();

                    return new GridFinishedFuture(ctx);
                }
                catch (Throwable t) {
                    if (t instanceof Error)
                        U.error(log, "Closure execution failed with error.", t);

                    return new GridFinishedFuture(ctx, U.cast(t));
                }
            }
            // If failed for other reasons - return error future.
            else
                return new GridFinishedFuture(ctx, U.cast(e));
        }
    }

    /**
     * @param c Closure to execute.
     * @param sys If {@code true}, then system pool will be used, otherwise public pool will be used.
     * @return Future.
     * @throws GridException Thrown in case of any errors.
     */
    private <R> IgniteFuture<R> callLocal(@Nullable final Callable<R> c, boolean sys) throws GridException {
        return callLocal(c, sys ? GridClosurePolicy.SYSTEM_POOL : GridClosurePolicy.PUBLIC_POOL);
    }

    /**
     * @param c Closure to execute.
     * @param plc Whether to run on system or public pool.
     * @param <R> Type of closure return value.
     * @return Future.
     * @throws GridException Thrown in case of any errors.
     */
    private <R> IgniteFuture<R> callLocal(@Nullable final Callable<R> c, GridClosurePolicy plc) throws GridException {
        if (c == null)
            return new GridFinishedFuture<>(ctx);

        enterBusy();

        try {
            // Inject only if needed.
            if (!(c instanceof GridPlainCallable))
                ctx.resource().inject(ctx.deploy().getDeployment(c.getClass().getName()), c.getClass(), c);

            final ClassLoader ldr = Thread.currentThread().getContextClassLoader();

            final GridWorkerFuture<R> fut = new GridWorkerFuture<>(ctx);

            workersCnt.increment();

            GridWorker w = new GridWorker(ctx.gridName(), "closure-proc-worker", log) {
                @Override protected void body() {
                    try {
                        if (ldr != null)
                            fut.onDone(U.wrapThreadLoader(ldr, c));
                        else
                            fut.onDone(c.call());
                    }
                    catch (Throwable e) {
                        if (e instanceof Error)
                            U.error(log, "Closure execution failed with error.", e);

                        fut.onDone(U.cast(e));
                    }
                    finally {
                        workersCnt.decrement();
                    }
                }
            };

            fut.setWorker(w);

            try {
                pool(plc).execute(w);
            }
            catch (RejectedExecutionException e) {
                U.error(log, "Failed to execute worker due to execution rejection " +
                    "(increase upper bound on " + poolName(plc) + " executor service).", e);

                w.run();
            }

            return fut;
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Executes closure on system pool. Companion to {@link #callLocal(Callable, boolean)}
     * but in case of rejected execution re-runs the closure in the current thread (blocking).
     *
     * @param c Closure to execute.
     * @return Future.
     */
    public <R> IgniteFuture<R> callLocalSafe(Callable<R> c) {
        return callLocalSafe(c, true);
    }

    /**
     * Executes closure on system pool. Companion to {@link #callLocal(Callable, boolean)}
     * but in case of rejected execution re-runs the closure in the current thread (blocking).
     *
     * @param c Closure to execute.
     * @param sys If {@code true}, then system pool will be used, otherwise public pool will be used.
     * @return Future.
     */
    public <R> IgniteFuture<R> callLocalSafe(Callable<R> c, boolean sys) {
        return callLocalSafe(c, sys ? GridClosurePolicy.SYSTEM_POOL : GridClosurePolicy.PUBLIC_POOL);
    }

    /**
     * Companion to {@link #callLocal(Callable, boolean)} but in case of rejected execution re-runs
     * the closure in the current thread (blocking).
     *
     * @param c Closure to execute.
     * @param plc Policy to choose executor pool.
     * @return Future.
     */
    public <R> IgniteFuture<R> callLocalSafe(Callable<R> c, GridClosurePolicy plc) {
        try {
            return callLocal(c, plc);
        }
        catch (GridException e) {
            // If execution was rejected - rerun locally.
            if (e.getCause() instanceof RejectedExecutionException) {
                U.warn(log, "Closure execution has been rejected (will execute in the same thread) [plc=" + plc +
                    ", closure=" + c + ']');

                try {
                    return new GridFinishedFuture<>(ctx, c.call());
                }
                // If failed again locally - return error future.
                catch (Exception e2) {
                    return new GridFinishedFuture<>(ctx, U.cast(e2));
                }
            }
            // If failed for other reasons - return error future.
            else
                return new GridFinishedFuture<>(ctx, U.cast(e));
        }
    }

    /**
     * Converts given closure with arguments to grid job.
     * @param job Job.
     * @param arg Optional argument.
     * @return Job.
     */
    @SuppressWarnings("IfMayBeConditional")
    private <T, R> ComputeJob job(final IgniteClosure<T, R> job, @Nullable final T arg) {
        A.notNull(job, "job");

        if (job instanceof ComputeJobMasterLeaveAware) {
            return new GridMasterLeaveAwareComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    return job.apply(arg);
                }

                @Override public void onMasterNodeLeft(GridComputeTaskSession ses) throws GridException {
                    ((ComputeJobMasterLeaveAware)job).onMasterNodeLeft(ses);
                }
            };
        }
        else {
            return new ComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    return job.apply(arg);
                }
            };
        }
    }

    /**
     * Converts given closure to a grid job.
     *
     * @param c Closure to convert to grid job.
     * @return Grid job made out of closure.
     */
    @SuppressWarnings("IfMayBeConditional")
    private ComputeJob job(final Callable<?> c) {
        A.notNull(c, "job");

        if (c instanceof ComputeJobMasterLeaveAware) {
            return new GridMasterLeaveAwareComputeJobAdapter() {
                @Override public Object execute() {
                    try {
                        return c.call();
                    }
                    catch (Exception e) {
                        throw new GridRuntimeException(e);
                    }
                }

                @Override public void onMasterNodeLeft(GridComputeTaskSession ses) throws GridException {
                    ((ComputeJobMasterLeaveAware)c).onMasterNodeLeft(ses);
                }
            };
        }
        else {
            return new ComputeJobAdapter() {
                @Override public Object execute() {
                    try {
                        return c.call();
                    }
                    catch (Exception e) {
                        throw new GridRuntimeException(e);
                    }
                }
            };
        }
    }

    /**
     * Converts given closure to a grid job.
     *
     * @param c Closure to convert to grid job.
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @return Grid job made out of closure.
     */
    @SuppressWarnings(value = {"IfMayBeConditional", "UnusedDeclaration"})
    private ComputeJob job(final Callable<?> c, @Nullable final String cacheName, final Object affKey) {
        A.notNull(c, "job");

        if (c instanceof ComputeJobMasterLeaveAware) {
            return new GridMasterLeaveAwareComputeJobAdapter() {
                /** */
                @GridCacheName
                private final String cn = cacheName;

                /** */
                @GridCacheAffinityKeyMapped
                private final Object ak = affKey;

                @Override public Object execute() {
                    try {
                        return c.call();
                    }
                    catch (Exception e) {
                        throw new GridRuntimeException(e);
                    }
                }

                @Override public void onMasterNodeLeft(GridComputeTaskSession ses) throws GridException {
                    ((ComputeJobMasterLeaveAware)c).onMasterNodeLeft(ses);
                }
            };
        }
        else {
            return new ComputeJobAdapter() {
                /** */
                @GridCacheName
                private final String cn = cacheName;

                /** */
                @GridCacheAffinityKeyMapped
                private final Object ak = affKey;

                @Override public Object execute() {
                    try {
                        return c.call();
                    }
                    catch (Exception e) {
                        throw new GridRuntimeException(e);
                    }
                }
            };
        }
    }

    /**
     * Converts given closure to a grid job.
     *
     * @param r Closure to convert to grid job.
     * @return Grid job made out of closure.
     */
    @SuppressWarnings("IfMayBeConditional")
    private static ComputeJob job(final Runnable r) {
        A.notNull(r, "job");

        if (r instanceof ComputeJobMasterLeaveAware) {
            return new GridMasterLeaveAwareComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    r.run();

                    return null;
                }

                @Override public void onMasterNodeLeft(GridComputeTaskSession ses) throws GridException {
                    ((ComputeJobMasterLeaveAware)r).onMasterNodeLeft(ses);
                }
            };
        }
        else {
            return new ComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    r.run();

                    return null;
                }
            };
        }
    }

    /**
     * Converts given closure to a grid job.
     *
     * @param r Closure to convert to grid job.
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @return Grid job made out of closure.
     */
    @SuppressWarnings(value = {"IfMayBeConditional", "UnusedDeclaration"})
    private ComputeJob job(final Runnable r, @Nullable final String cacheName, final Object affKey) {
        A.notNull(r, "job");

        if (r instanceof ComputeJobMasterLeaveAware) {
            return new GridMasterLeaveAwareComputeJobAdapter() {
                /** */
                @GridCacheName
                private final String cn = cacheName;

                /** */
                @GridCacheAffinityKeyMapped
                private final Object ak = affKey;

                @Nullable @Override public Object execute() {
                    r.run();

                    return null;
                }

                @Override public void onMasterNodeLeft(GridComputeTaskSession ses) throws GridException {
                    ((ComputeJobMasterLeaveAware)r).onMasterNodeLeft(ses);
                }
            };
        }
        else {
            return new ComputeJobAdapter() {
                /** */
                @GridCacheName
                private final String cn = cacheName;

                /** */
                @GridCacheAffinityKeyMapped
                private final Object ak = affKey;

                @Nullable @Override public Object execute() {
                    r.run();

                    return null;
                }
            };
        }
    }

    /** */
    private class JobMapper {
        /** */
        private final Map<ComputeJob, ClusterNode> map;

        /** */
        private boolean hadLocNode;

        /**
         * @param map Jobs map.
         */
        private JobMapper(Map<ComputeJob, ClusterNode> map) {
            assert map != null;
            assert map.isEmpty();

            this.map = map;
        }

        /**
         * @param job Job.
         * @param node Node.
         * @throws GridException In case of error.
         */
        public void map(ComputeJob job, ClusterNode node) throws GridException {
            assert job != null;
            assert node != null;

            if (ctx.localNodeId().equals(node.id())) {
                if (hadLocNode) {
                    GridMarshaller marsh = ctx.config().getMarshaller();

                    job = marsh.unmarshal(marsh.marshal(job), null);
                }
                else
                    hadLocNode = true;
            }

            map.put(job, node);
        }
    }

    /**
     * No-reduce task adapter.
     */
    private abstract static class TaskNoReduceAdapter<T> extends GridPeerDeployAwareTaskAdapter<T, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param pda Peer deploy aware instance.
         */
        protected TaskNoReduceAdapter(@Nullable GridPeerDeployAware pda) {
            super(pda);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     * Task that is free of dragged in enclosing context for the method
     * {@link GridClosureProcessor#runAsync(GridClosureCallMode, Collection, Collection)}.
     */
    private class T1 extends TaskNoReduceAdapter<Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @GridLoadBalancerResource
        private ComputeLoadBalancer lb;

        /** */
        private IgniteBiTuple<GridClosureCallMode, Collection<? extends Runnable>> t;

        /**
         * @param mode Call mode.
         * @param jobs Collection of jobs.
         */
        private T1(GridClosureCallMode mode, Collection<? extends Runnable> jobs) {
            super(U.peerDeployAware0(jobs));

            t = F.<
                GridClosureCallMode,
                Collection<? extends Runnable>
                >t(mode, jobs);
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
            throws GridException {
            return absMap(t.get1(), t.get2(), subgrid, lb);
        }
    }

    /**
     * Task that is free of dragged in enclosing context for the method
     * {@link GridClosureProcessor#runAsync(GridClosureCallMode, Runnable, Collection)}.
     */
    private class T2 extends TaskNoReduceAdapter<Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @GridLoadBalancerResource
        private ComputeLoadBalancer lb;

        /** */
        private IgniteBiTuple<GridClosureCallMode, Runnable> t;

        /**
         * @param mode Call mode.
         * @param job Job.
         */
        private T2(GridClosureCallMode mode, Runnable job) {
            super(U.peerDeployAware(job));

            t = F.t(mode, job);
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
            throws GridException {
            return absMap(t.get1(), F.asList(t.get2()), subgrid, lb);
        }
    }

    /**
     * Task that is free of dragged in enclosing context for the method
     * {@link GridClosureProcessor#forkjoinAsync(GridClosureCallMode, Collection, org.apache.ignite.lang.IgniteReducer, Collection)}
     */
    private class T3<R1, R2> extends GridPeerDeployAwareTaskAdapter<Void, R2> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @GridLoadBalancerResource
        private ComputeLoadBalancer lb;

        /** */
        private GridTuple3<GridClosureCallMode,
            Collection<? extends Callable<R1>>,
            IgniteReducer<R1, R2>
            > t;

        /**
         *
         * @param mode Call mode.
         * @param jobs Collection of jobs.
         * @param rdc Reducer.
         */
        private T3(GridClosureCallMode mode, Collection<? extends Callable<R1>> jobs, IgniteReducer<R1, R2> rdc) {
            super(U.peerDeployAware0(jobs));

            t = F.<
                GridClosureCallMode,
                Collection<? extends Callable<R1>>,
                IgniteReducer<R1, R2>
                >t(mode, jobs, rdc);
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
            throws GridException {
            return outMap(t.get1(), t.get2(), subgrid, lb);
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd)
            throws GridException {
            ComputeJobResultPolicy resPlc = super.result(res, rcvd);

            if (res.getException() == null && resPlc != FAILOVER && !t.get3().collect((R1)res.getData()))
                resPlc = REDUCE; // If reducer returned false - reduce right away.

            return resPlc;
        }

        /** {@inheritDoc} */
        @Override public R2 reduce(List<ComputeJobResult> res) {
            return t.get3().reduce();
        }
    }

    /**
     */
    private class T4 extends TaskNoReduceAdapter<Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String cacheName;

        /** */
        private Object affKey;

        /** */
        private Runnable job;

        /** */
        @GridLoadBalancerResource
        private ComputeLoadBalancer lb;

        /**
         * @param cacheName Cache name.
         * @param affKey Affinity key.
         * @param job Job.
         */
        private T4(@Nullable String cacheName, Object affKey, Runnable job) {
            super(U.peerDeployAware0(job));

            this.cacheName = cacheName;
            this.affKey = affKey;
            this.job = job;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
            throws GridException {
            ComputeJob job = job(this.job, cacheName, affKey);

            return Collections.singletonMap(job, lb.getBalancedNode(job, null));
        }
    }

    /**
     */
    private class T5<R> extends GridPeerDeployAwareTaskAdapter<Void, R> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String cacheName;

        /** */
        private Object affKey;

        /** */
        private Callable<R> job;

        /** */
        @GridLoadBalancerResource
        private ComputeLoadBalancer lb;

        /**
         * @param cacheName Cache name.
         * @param affKey Affinity key.
         * @param job Job.
         */
        private T5(@Nullable String cacheName, Object affKey, Callable<R> job) {
            super(U.peerDeployAware0(job));

            this.cacheName = cacheName;
            this.affKey = affKey;
            this.job = job;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
            throws GridException {
            ComputeJob job = job(this.job, cacheName, affKey);

            return Collections.singletonMap(job, lb.getBalancedNode(job, null));
        }

        /** {@inheritDoc} */
        @Override public R reduce(List<ComputeJobResult> res) throws GridException {
            for (ComputeJobResult r : res) {
                if (r.getException() == null)
                    return r.getData();
            }

            throw new GridException("Failed to find successful job result: " + res);
        }
    }

    /**
     * Task that is free of dragged in enclosing context for the method
     * {@link GridClosureProcessor#callAsync(GridClosureCallMode, Collection, Collection)}
     */
    private class T6<R> extends GridPeerDeployAwareTaskAdapter<Void, Collection<R>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final GridClosureCallMode mode;

        /** */
        private final Collection<? extends Callable<R>> jobs;

        /** */
        @GridLoadBalancerResource
        private ComputeLoadBalancer lb;

        /**
         *
         * @param mode Call mode.
         * @param jobs Collection of jobs.
         */
        private T6(
            GridClosureCallMode mode,
            Collection<? extends Callable<R>> jobs) {
            super(U.peerDeployAware0(jobs));

            this.mode = mode;
            this.jobs = jobs;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
            throws GridException {
            return outMap(mode, jobs, subgrid, lb);
        }

        /** {@inheritDoc} */
        @Override public Collection<R> reduce(List<ComputeJobResult> res) {
            return F.jobResults(res);
        }
    }

    /**
     * Task that is free of dragged in enclosing context for the method
     * {@link GridClosureProcessor#callAsync(GridClosureCallMode, Callable, Collection)}
     */
    private class T7<R> extends GridPeerDeployAwareTaskAdapter<Void, R> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private IgniteBiTuple<GridClosureCallMode, Callable<R>> t;

        /** */
        @GridLoadBalancerResource
        private ComputeLoadBalancer lb;

        /**
         * @param mode Call mode.
         * @param job Job.
         */
        private T7(GridClosureCallMode mode, Callable<R> job) {
            super(U.peerDeployAware(job));

            t = F.t(mode, job);
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
            throws GridException {
            return outMap(t.get1(), F.asList(t.get2()), subgrid, lb);
        }

        /** {@inheritDoc} */
        @Override public R reduce(List<ComputeJobResult> res) throws GridException {
            for (ComputeJobResult r : res)
                if (r.getException() == null)
                    return r.getData();

            throw new GridException("Failed to find successful job result: " + res);
        }
    }

    /**
     */
    private class T8<T, R> extends GridPeerDeployAwareTaskAdapter<Void, R> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private IgniteClosure<T, R> job;

        /** */
        private T arg;

        /** */
        @GridLoadBalancerResource
        private ComputeLoadBalancer lb;

        /**
         * @param job Job.
         * @param arg Optional job argument.
         */
        private T8(IgniteClosure<T, R> job, @Nullable T arg) {
            super(U.peerDeployAware(job));

            this.job = job;
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
            throws GridException {
            ComputeJob job = job(this.job, this.arg);

            return Collections.singletonMap(job, lb.getBalancedNode(job, null));
        }

        /** {@inheritDoc} */
        @Override public R reduce(List<ComputeJobResult> res) throws GridException {
            for (ComputeJobResult r : res)
                if (r.getException() == null)
                    return r.getData();

            throw new GridException("Failed to find successful job result: " + res);
        }
    }

    /**
     */
    private class T9<T, R> extends GridPeerDeployAwareTaskAdapter<Void, Collection<R>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private IgniteClosure<T, R> job;

        /** */
        private Collection<? extends T> args;

        /** */
        @GridLoadBalancerResource
        private ComputeLoadBalancer lb;

        /**
         * @param job Job.
         * @param args Job arguments.
         */
        private T9(IgniteClosure<T, R> job, Collection<? extends T> args) {
            super(U.peerDeployAware(job));

            this.job = job;
            this.args = args;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
            throws GridException {
            Map<ComputeJob, ClusterNode> map = new HashMap<>(args.size(), 1);

            JobMapper mapper = new JobMapper(map);

            for (T jobArg : args) {
                ComputeJob job = job(this.job, jobArg);

                mapper.map(job, lb.getBalancedNode(job, null));
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Collection<R> reduce(List<ComputeJobResult> res) throws GridException {
            return F.jobResults(res);
        }
    }

    /**
     */
    private class T10<T, R1, R2> extends GridPeerDeployAwareTaskAdapter<Void, R2> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private IgniteClosure<T, R1> job;

        /** */
        private Collection<? extends T> args;

        /** */
        private IgniteReducer<R1, R2> rdc;

        /** */
        @GridLoadBalancerResource
        private ComputeLoadBalancer lb;

        /**
         * @param job Job.
         * @param args Job arguments.
         * @param rdc Reducer.
         */
        private T10(IgniteClosure<T, R1> job, Collection<? extends T> args, IgniteReducer<R1, R2> rdc) {
            super(U.peerDeployAware(job));

            this.job = job;
            this.args = args;
            this.rdc = rdc;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
            throws GridException {
            Map<ComputeJob, ClusterNode> map = new HashMap<>(args.size(), 1);

            JobMapper mapper = new JobMapper(map);

            for (T jobArg : args) {
                ComputeJob job = job(this.job, jobArg);

                mapper.map(job, lb.getBalancedNode(job, null));
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd)
            throws GridException {
            ComputeJobResultPolicy resPlc = super.result(res, rcvd);

            if (res.getException() == null && resPlc != FAILOVER && !rdc.collect((R1) res.getData()))
                resPlc = REDUCE; // If reducer returned false - reduce right away.

            return resPlc;
        }

        /** {@inheritDoc} */
        @Override public R2 reduce(List<ComputeJobResult> res) throws GridException {
            return rdc.reduce();
        }
    }

    /**
     */
    private class T11<T, R> extends GridPeerDeployAwareTaskAdapter<Void, Collection<R>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteClosure<T, R> job;

        /** */
        private final T arg;

        /**
         * @param job Job.
         * @param arg Job argument.
         * @param nodes Collection of nodes.
         */
        private T11(IgniteClosure<T, R> job, @Nullable T arg, Collection<ClusterNode> nodes) {
            super(U.peerDeployAware(job));

            this.job = job;
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Void arg)
            throws GridException {
            if (F.isEmpty(subgrid))
                return Collections.emptyMap();

            Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size(), 1);

            JobMapper mapper = new JobMapper(map);

            for (ClusterNode n : subgrid)
                mapper.map(job(job, this.arg), n);

            return map;
        }

        /** {@inheritDoc} */
        @Override public Collection<R> reduce(List<ComputeJobResult> res) {
            return F.jobResults(res);
        }
    }
}
