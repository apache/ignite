// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.closure;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.compute.GridComputeJobResultPolicy.*;
import static org.gridgain.grid.kernal.processors.task.GridTaskThreadContextKey.*;

/**
 * @author @java.author
 * @version @java.version
 */
public class GridClosureProcessor extends GridProcessorAdapter {
    /** */
    private final Executor sysPool;

    /** */
    private final Executor pubPool;

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
     * @param r Either runnable or callable.
     * @return {@code True} if entered.
     */
    private boolean enterBusy(Object r) {
        if (!busyLock.tryReadLock()) {
            U.warn(log, "Ignoring closure execution (grid is stopping): " + r);

            return false;
        }

        return true;
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
    public GridFuture<?> runAsync(GridClosureCallMode mode, @Nullable Collection<? extends Runnable> jobs,
        @Nullable Collection<GridNode> nodes) {
        return runAsync(mode, jobs, nodes, false);
    }

    /**
     * @param mode Distribution mode.
     * @param jobs Closures to execute.
     * @param nodes Grid nodes.
     * @param sys If {@code true}, then system pool will be used.
     * @return Task execution future.
     */
    public GridFuture<?> runAsync(GridClosureCallMode mode, @Nullable Collection<? extends Runnable> jobs,
        @Nullable Collection<GridNode> nodes, boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (F.isEmpty(jobs))
                return new GridFinishedFuture(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture(ctx, makeException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(
                new T1(
                    mode,
                    jobs,
                    nodes,
                    (GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy>)
                        ctx.task().getThreadContext(TC_RESULT)
                ),
                null, sys
            );
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Task that is free of dragged in enclosing context for the method
     * {@link GridClosureProcessor#runAsync(GridClosureCallMode, Collection, Collection)}.
     */
    private class T1 extends GridComputeTaskNoReduceAdapter<Void> {
        /** */
        @GridLoadBalancerResource
        private GridComputeLoadBalancer lb;

        /** */
        private GridTuple4<
            GridClosureCallMode,
            Collection<? extends Runnable>,
            Collection<GridNode>,
            GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy>
        > t;

        /**
         * @param mode Call mode.
         * @param jobs Collection of jobs.
         * @param nodes Collection of nodes.
         * @param res Ad-hoc {@link org.gridgain.grid.compute.GridComputeTask#result(org.gridgain.grid.compute.GridComputeJobResult, List)}
         *      method implementation.
         */
        private T1(
            GridClosureCallMode mode,
            Collection<? extends Runnable> jobs,
            Collection<GridNode> nodes,
            @Nullable GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy> res) {
            super(U.peerDeployAware0(jobs));

            t = F.<
                GridClosureCallMode,
                Collection<? extends Runnable>,
                Collection<GridNode>,
                GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy>
                >t(mode, jobs, nodes, res);
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd)
            throws GridException {
            return t.get4() == null ? super.result(res, rcvd) : t.get4().apply(res, rcvd);
        }

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable Void arg)
            throws GridException {
            return absMap(t.get1(), t.get2(), F.retain(t.get3(), true, subgrid), lb);
        }
    }

    /**
     * @param mode Distribution mode.
     * @param job Closure to execute.
     * @param nodes Grid nodes.
     * @return Task execution future.
     */
    public GridFuture<?> runAsync(GridClosureCallMode mode, @Nullable Runnable job,
        @Nullable Collection<GridNode> nodes) {
        return runAsync(mode, job, nodes, false);
    }

    /**
     * @param mode Distribution mode.
     * @param job Closure to execute.
     * @param nodes Grid nodes.
     * @param sys If {@code true}, then system pool will be used.
     * @return Task execution future.
     */
    public GridFuture<?> runAsync(GridClosureCallMode mode, @Nullable Runnable job,
        @Nullable Collection<GridNode> nodes, boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (job == null)
                return new GridFinishedFuture(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture(ctx, makeException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(
                new T2(
                    mode,
                    job,
                    nodes,
                    (GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy>)
                        ctx.task().getThreadContext(TC_RESULT)
                ),
                null, sys
            );
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Task that is free of dragged in enclosing context for the method
     * {@link GridClosureProcessor#runAsync(GridClosureCallMode, Runnable, Collection)}.
     */
    private class T2 extends GridComputeTaskNoReduceAdapter<Void> {
        /** */
        @GridLoadBalancerResource
        private GridComputeLoadBalancer lb;

        /** */
        private GridTuple4<
            GridClosureCallMode,
            Runnable,
            Collection<GridNode>,
            GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy>
        > t;

        /**
         * @param mode Call mode.
         * @param job Job.
         * @param nodes Collection of nodes.
         * @param res Ad-hoc {@link org.gridgain.grid.compute.GridComputeTask#result(org.gridgain.grid.compute.GridComputeJobResult, List)}
         *      method implementation.
         */
        private T2(
            GridClosureCallMode mode,
            Runnable job,
            Collection<GridNode> nodes,
            GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy> res) {
            super(U.peerDeployAware(job));

            t = F.t(mode, job, nodes, res);
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd)
            throws GridException {
            return t.get4() == null ? super.result(res, rcvd) : t.get4().apply(res, rcvd);
        }

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable Void arg)
            throws GridException {
            return absMap(t.get1(), F.asList(t.get2()), F.retain(t.get3(), true, subgrid), lb);
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
    private Map<GridComputeJob, GridNode> absMap(GridClosureCallMode mode, Collection<? extends Runnable> jobs,
        Collection<GridNode> nodes, GridComputeLoadBalancer lb) throws GridException {
        assert mode != null;
        assert jobs != null;
        assert nodes != null;
        assert lb != null;

        if (!F.isEmpty(jobs) && !F.isEmpty(nodes)) {
            Map<GridComputeJob, GridNode> map = new HashMap<>(jobs.size(), 1);

            JobMapper mapper = new JobMapper(map);

            switch (mode) {
                case BROADCAST: {
                    for (GridNode n : nodes)
                        for (Runnable r : jobs)
                            mapper.map(new GridComputeJobWrapper(F.job(r), true), n);

                    break;
                }

                case BALANCE: {
                    for (Runnable r : jobs) {
                        GridComputeJob job = F.job(r);

                        mapper.map(job, lb.getBalancedNode(job, null));
                    }

                    break;
                }
            }

            return map;
        }
        else {
            return Collections.emptyMap();
        }
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
    private <R> Map<GridComputeJob, GridNode> outMap(GridClosureCallMode mode,
        Collection<? extends Callable<R>> jobs, Collection<GridNode> nodes, GridComputeLoadBalancer lb)
        throws GridException {
        assert mode != null;
        assert jobs != null;
        assert nodes != null;
        assert lb != null;

        if (!F.isEmpty(jobs) && !F.isEmpty(nodes)) {
            Map<GridComputeJob, GridNode> map = new HashMap<>(jobs.size(), 1);

            JobMapper mapper = new JobMapper(map);

            switch (mode) {
                case BROADCAST: {
                    for (GridNode n : nodes)
                        for (Callable<R> c : jobs)
                            mapper.map(new GridComputeJobWrapper(F.job(c), true), n);

                    break;
                }

                case BALANCE: {
                    for (Callable<R> c : jobs) {
                        GridComputeJob job = F.job(c);

                        mapper.map(job, lb.getBalancedNode(job, null));
                    }

                    break;
                }
            }

            return map;
        }
        else {
            return Collections.emptyMap();
        }
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
    public <R1, R2> GridFuture<R2> forkjoinAsync(GridClosureCallMode mode,
        @Nullable Collection<? extends Callable<R1>> jobs,
        @Nullable GridReducer<R1, R2> rdc, @Nullable Collection<GridNode> nodes) {
        assert mode != null;

        enterBusy();

        try {
            if (F.isEmpty(jobs) || rdc == null)
                return new GridFinishedFuture<>(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, makeException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(
                new T3<>(
                    mode,
                    jobs,
                    rdc,
                    nodes,
                    (GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy>)ctx.
                        task().getThreadContext(TC_RESULT)
                ),
                null
            );
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Task that is free of dragged in enclosing context for the method
     * {@link GridClosureProcessor#forkjoinAsync(GridClosureCallMode, Collection, GridReducer, Collection)}
     */
    private class T3<R1, R2> extends GridComputeTaskAdapter<Void, R2> {
        /** */
        @GridLoadBalancerResource
        private GridComputeLoadBalancer lb;

        /** */
        private GridTuple5<
            GridClosureCallMode,
            Collection<? extends Callable<R1>>,
            GridReducer<R1, R2>,
            Collection<GridNode>,
            GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy>
        > t;

        /**
         *
         * @param mode Call mode.
         * @param jobs Collection of jobs.
         * @param rdc Reducer.
         * @param nodes Collection of nodes.
         * @param res Ad-hoc {@link org.gridgain.grid.compute.GridComputeTask#result(org.gridgain.grid.compute.GridComputeJobResult, List)}
         *      method implementation.
         */
        private T3(
            GridClosureCallMode mode,
            Collection<? extends Callable<R1>> jobs,
            GridReducer<R1, R2> rdc,
            Collection<GridNode> nodes,
            GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy> res) {
            super(U.peerDeployAware0(jobs));

            t = F.<
                GridClosureCallMode,
                Collection<? extends Callable<R1>>,
                GridReducer<R1, R2>,
                Collection<GridNode>,
                GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy>
            >t(mode, jobs, rdc, nodes, res);
        }

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable Void arg)
            throws GridException {
            return outMap(t.get1(), t.get2(), F.retain(t.get4(), true, subgrid), lb);
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd)
            throws GridException {
            GridComputeJobResultPolicy resPlc = t.get5() == null ? super.result(res, rcvd) : t.get5().apply(res, rcvd);

            if (res.getException() == null && resPlc != FAILOVER && !t.get3().collect((R1)res.getData()))
                resPlc = REDUCE; // If reducer returned false - reduce right away.

            return resPlc;
        }

        /** {@inheritDoc} */
        @Override public R2 reduce(List<GridComputeJobResult> res) {
            return t.get3().reduce();
        }
    }

    /**
     * Creates appropriate empty projection exception.
     *
     * @return Empty projection exception.
     */
    private GridEmptyProjectionException makeException() {
        return new GridEmptyProjectionException("Topology projection is empty.");
    }

    /**
     * @param mode Distribution mode.
     * @param jobs Closures to execute.
     * @param nodes Grid nodes.
     * @param <R> Type.
     * @return Grid future for collection of closure results.
     */
    public <R> GridFuture<Collection<R>> callAsync(
        GridClosureCallMode mode,
        @Nullable Collection<? extends Callable<R>> jobs,
        @Nullable Collection<GridNode> nodes) {
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
    public <R> GridFuture<Collection<R>> callAsync(GridClosureCallMode mode,
        @Nullable Collection<? extends Callable<R>> jobs, @Nullable Collection<GridNode> nodes,
        boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (F.isEmpty(jobs))
                return new GridFinishedFuture<>(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, makeException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T7<>(mode, jobs, nodes, ctx), null, sys);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Task that is free of dragged in enclosing context for the method
     * {@link GridClosureProcessor#callAsync(GridClosureCallMode, Collection, Collection)}
     */
    private class T7<R> extends GridComputeTaskAdapter<Void, Collection<R>> {
        /** */
        private final GridClosureCallMode mode;

        /** */
        private final Collection<? extends Callable<R>> jobs;

        /** */
        private final Collection<GridNode> nodes;

        /** */
        private final GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>,
            GridComputeJobResultPolicy> resHndl;

        /**
         *
         * @param mode Call mode.
         * @param jobs Collection of jobs.
         * @param nodes Collection of nodes.
         * @param ctx Kernal context.
         */
        private T7(
            GridClosureCallMode mode,
            Collection<? extends Callable<R>> jobs,
            Collection<GridNode> nodes,
            GridKernalContext ctx) {
            super(U.peerDeployAware0(jobs));

            this.mode = mode;
            this.jobs = jobs;
            this.nodes = nodes;

            resHndl = ctx.task().getThreadContext(TC_RESULT);
        }

        /** */
        @GridLoadBalancerResource
        private GridComputeLoadBalancer lb;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable Void arg)
            throws GridException {
            return outMap(mode, jobs, F.retain(nodes, true, subgrid), lb);
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd)
            throws GridException {
            return resHndl == null ? super.result(res, rcvd) : resHndl.apply(res, rcvd);
        }

        /** {@inheritDoc} */
        @Override public Collection<R> reduce(List<GridComputeJobResult> res) {
            return F.jobResults(res);
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
    public <R> GridFuture<R> callAsync(GridClosureCallMode mode,
        @Nullable Callable<R> job, @Nullable Collection<GridNode> nodes) {
        return callAsync(mode, job, nodes, false);
    }

    /**
     * @param mode Distribution mode.
     * @param job Closure to execute.
     * @param nodes Grid nodes.
     * @param sys If {@code true}, then system pool will be used.
     * @param <R> Type.
     * @return Grid future for collection of closure results.
     */
    public <R> GridFuture<R> callAsyncNoFailover(GridClosureCallMode mode, @Nullable Callable<R> job,
        @Nullable Collection<GridNode> nodes, boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (job == null)
                return new GridFinishedFuture<>(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, makeException());

            ctx.task().setThreadContext(TC_RESULT, X.NO_FAILOVER);
            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T8<>(mode, job, nodes, ctx), null, sys);
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
    public <R> GridFuture<Collection<R>> callAsyncNoFailover(GridClosureCallMode mode,
        @Nullable Collection<? extends Callable<R>> jobs, @Nullable Collection<GridNode> nodes,
        boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (F.isEmpty(jobs))
                return new GridFinishedFuture<>(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, makeException());

            ctx.task().setThreadContext(TC_RESULT, X.NO_FAILOVER);
            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T7<>(mode, jobs, nodes, ctx), null, sys);
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
    public <R> GridFuture<R> callAsync(GridClosureCallMode mode,
        @Nullable Callable<R> job, @Nullable Collection<GridNode> nodes, boolean sys) {
        assert mode != null;

        enterBusy();

        try {
            if (job == null)
                return new GridFinishedFuture<>(ctx);

            if (F.isEmpty(nodes))
                return new GridFinishedFuture<>(ctx, makeException());

            ctx.task().setThreadContext(TC_SUBGRID, nodes);

            return ctx.task().execute(new T8<>(mode, job, nodes, ctx), null, sys);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Task that is free of dragged in enclosing context for the method
     * {@link GridClosureProcessor#callAsync(GridClosureCallMode, Callable, Collection)}
     */
    private class T8<R> extends GridComputeTaskAdapter<Void, R> {
        /** */
        private GridTuple4<
            GridClosureCallMode,
            Callable<R>,
            Collection<GridNode>,
            GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy>
        > t;

        /**
         *
         * @param mode Call mode.
         * @param job Job.
         * @param nodes Collection of nodes.
         * @param ctx Kernal context.
         */
        private T8(
            GridClosureCallMode mode,
            Callable<R> job,
            Collection<GridNode> nodes,
            GridKernalContext ctx) {
            super(U.peerDeployAware(job));

            t = F.t(
                mode,
                job,
                nodes,
                (GridClosure2X<GridComputeJobResult, List<GridComputeJobResult>, GridComputeJobResultPolicy>)ctx.task().
                    getThreadContext(TC_RESULT));
        }

        /** */
        @GridLoadBalancerResource
        private GridComputeLoadBalancer lb;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable Void arg)
            throws GridException {
            return outMap(t.get1(), F.asList(t.get2()), F.retain(t.get3(), true, subgrid), lb);
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd)
            throws GridException {
            return t.get4() == null ? super.result(res, rcvd) : t.get4().apply(res, rcvd);
        }

        /** {@inheritDoc} */
        @Override public R reduce(List<GridComputeJobResult> res) throws GridException {
            for (GridComputeJobResult r : res)
                if (r.getException() == null)
                    return r.getData();

            throw new GridException("Failed to find successful job result: " + res);
        }
    }

    /**
     * Gets either system or public pool.
     *
     * @param sys Whether to get system or public pool.
     * @return Requested worker pool.
     */
    private Executor pool(boolean sys) {
        return sys ? sysPool : pubPool;
    }

    /**
     * @param c Closure to execute.
     * @param sys Whether to run on system or public pool.
     * @return Future.
     * @throws GridException Thrown in case of any errors.
     */
    private GridFuture<?> runLocal(@Nullable final Runnable c, boolean sys) throws GridException {
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
                pool(sys).execute(w);
            }
            catch (RejectedExecutionException e) {
                U.error(log, "Failed to execute worker due to execution rejection " +
                    "(increase upper bound on " + (sys ? "system" : "public") + " executor service).", e);

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
    public GridFuture<?> runLocalSafe(Runnable c) {
        return runLocalSafe(c, true);
    }

    /**
     * Companion to {@link #runLocal(Runnable, boolean)} but in case of rejected execution re-runs
     * the closure in the current thread (blocking).
     *
     * @param c Closure to execute.
     * @param sys Whether to run on system or public pool.
     * @return Future.
     */
    public GridFuture<?> runLocalSafe(Runnable c, boolean sys) {
        try {
            return runLocal(c, sys);
        }
        catch (Throwable e) {
            if (e instanceof Error)
                U.error(log, "Closure execution failed with error.", e);

            // If execution was rejected - rerun locally.
            if (e.getCause() instanceof RejectedExecutionException) {
                U.warn(log, "Closure execution has been rejected (will execute in the same thread) [sysPool=" + sys +
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
     * @param sys Whether to run on system or public pool.
     * @param <R> Type of closure return value.
     * @return Future.
     * @throws GridException Thrown in case of any errors.
     */
    private <R> GridFuture<R> callLocal(@Nullable final Callable<R> c, boolean sys) throws GridException {
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
                pool(sys).execute(w);
            }
            catch (RejectedExecutionException e) {
                U.error(log, "Failed to execute worker due to execution rejection " +
                    "(increase upper bound on " + (sys ? "system" : "public") + " executor service).", e);

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
    public <R> GridFuture<R> callLocalSafe(Callable<R> c) {
        return callLocalSafe(c, true);
    }

    /**
     * Companion to {@link #callLocal(Callable, boolean)} but in case of rejected execution re-runs
     * the closure in the current thread (blocking).
     *
     * @param c Closure to execute.
     * @param sys Whether to run on system or public pool.
     * @return Future.
     */
    public <R> GridFuture<R> callLocalSafe(Callable<R> c, boolean sys) {
        try {
            return callLocal(c, sys);
        }
        catch (GridException e) {
            // If execution was rejected - rerun locally.
            if (e.getCause() instanceof RejectedExecutionException) {
                U.warn(log, "Closure execution has been rejected (will execute in the same thread) [sysPool=" + sys +
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

    /** */
    private class JobMapper {
        /** */
        private final Map<GridComputeJob, GridNode> map;

        /** */
        private boolean hadLocNode;

        /**
         * @param map Jobs map.
         */
        private JobMapper(Map<GridComputeJob, GridNode> map) {
            assert map != null;
            assert map.isEmpty();

            this.map = map;
        }

        /**
         * @param job Job.
         * @param node Node.
         * @throws GridException In case of error.
         */
        public void map(GridComputeJob job, GridNode node) throws GridException {
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
}
