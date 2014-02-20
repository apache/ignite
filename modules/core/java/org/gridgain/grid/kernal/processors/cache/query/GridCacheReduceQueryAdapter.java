// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Adapter for reduce cache queries.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheReduceQueryAdapter<K, V, R1, R2> extends GridCacheQueryBaseAdapter<K, V>
    implements GridCacheReduceQuery<K, V, R1, R2> {
    /** Remote reducer. */
    private volatile GridClosure<Object[], GridReducer<Map.Entry<K, V>, R1>> rmtRdc;

    /** Local reducer. */
    private volatile GridClosure<Object[], GridReducer<R1, R2>> locRdc;

    /**
     * @param ctx Cache registry.
     * @param type Query type.
     * @param clause Query clause.
     * @param cls Query class.
     * @param clsName Query class name.
     * @param prjFilter Projection filter.
     * @param prjFlags Projection flags.
     */
    public GridCacheReduceQueryAdapter(GridCacheContext<K, V> ctx, GridCacheQueryType type, String clause,
        Class<?> cls, String clsName, GridPredicate<GridCacheEntry<K, V>> prjFilter,
        Collection<GridCacheFlag> prjFlags) {
        super(ctx, type, clause, cls, clsName, prjFilter, prjFlags);
    }

    /**
     * @param qry Query to copy from (ignoring arguments).
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    private GridCacheReduceQueryAdapter(GridCacheReduceQueryAdapter<K, V, R1, R2> qry) {
        super(qry);

        rmtRdc = qry.rmtRdc;
        locRdc = qry.locRdc;
    }

    /** {@inheritDoc} */
    @Override protected void registerClasses() throws GridException {
        assert cctx.deploymentEnabled();

        context().deploy().registerClass(rmtRdc);
    }

    /** {@inheritDoc} */
    @Override public void remoteReducer(GridClosure<Object[], GridReducer<Map.Entry<K, V>, R1>> rmtRdc) {
        synchronized (mux) {
            checkSealed();

            this.rmtRdc = rmtRdc;
        }
    }

    /**
     * @return Remote reducer.
     */
    public GridClosure<Object[], GridReducer<Map.Entry<K, V>, R1>> remoteReducer() {
        return rmtRdc;
    }

    /** {@inheritDoc} */
    @Override public void localReducer(GridClosure<Object[], GridReducer<R1, R2>> locRdc) {
        synchronized (mux) {
            checkSealed();

            this.locRdc = locRdc;
        }
    }

    /**
     * @return Local reducer.
     */
    public GridClosure<Object[], GridReducer<R1, R2>> localReducer() {
        return locRdc;
    }

    /** {@inheritDoc} */
    @Override public GridCacheReduceQuery<K, V, R1, R2> queryArguments(@Nullable Object... args) {
        GridCacheReduceQueryAdapter<K, V, R1, R2> cp = new GridCacheReduceQueryAdapter<>(this);

        cp.arguments(args);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public GridCacheReduceQuery<K, V, R1, R2> closureArguments(@Nullable Object... args) {
        GridCacheReduceQueryAdapter<K, V, R1, R2> cp = new GridCacheReduceQueryAdapter<>(this);

        cp.setClosureArguments(args);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<R2> reduce(GridProjection[] grid) {
        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing reduce query " + toShortString(nodes)));

        if (locRdc == null) {
            GridFutureAdapter<R2> errFut = new GridFutureAdapter<>(cctx.kernalContext());

            errFut.onDone(new GridException("Local reducer must be set."));

            return errFut;
        }

        GridCacheQueryFuture<R2> fut = execute(nodes, false, false, null, null);

        final ReduceFuture<R2> rdcFut = new ReduceFuture<>(fut);

        fut.listenAsync(new GridInClosure<GridFuture<Collection<R2>>>() {
            @Override public void apply(GridFuture<Collection<R2>> fut) {
                try {
                    Collection<R2> coll = fut.get();

                    rdcFut.onDone(coll.iterator().next());
                }
                catch (GridException e) {
                    rdcFut.onDone(e);
                }
            }
        });

        return rdcFut;
    }

    /** {@inheritDoc} */
    @Override public R2 reduceSync(@Nullable GridProjection... grid) throws GridException {
        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing reduce query " + toShortString(nodes)));

        if (locRdc == null)
            throw new GridException("Local reducer must be set.");

        return (R2)F.first(executeSync(nodes, false, false, null, null));
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Collection<R1>> reduceRemote(GridProjection[] grid) {
        if (rmtRdc == null) {
            GridFutureAdapter<Collection<R1>> errFut = new GridFutureAdapter<>(cctx.kernalContext());

            errFut.onDone(new GridException("Remote reducer must be set."));

            return errFut;
        }

        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing reduce remote query " + toShortString(nodes)));

        return execute(nodes, false, true, null, null);
    }

    /** {@inheritDoc} */
    @Override public Collection<R1> reduceRemoteSync(GridProjection... grid) throws GridException {
        if (rmtRdc == null)
            throw new GridException("Remote reducer must be set.");

        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing reduce remote query " + toShortString(nodes)));

        return executeSync(nodes, false, true, null, null);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        // No-op.
    }

    /**
     *
     */
    private class ReduceFuture<T> extends GridFutureAdapter<T> {
        /** */
        private GridCacheQueryFuture<R2> fut;

        /**
         * Required by {@link Externalizable}.
         */
        public ReduceFuture() {
            // No-op.
        }

        /**
         * @param fut General query future.
         */
        private ReduceFuture(GridCacheQueryFuture<R2> fut) {
            super(cctx.kernalContext());

            this.fut = fut;
        }

        /**
         * @return General query future.
         */
        public GridCacheQueryFuture<R2> future() {
            return fut;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws GridException {
            return fut.cancel();
        }
    }
}
