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
public class GridCacheReduceQueryAdapter<K, V, R1, R2> extends GridCacheQueryBaseAdapter<K, V, GridCacheReduceQuery<K, V, R1, R2>>
    implements GridCacheReduceQuery<K, V, R1, R2> {
    /** Remote reducer. */
    private volatile GridReducer<Map.Entry<K, V>, R1> rmtRdc;

    /** Remote reducer bytes. */
    private volatile byte[] rmtRdcBytes;

    /** Local reducer bytes. */
    private volatile byte[] locRdcBytes;

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
        rmtRdcBytes = qry.rmtRdcBytes;

        locRdcBytes = qry.locRdcBytes;
    }

    /** {@inheritDoc} */
    @Override protected void registerClasses() throws GridException {
        assert cctx.deploymentEnabled();

        context().deploy().registerClass(rmtRdc);
    }

    /** {@inheritDoc} */
    @Override public GridCacheReduceQueryAdapter<K, V, R1, R2> remoteReducer(GridReducer<Map.Entry<K, V>, R1> rmtRdc) {
        GridCacheReduceQueryAdapter<K, V, R1, R2> cp = copy();

        try {
            cp.rmtRdc = rmtRdc;
            cp.rmtRdcBytes = cctx.marshaller().marshal(rmtRdc);

            return cp;
        }
        catch (GridException e) {
            throw new GridRuntimeException("Failed to save remote reducer state for future executions: " + rmtRdc, e);
        }
    }

    /**
     * @return Remote reducer.
     */
    public GridReducer<Map.Entry<K, V>, R1> remoteReducer() {
        try {
            if (rmtRdcBytes == null)
                return null;

            return cctx.marshaller().unmarshal(rmtRdcBytes, cctx.deploy().globalLoader());
        }
        catch (Exception e) {
            throw new GridRuntimeException("Failed to restore remote reducer state.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheReduceQueryAdapter<K, V, R1, R2> localReducer(GridReducer<R1, R2> locRdc) {
        GridCacheReduceQueryAdapter<K, V, R1, R2> cp = copy();

        try {
            cp.locRdcBytes = cctx.marshaller().marshal(locRdc);

            return cp;
        }
        catch (GridException e) {
            throw new GridRuntimeException("Failed to save local reducer state for future executions: " + locRdc, e);
        }
    }

    /**
     * @return Local reducer.
     */
    public GridReducer<R1, R2> localReducer() {
        try {
            return cctx.marshaller().unmarshal(locRdcBytes, cctx.deploy().globalLoader());
        }
        catch (GridException e) {
            throw new GridRuntimeException("Failed to restore local reducer state.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheReduceQuery<K, V, R1, R2> queryArguments(@Nullable Object... args) {
        GridCacheReduceQueryAdapter<K, V, R1, R2> cp = new GridCacheReduceQueryAdapter<>(this);

        cp.arguments(args);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<R2> reduce() {
        Collection<GridNode> nodes = nodes();

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing reduce query " + toShortString(nodes)));

        if (locRdcBytes == null) {
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
    @Override public GridFuture<Collection<R1>> reduceRemote() {
        if (rmtRdc == null) {
            GridFutureAdapter<Collection<R1>> errFut = new GridFutureAdapter<>(cctx.kernalContext());

            errFut.onDone(new GridException("Remote reducer must be set."));

            return errFut;
        }

        Collection<GridNode> nodes = nodes();

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing reduce remote query " + toShortString(nodes)));

        return execute(nodes, false, true, null, null);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheReduceQueryAdapter<K, V, R1, R2> copy() {
        return new GridCacheReduceQueryAdapter<>(this);
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
