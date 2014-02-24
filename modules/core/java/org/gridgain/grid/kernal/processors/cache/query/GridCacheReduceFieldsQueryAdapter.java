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
 * Adapter for reduce fields cache queries.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheReduceFieldsQueryAdapter<K, V, R1, R2>
    extends GridCacheFieldsQueryBase<K, V, GridCacheReduceFieldsQuery<K, V, R1, R2>>
    implements GridCacheReduceFieldsQuery<K, V, R1, R2> {
    /** Remote reducer. */
    private volatile GridReducer<List<Object>, R1> rmtRdc;

    /** Remote reducer bytes. */
    private volatile byte[] rmtRdcBytes;

    /** Local reducer bytes. */
    private volatile byte[] locRdcBytes;

    /**
     * @param cctx Cache context.
     * @param clause Clause.
     * @param prjFilter Projection filter.
     * @param prjFlags Projection flags.
     */
    protected GridCacheReduceFieldsQueryAdapter(GridCacheContext<K, V> cctx, String clause,
        GridPredicate<GridCacheEntry<K, V>> prjFilter, Collection<GridCacheFlag> prjFlags) {
        super(cctx, null, clause, null, null, prjFilter, prjFlags);
    }

    /**
     * @param qry Query.
     */
    private GridCacheReduceFieldsQueryAdapter(GridCacheReduceFieldsQueryAdapter<K, V, R1, R2> qry) {
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
    @Override public GridCacheReduceFieldsQueryAdapter<K, V, R1, R2> remoteReducer(GridReducer<List<Object>, R1> rmtRdc) {
        GridCacheReduceFieldsQueryAdapter<K, V, R1, R2> cp = copy();

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
    public GridReducer<List<Object>, R1> remoteReducer() {
        // Reducer is stateful, thus needs to be unmarshalled on every query execution.
        try {
            if (rmtRdcBytes == null)
                return null;

            return cctx.marshaller().unmarshal(rmtRdcBytes, cctx.deploy().globalLoader());
        }
        catch (GridException e) {
            throw new GridRuntimeException("Failed to restore remote reducer state.", e);
        }
    }

    /**
     * @return Remote reducer bytes.
     */
    public byte[] remoteReducerBytes() {
        return rmtRdcBytes;
    }

    /** {@inheritDoc} */
    @Override public GridCacheReduceFieldsQueryAdapter<K, V, R1, R2> localReducer(GridReducer<R1, R2> locRdc) {
        GridCacheReduceFieldsQueryAdapter<K, V, R1, R2> cp = copy();

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
    @Override public void close() throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridCacheReduceFieldsQueryAdapter<K, V, R1, R2> queryArguments(@Nullable Object... args) {
        arguments(args);

        return this;
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
    @Override protected GridCacheReduceFieldsQueryAdapter<K, V, R1, R2> copy() {
        return new GridCacheReduceFieldsQueryAdapter<>(this);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected <R> GridCacheQueryFuture<R> execute(Collection<GridNode> nodes, boolean single,
        boolean rmtRdcOnly, @Nullable final GridBiInClosure<UUID, Collection<R>> pageLsnr,
        @Nullable GridPredicate<?> vis) {
        if (cctx.deploymentEnabled()) {
            try {
                cctx.deploy().registerClasses(arguments());
            }
            catch (GridException e) {
                return (GridCacheQueryFuture<R>)new GridCacheErrorFieldsQueryFuture(
                    cctx.kernalContext(), e, includeMetadata());
            }
        }

        GridCacheQueryManager qryMgr = cctx.queries();

        GridBiInClosure<UUID, Collection<List<Object>>> pageLsnr0 =
            new CI2<UUID, Collection<List<Object>>>() {
                @Override public void apply(UUID uuid, Collection<List<Object>> cols) {
                    if (pageLsnr != null) {
                        Collection<R> col = (Collection<R>)cols;

                        pageLsnr.apply(uuid, col);
                    }
                }
            };

        return (GridCacheQueryFuture<R>)(
            nodes.size() == 1 && nodes.iterator().next().equals(cctx.discovery().localNode()) ?
                qryMgr.queryFieldsLocal(this, false, rmtRdcOnly, pageLsnr0, vis) :
                qryMgr.queryFieldsDistributed(this, nodes, false, rmtRdcOnly, pageLsnr0, vis));
    }

    /** {@inheritDoc} */
    @Override public void enableDedup(boolean dedup) {
        throw new UnsupportedOperationException("Dedup operation is not supported by fields queries.");
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
