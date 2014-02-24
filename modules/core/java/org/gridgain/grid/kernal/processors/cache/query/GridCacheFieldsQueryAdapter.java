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
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Fields query implementation.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheFieldsQueryAdapter<K, V>
    extends GridCacheFieldsQueryBase<K, V, GridCacheFieldsQuery<K, V>> implements GridCacheFieldsQuery<K, V> {
    /**
     * @param cctx Cache context.
     * @param clause Clause.
     * @param prjFilter Projection filter.
     * @param prjFlags Projection flags.
     */
    protected GridCacheFieldsQueryAdapter(GridCacheContext<K, V> cctx, String clause,
        GridPredicate<GridCacheEntry<K, V>> prjFilter, Collection<GridCacheFlag> prjFlags) {
        super(cctx, null, clause, null, null, prjFilter, prjFlags);
    }

    /**
     * @param qry Query.
     */
    protected GridCacheFieldsQueryAdapter(GridCacheFieldsQueryAdapter<K, V> qry) {
        super(qry);
    }

    /** {@inheritDoc} */
    @Override public GridCacheFieldsQuery queryArguments(@Nullable Object[] args) {
        arguments(args);

        return this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheFieldsQueryFuture execute() {
        seal();

        Collection<GridNode> nodes = nodes();

        if (log.isDebugEnabled())
            log.debug("Executing query [query=" + this + ", nodes=" + nodes + ']');

        GridCacheQueryFuture<List<Object>> fut = execute(nodes, false, false, null, null);

        return (GridCacheFieldsQueryFuture)fut;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<List<Object>> executeSingle() {
        seal();

        Collection<GridNode> nodes = nodes();

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing fields query for single result on nodes: " + toShortString(nodes)));

        return new SingleFuture<List<Object>>(nodes);
    }

    /** {@inheritDoc} */
    @Override public <T> GridFuture<T> executeSingleField() {
        return executeSingle().chain(new CX1<GridFuture<List<Object>>, T>() {
            @Override public T applyx(GridFuture<List<Object>> res) throws GridException {
                Collection<Object> row = res.get();

                return (T)F.first(row);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> visit(GridPredicate<List<Object>> vis) {
        Collection<GridNode> nodes = nodes();

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing fields query with visitor on nodes: " + toShortString(nodes)));

        return execute(nodes, false, false, null, vis);
    }

    /** {@inheritDoc} */
    @Override protected void registerClasses() throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected GridCacheFieldsQueryAdapter<K, V> copy() {
        return new GridCacheFieldsQueryAdapter<>(this);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        // No-op.
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
}
