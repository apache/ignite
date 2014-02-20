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
 * Fields query implementation.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheFieldsQueryAdapter<K, V> extends GridCacheQueryBaseAdapter<K, V>
    implements GridCacheFieldsQuery<K, V> {
    /** Include meta data or not. */
    private boolean incMeta;

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
    protected GridCacheFieldsQueryAdapter(GridCacheQueryBaseAdapter<K, V> qry) {
        super(qry);
    }

    /** {@inheritDoc} */
    @Override public boolean includeMetadata() {
        return incMeta;
    }

    /** {@inheritDoc} */
    @Override public void includeMetadata(boolean incMeta) {
        this.incMeta = incMeta;
    }

    /** {@inheritDoc} */
    @Override public GridCacheFieldsQuery queryArguments(@Nullable Object[] args) {
        arguments(args);

        return this;
    }

    /** {@inheritDoc} */
    @Override public GridCacheFieldsQueryFuture execute(GridProjection[] grid) {
        seal();

        Collection<GridNode> nodes = nodes(grid);

        if (log.isDebugEnabled())
            log.debug("Executing query [query=" + this + ", nodes=" + nodes + ']');

        GridCacheQueryFuture<List<Object>> fut = execute(nodes, false, false, null, null);

        return (GridCacheFieldsQueryFuture)fut;
    }

    /** {@inheritDoc} */
    @Override public Collection<List<Object>> executeSync(GridProjection... grid) throws GridException {
        seal();

        Collection<GridNode> nodes = nodes(grid);

        if (log.isDebugEnabled())
            log.debug("Executing query [query=" + this + ", nodes=" + nodes + ']');

        return executeSync(nodes, false, false, null, null);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<List<Object>> executeSingle(GridProjection... grid) {
        seal();

        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing fields query for single result on nodes: " + toShortString(nodes)));

        return new SingleFuture<List<Object>>(nodes);
    }

    /** {@inheritDoc} */
    @Override public List<Object> executeSingleSync(GridProjection... grid) throws GridException {
        seal();

        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing fields query for single result on nodes: " + toShortString(nodes)));

        Collection<List<Object>> res = executeSync(nodes, false, false, null, null);

        return F.first(res);
    }

    /** {@inheritDoc} */
    @Override public <T> GridFuture<T> executeSingleField(GridProjection... grid) {
        final GridFutureAdapter<T> fut = new GridFutureAdapter<>(cctx.kernalContext());

        executeSingle(grid).listenAsync(new CI1<GridFuture<List<Object>>>() {
            @Override public void apply(GridFuture<List<Object>> f) {
                try {
                    Collection<Object> row = f.get();

                    fut.onDone((T)F.first(row));
                }
                catch (GridException e) {
                    fut.onDone(e);
                }
            }
        });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public <T> T executeSingleFieldSync(GridProjection... grid) throws GridException {
        List<Object> row = executeSingleSync(grid);

        return row != null ? (T)row.get(0) : null;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> visit(GridPredicate<List<Object>> vis, GridProjection[] grid) {
        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing fields query with visitor on nodes: " + toShortString(nodes)));

        return execute(nodes, false, false, null, vis);
    }

    /** {@inheritDoc} */
    @Override public void visitSync(GridPredicate<List<Object>> vis, GridProjection... grid) throws GridException {
        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing fields query with visitor on nodes: " + toShortString(nodes)));

        executeSync(nodes, false, false, null, vis);
    }

    /** {@inheritDoc} */
    @Override protected void registerClasses() throws GridException {
        // No-op.
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
    @SuppressWarnings("unchecked")
    @Override protected <R> Collection<R> executeSync(Collection<GridNode> nodes, boolean single,
        boolean rmtRdcOnly, @Nullable final GridBiInClosure<UUID, Collection<R>> pageLsnr,
        @Nullable GridPredicate<?> vis) throws GridException {
        cctx.deploy().registerClasses(arguments());

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

        return nodes.size() == 1 && nodes.iterator().next().equals(cctx.discovery().localNode()) ?
            qryMgr.queryFieldsLocalSync(this, false, rmtRdcOnly, pageLsnr0, vis) :
            qryMgr.queryFieldsDistributed(this, nodes, false, rmtRdcOnly, pageLsnr0, vis).get();
    }

    /** {@inheritDoc} */
    @Override public void enableDedup(boolean dedup) {
        throw new UnsupportedOperationException("Dedup operation is not supported by fields queries.");
    }
}
