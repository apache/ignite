// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.query.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Local query manager.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheLocalQueryManager<K, V> extends GridCacheQueryManager<K, V> {
    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected boolean onPageReady(
        boolean loc,
        GridCacheQueryInfo qryInfo,
        Collection<?> data,
        boolean finished, Throwable e) {
        GridCacheQueryFutureAdapter fut = qryInfo.localQueryFuture();

        assert fut != null;

        if (e != null)
            fut.onPage(null, null, e, true);
        else
            fut.onPage(null, data, null, finished);

        return true;
    }

//    /** {@inheritDoc} */
//    @Override protected boolean onFieldsPageReady(boolean loc,
//        GridCacheQueryInfo<K, V> qryInfo,
//        @Nullable List<GridCacheSqlFieldMetadata> metaData,
//        @Nullable Collection<List<GridIndexingEntity<?>>> entities,
//        @Nullable Collection<?> data,
//        boolean finished,
//        @Nullable Throwable e) {
//        assert qryInfo != null;
//
//        GridCacheLocalFieldsQueryFuture fut = (GridCacheLocalFieldsQueryFuture)qryInfo.localQueryFuture();
//
//        assert fut != null;
//
//        if (e != null)
//            fut.onPage(null, null, null, e, true);
//        else
//            fut.onPage(null, metaData, data, null, finished);
//
//        return true;
//    }

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        super.start0();

        assert cctx.config().getCacheMode() == LOCAL;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryFuture<?> queryLocal(GridCacheQueryBean qry) {
        assert cctx.config().getCacheMode() == LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing query on local node: " + qry);

        GridCacheLocalQueryFuture<K, V, ?> fut = new GridCacheLocalQueryFuture<>(cctx, qry);

        try {
            qry.query().validate();

            fut.execute();
        }
        catch (GridException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryFuture<?> queryDistributed(GridCacheQueryBean qry, Collection<GridNode> nodes) {
        assert cctx.config().getCacheMode() == LOCAL;

        throw new GridRuntimeException("Distributed queries are not available for local cache " +
            "(use 'GridCacheQuery.execute(grid.forLocal())' instead) [cacheName=" + cctx.name() + ']');
    }

    /** {@inheritDoc} */
    @Override public void loadPage(long id, GridCacheQueryAdapter<?> qry, Collection<GridNode> nodes, boolean all) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridCacheFieldsQueryFuture queryFieldsLocal(GridCacheFieldsQueryBase qry, boolean single,
        boolean rmtOnly, @Nullable GridBiInClosure<UUID, Collection<List<Object>>> pageLsnr,
        @Nullable GridPredicate<?> vis) {
        return queryFieldsLocal(qry, single, rmtOnly, pageLsnr, vis, false);
    }

    /**
     * @param qry Query.
     * @param single {@code true} if single result requested, {@code false} if multiple.
     * @param rmtOnly {@code true} for reduce query when using remote reducer only,
     *      otherwise it is always {@code false}.
     * @param pageLsnr Page listener.
     * @param vis Visitor predicate.
     * @param sync Whether to execute synchronously.
     * @return Iterator over query results. Note that results become available as they come.
     */
    private GridCacheFieldsQueryFuture queryFieldsLocal(GridCacheFieldsQueryBase qry, boolean single,
        boolean rmtOnly, @Nullable GridBiInClosure<UUID, Collection<List<Object>>> pageLsnr,
        @Nullable GridPredicate<?> vis, boolean sync) {
        assert cctx.config().getCacheMode() == LOCAL;

        if (log.isDebugEnabled())
            log.debug("Executing query on local node: " + qry);

        GridCacheLocalFieldsQueryFuture fut = new GridCacheLocalFieldsQueryFuture(
            cctx, qry, single, rmtOnly, pageLsnr, vis);

        try {
            validateQuery(qry);

            fut.execute(sync);
        }
        catch (GridException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /** {@inheritDoc} */
    @Override public GridCacheFieldsQueryFuture queryFieldsDistributed(GridCacheFieldsQueryBase qry,
        Collection<GridNode> nodes, boolean single, boolean rmtOnly,
        @Nullable GridBiInClosure<UUID, Collection<List<Object>>> pageLsnr, @Nullable GridPredicate<?> vis) {
        assert cctx.config().getCacheMode() == LOCAL;

        throw new GridRuntimeException("Distributed queries are not available for local cache " +
            "(use 'GridCacheQuery.execute(grid.forLocal())' instead) [cacheName=" + cctx.name() + ']');
    }
}
