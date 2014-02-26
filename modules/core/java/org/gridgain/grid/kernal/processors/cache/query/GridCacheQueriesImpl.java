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
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * TODO
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheQueriesImpl<K, V> implements GridCacheQueries<K, V> {
    /** */
    private final GridCacheContext<K, V> ctx;

    /** */
    private GridCacheProjectionImpl<K, V> prj;

    /**
     * @param ctx Context.
     * @param prj Projection.
     */
    public GridCacheQueriesImpl(GridCacheContext<K, V> ctx, @Nullable GridCacheProjectionImpl<K, V> prj) {
        assert ctx != null;

        this.ctx = ctx;
        this.prj = prj;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<Map.Entry<K, V>> createSqlQuery(Class<? extends V> cls, String clause) {
        return new GridCacheSqlQuery<>(ctx, (GridPredicate<GridCacheEntry<?, ?>>)filter(), cls, clause);
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<List<?>> createSqlFieldsQuery(String qry) {
        return new GridCacheSqlFieldsQuery(ctx, (GridPredicate<GridCacheEntry<?, ?>>)filter(), qry);
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<Map.Entry<K, V>> createFullTextQuery(Class<? extends V> cls, String search) {
        return new GridCacheFullTextQuery<>(ctx, (GridPredicate<GridCacheEntry<?, ?>>)filter(), cls, search);
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<Map.Entry<K, V>> createScanQuery() {
        return new GridCacheScanQuery<>(ctx, (GridPredicate<GridCacheEntry<?, ?>>)filter());
    }

    /** {@inheritDoc} */
    @Override public GridCacheContinuousQuery<K, V> createContinuousQuery() {
        return ctx.continuousQueries().createQuery(filter());
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> rebuildIndexes(Class<?> cls) {
        return ctx.queries().rebuildIndexes(cls);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> rebuildAllIndexes() {
        return ctx.queries().rebuildAllIndexes();
    }

    /**
     * @return Optional projection filter.
     */
    @Nullable private GridPredicate<GridCacheEntry<K, V>> filter() {
        return prj == null ? null : prj.predicate();
    }
}
