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
 * Per-projection queries object returned to user.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheQueriesImpl<K, V> implements GridCacheQueries<K, V> {
    /** Cache projection, can be {@code null}. */
    private GridCacheProjectionImpl<K, V> prj;

    /** Cache context. */
    private GridCacheContext<K, V> cctx;

    /**
     * Create cache queries implementation.
     *
     * @param cctx Cache context.
     * @param prj Optional cache projection.
     */
    public GridCacheQueriesImpl(GridCacheContext<K, V> cctx, @Nullable GridCacheProjectionImpl<K, V> prj) {
        this.cctx = cctx;
        this.prj = prj;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheQueryMetrics> queryMetrics() {
        return cctx.queries().metrics();
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<K, V> createQuery(GridCacheQueryType type) {
        return cctx.queries().createQuery(type, filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<K, V> createQuery(GridCacheQueryType type, @Nullable Class<?> cls,
        @Nullable String clause) {
        return cctx.queries().createQuery(type, cls, clause, filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<K, V> createQuery(GridCacheQueryType type, @Nullable String clsName,
        @Nullable String clause) {
        return cctx.queries().createQuery(type, clsName, clause, filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public GridCacheFieldsQuery<K, V> createFieldsQuery(String clause) {
        return cctx.queries().createFieldsQuery(clause, filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridCacheReduceFieldsQuery<K, V, R1, R2> createReduceFieldsQuery(String clause) {
        return cctx.queries().createReduceFieldsQuery(clause, filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMetadata> sqlMetadata() throws GridException {
        return cctx.queries().sqlMetadata();
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheTransformQuery<K, V, T> createTransformQuery() {
        return cctx.queries().createTransformQuery(filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type) {
        return cctx.queries().createTransformQuery(type, filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type,
        @Nullable Class<?> cls, @Nullable String clause) {
        return cctx.queries().createTransformQuery(type, cls, clause, filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type,
        @Nullable String clsName, @Nullable String clause) {
        return cctx.queries().createTransformQuery(type, clsName, clause, filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery() {
        return cctx.queries().createReduceQuery(filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type) {
        return cctx.queries().createReduceQuery(type, filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type,
        @Nullable Class<?> cls, @Nullable String clause) {
        return cctx.queries().createReduceQuery(type, cls, clause, filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type,
        @Nullable String clsName, @Nullable String clause) {
        return cctx.queries().createReduceQuery(type, clsName, clause, filter(), flags());
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> rebuildIndexes(Class<?> cls) {
        return cctx.queries().rebuildIndexes(cls);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> rebuildAllIndexes() {
        return cctx.queries().rebuildAllIndexes();
    }

    /** {@inheritDoc} */
    @Override public GridCacheContinuousQuery<K, V> createContinuousQuery() {
        return cctx.continuousQueries().createQuery(filter());
    }

    /**
     * @return Optional projection filter.
     */
    @Nullable private GridPredicate<GridCacheEntry<K, V>> filter() {
        return prj == null ? null : (GridPredicate<GridCacheEntry<K, V>>)prj.predicate();
    }

    /**
     * @return Projection flags.
     */
    private Set<GridCacheFlag> flags() {
        return prj == null ? Collections.<GridCacheFlag>emptySet() : prj.flags();
    }
}
