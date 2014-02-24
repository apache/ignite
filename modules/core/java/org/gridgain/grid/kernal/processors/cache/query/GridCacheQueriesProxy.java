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
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Per-projection queries object returned to user.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheQueriesProxy<K, V> implements GridCacheQueries<K, V> {
    /** Cache gateway. */
    private GridCacheGateway<K, V> gate;

    /** Cache projection, can be {@code null}. */
    private GridCacheProjectionImpl<K, V> prj;

    /**  */
    private GridCacheQueries<K, V> delegate;

    /**
     * Create cache queries implementation.
     *
     * @param cctx Cache context.
     * @param prj Optional cache projection.
     * @param delegate Delegate object.
     */
    public GridCacheQueriesProxy(GridCacheContext<K, V> cctx, @Nullable GridCacheProjectionImpl<K, V> prj,
        GridCacheQueries<K, V> delegate) {
        this.prj = prj;
        this.delegate = delegate;

        gate = cctx.gate();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheQueryMetrics> queryMetrics() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.queryMetrics();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<K, V> createQuery(GridCacheQueryType type) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createQuery(type);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<K, V> createQuery(GridCacheQueryType type, @Nullable Class<?> cls,
        @Nullable String clause) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createQuery(type, cls, clause);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<K, V> createQuery(GridCacheQueryType type, @Nullable String clsName,
        @Nullable String clause) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createQuery(type, clsName, clause);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheFieldsQuery<K, V> createFieldsQuery(String clause) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createFieldsQuery(clause);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridCacheReduceFieldsQuery<K, V, R1, R2> createReduceFieldsQuery(String clause) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createReduceFieldsQuery(clause);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMetadata> sqlMetadata() throws GridException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.sqlMetadata();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheTransformQuery<K, V, T> createTransformQuery() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createTransformQuery();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createTransformQuery(type);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type,
        @Nullable Class<?> cls, @Nullable String clause) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createTransformQuery(type, cls, clause);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheTransformQuery<K, V, T> createTransformQuery(GridCacheQueryType type,
        @Nullable String clsName, @Nullable String clause) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createTransformQuery(type, clsName, clause);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createReduceQuery();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createReduceQuery(type);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type,
        @Nullable Class<?> cls, @Nullable String clause) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createReduceQuery(type, cls, clause);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <R1, R2> GridCacheReduceQuery<K, V, R1, R2> createReduceQuery(GridCacheQueryType type,
        @Nullable String clsName, @Nullable String clause) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createReduceQuery(type, clsName, clause);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> rebuildIndexes(Class<?> cls) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.rebuildIndexes(cls);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> rebuildAllIndexes() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.rebuildAllIndexes();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheContinuousQuery<K, V> createContinuousQuery() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createContinuousQuery();
        }
        finally {
            gate.leave(prev);
        }
    }
}
