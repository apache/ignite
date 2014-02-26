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
    /** */
    private GridCacheGateway<K, V> gate;

    /** */
    private GridCacheProjectionImpl<K, V> prj;

    /** */
    private GridCacheQueries<K, V> delegate;

    /**
     * Create cache queries implementation.
     *
     * @param ctx Сontext.
     * @param prj Optional cache projection.
     * @param delegate Delegate object.
     */
    public GridCacheQueriesProxy(GridCacheContext<K, V> ctx, @Nullable GridCacheProjectionImpl<K, V> prj,
        GridCacheQueries<K, V> delegate) {
        assert ctx != null;
        assert delegate != null;

        gate = ctx.gate();

        this.prj = prj;
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<Map.Entry<K, V>> createSqlQuery(Class<?> cls, String clause) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createSqlQuery(cls, clause);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<List<?>> createSqlFieldsQuery(String qry) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createSqlFieldsQuery(qry);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<Map.Entry<K, V>> createFullTextQuery(Class<?> cls, String search) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createFullTextQuery(cls, search);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<Map.Entry<K, V>> createScanQuery() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createScanQuery();
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
}
