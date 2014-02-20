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
 * Adapter for cache queries.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheQueryAdapter<K, V> extends GridCacheQueryBaseAdapter<K, V> implements GridCacheQuery<K, V> {
    /**
     * @param ctx Cache registry.
     * @param type Query type.
     * @param clause Query clause.
     * @param cls Query class.
     * @param clsName Query class name.
     * @param prjFilter Projection filter.
     * @param prjFlags Projection flags.
     */
    public GridCacheQueryAdapter(GridCacheContext<K, V> ctx, @Nullable GridCacheQueryType type, @Nullable String clause,
        @Nullable Class<?> cls, @Nullable String clsName, GridPredicate<GridCacheEntry<K, V>> prjFilter,
        Collection<GridCacheFlag> prjFlags) {
        super(ctx, type, clause, cls, clsName, prjFilter, prjFlags);
    }

    /**
     * @param ctx Cache registry.
     * @param qryId Query id.
     * @param type Query type.
     * @param clause Query clause.
     * @param cls Query class.
     * @param clsName Query class name.
     * @param prjFilter Projection filter.
     * @param prjFlags Projection flags.
     */
    public GridCacheQueryAdapter(GridCacheContext<K, V> ctx, int qryId, @Nullable GridCacheQueryType type,
        @Nullable String clause, @Nullable Class<?> cls, @Nullable String clsName,
        GridPredicate<GridCacheEntry<K, V>> prjFilter, Collection<GridCacheFlag> prjFlags) {
        super(ctx, qryId, type, clause, cls, clsName, prjFilter, prjFlags);
    }

    /**
     * @param qry Query to copy from (ignoring arguments).
     */
    private GridCacheQueryAdapter(GridCacheQueryAdapter<K, V> qry) {
        super(qry);
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<K, V> queryArguments(@Nullable Object[] args) {
        GridCacheQueryAdapter<K, V> cp = new GridCacheQueryAdapter<>(this);

        cp.arguments(args);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<K, V> closureArguments(@Nullable Object[] args) {
        GridCacheQueryAdapter<K, V> cp = new GridCacheQueryAdapter<>(this);

        cp.setClosureArguments(args);

        return cp;
    }

    /** {@inheritDoc} */
    @Override protected void registerClasses() throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Map.Entry<K, V>> executeSingle(GridProjection[] grid) {
        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing query for single result on nodes: " + toShortString(nodes)));

        return new SingleFuture<Map.Entry<K, V>>(nodes);
    }

    /** {@inheritDoc} */
    @Override public Map.Entry<K, V> executeSingleSync(GridProjection... grid) throws GridException {
        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing query for single result on nodes: " + toShortString(nodes)));

        Collection<Map.Entry<K, V>> res = executeSync(nodes, false, false, null, null);

        return F.first(res);
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryFuture<Map.Entry<K, V>> execute(GridProjection[] grid) {
        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing query on nodes: " + toShortString(nodes)));

        return execute(nodes, false, false, null, null);
    }

    /** {@inheritDoc} */
    @Override public Collection<Map.Entry<K, V>> executeSync(GridProjection... grid) throws GridException {
        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing query on nodes: " + toShortString(nodes)));

        return executeSync(nodes, false, false, null, null);
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> visit(GridPredicate<Map.Entry<K, V>> vis, GridProjection[] grid) {
        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing query with visitor on nodes: " + toShortString(nodes)));

        return execute(nodes, false, false, null, vis);
    }

    /** {@inheritDoc} */
    @Override public void visitSync(GridPredicate<Map.Entry<K, V>> vis, GridProjection... grid) throws GridException {
        Collection<GridNode> nodes = nodes(grid);

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing query with visitor on nodes: " + toShortString(nodes)));

        executeSync(nodes, false, false, null, vis);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        // No-op.
    }
}
