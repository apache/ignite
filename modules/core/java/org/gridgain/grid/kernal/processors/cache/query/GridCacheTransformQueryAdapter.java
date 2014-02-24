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
 * Adapter for transforming cache queries.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheTransformQueryAdapter<K, V, T>
    extends GridCacheQueryBaseAdapter<K, V, GridCacheTransformQuery<K, V, T>>
    implements GridCacheTransformQuery<K, V, T> {
    /** Transformation closure. */
    private volatile GridClosure<V, T> trans;

    /**
     * @param ctx Cache registry.
     * @param type Query type.
     * @param clause Query clause.
     * @param cls Query class.
     * @param clsName Query class name.
     * @param prjFilter Projection filter.
     * @param prjFlags Projection flags.
     */
    public GridCacheTransformQueryAdapter(GridCacheContext<K, V> ctx, GridCacheQueryType type, String clause,
        Class<?> cls, String clsName, GridPredicate<GridCacheEntry<K, V>> prjFilter,
        Collection<GridCacheFlag> prjFlags) {
        super(ctx, type, clause, cls, clsName, prjFilter, prjFlags);
    }

    /**
     * @param qry Query to copy from (ignoring arguments).
     */
    @SuppressWarnings({"TypeMayBeWeakened"})
    private GridCacheTransformQueryAdapter(GridCacheTransformQueryAdapter<K, V, T> qry) {
        super(qry);

        trans = qry.trans;
    }

    /** {@inheritDoc} */
    @Override public GridCacheTransformQuery<K, V, T> queryArguments(@Nullable Object[] args) {
        GridCacheTransformQueryAdapter<K, V, T> cp = new GridCacheTransformQueryAdapter<>(this);

        cp.arguments(args);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Map.Entry<K, T>> executeSingle() {
        if (trans == null) {
            GridFutureAdapter<Map.Entry<K, T>> err = new GridFutureAdapter<>(cctx.kernalContext());

            err.onDone(new GridException("Transformer must be set."));

            return err;
        }

        Collection<GridNode> nodes = nodes();

        if (qryLog.isDebugEnabled())
            qryLog.debug("Executing transform query for single result " + toShortString(nodes));

        return new SingleFuture<Map.Entry<K, T>>(nodes);
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryFuture<Map.Entry<K, T>> execute() {
        if (trans == null)
            return new GridCacheErrorQueryFuture<>
                (cctx.kernalContext(), new GridException("Transformer must be set for transform query."));

        Collection<GridNode> nodes = nodes();

        if (qryLog.isDebugEnabled())
            qryLog.debug(U.compact("Executing transform query " + toShortString(nodes)));

        return execute(nodes, false, false, null, null);
    }

    /** {@inheritDoc} */
    @Override protected void registerClasses() throws GridException {
        assert cctx.deploymentEnabled();

        context().deploy().registerClass(trans);
    }

    /** {@inheritDoc} */
    @Override public GridCacheTransformQuery<K, V, T> remoteTransformer(GridClosure<V, T> trans) {
        GridCacheTransformQueryAdapter<K, V, T> cp = copy();

        cp.trans = trans;

        return cp;
    }

    /**
     * @return Transformer.
     */
    public GridClosure<V, T> remoteTransformer() {
        return trans;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheTransformQueryAdapter<K, V, T> copy() {
        return new GridCacheTransformQueryAdapter<>(this);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        // No-op.
    }
}
