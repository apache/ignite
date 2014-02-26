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
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * TODO
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridCacheQueryAdapter<T> implements GridCacheQuery<T> {
    /** */
    protected final GridCacheContext<?, ?> ctx;

    /** */
    protected final GridPredicate<GridCacheEntry<?, ?>> prjPred;

    /** */
    private final GridCacheQueryType type;

    /** */
    private GridLogger log;

    /** */
    private int pageSize;

    /** */
    private long timeout;

    /** */
    private boolean keepAll;

    /** */
    private boolean incBackups;

    /** */
    private boolean dedup;

    /** */
    private GridProjection prj;

    /** */
    private GridBiPredicate<?, ?> filter;

    /**
     * @param ctx Context.
     * @param type Query type.
     * @param prjPred Cache projection filter.
     */
    protected GridCacheQueryAdapter(GridCacheContext<?, ?> ctx, GridCacheQueryType type,
        @Nullable GridPredicate<GridCacheEntry<?, ?>> prjPred) {
        assert ctx != null;
        assert type != null;

        this.ctx = ctx;
        this.type = type;
        this.prjPred = prjPred;

        log = ctx.logger(getClass());

        pageSize = DFLT_PAGE_SIZE;
        timeout = 0;
        keepAll = true;
        incBackups = false;
        dedup = false;
        prj = null;
        filter = null;
    }

    /**
     * @return cache projection filter.
     */
    public GridPredicate<GridCacheEntry<?, ?>> projectionFilter() {
        return prjPred;
    }

    /**
     * @return Type.
     */
    public GridCacheQueryType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> pageSize(int pageSize) {
        GridCacheQueryAdapter<T> cp = copy();

        cp.pageSize = pageSize;

        return cp;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> timeout(long timeout) {
        GridCacheQueryAdapter<T> cp = copy();

        cp.timeout = timeout;

        return cp;
    }

    /**
     * @return Timeout.
     */
    public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> keepAll(boolean keepAll) {
        GridCacheQueryAdapter<T> cp = copy();

        cp.keepAll = keepAll;

        return cp;
    }

    /**
     * @return Keep all flag.
     */
    public boolean keepAll() {
        return keepAll;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> includeBackups(boolean incBackups) {
        GridCacheQueryAdapter<T> cp = copy();

        cp.incBackups = incBackups;

        return cp;
    }

    /**
     * @return Include backups.
     */
    public boolean includeBackups() {
        return incBackups;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> enableDedup(boolean dedup) {
        GridCacheQueryAdapter<T> cp = copy();

        cp.dedup = dedup;

        return cp;
    }

    /**
     * @return Enable dedup flag.
     */
    public boolean enableDedup() {
        return dedup;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> projection(GridProjection prj) {
        GridCacheQueryAdapter<T> cp = copy();

        cp.prj = prj;

        return cp;
    }

    /**
     * @return Grid projection.
     */
    public GridProjection projection() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCacheQuery<T> remoteFilter(GridBiPredicate<K, V> filter) {
        GridCacheQueryAdapter<T> cp = copy();

        cp.filter = filter;

        return cp;
    }

    /**
     * @return Key-value filter.
     */
    public <K, V> GridBiPredicate<K, V> remoteFilter() {
        return (GridBiPredicate<K, V>)filter;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryFuture<T> execute(@Nullable Object... args) {
        return execute(null, null, args);
    }

    /** {@inheritDoc} */
    @Override public <R> GridCacheQueryFuture<R> execute(GridReducer <T, R> rmtReducer, @Nullable Object... args) {
        return execute(rmtReducer, null, args);
    }

    /** {@inheritDoc} */
    @Override public <R> GridCacheQueryFuture<R> execute(GridClosure<T, R> rmtTransform, @Nullable Object... args) {
        return execute(null, rmtTransform, args);
    }

    /**
     * @param rmtReducer Optional reducer.
     * @param rmtTransform Optional transformer.
     * @param args Arguments.
     * @return Future.
     */
    private <R> GridCacheQueryFuture<R> execute(@Nullable GridReducer<T, R> rmtReducer,
        @Nullable GridClosure<T, R> rmtTransform, @Nullable Object... args) {
        Collection<GridNode> nodes = nodes();

        if (log.isDebugEnabled())
            log.debug("Executing query [query=" + this + ", nodes=" + nodes + ']');

        if (ctx.deploymentEnabled()) {
            try {
                ctx.deploy().registerClasses(filter);
                ctx.deploy().registerClasses(args);

                registerClasses();
            }
            catch (GridException e) {
                return new GridCacheQueryErrorFuture<>(ctx.kernalContext(), e);
            }
        }

        GridCacheQueryManager qryMgr = ctx.queries();

        return null;
    }

    /**
     * @return Nodes to execute on.
     */
    private Collection<GridNode> nodes() {
        Collection<GridNode> nodes = CU.allNodes(ctx);

        if (prj == null) {
            if (ctx.isReplicated())
                return Collections.singletonList(ctx.localNode());

            return nodes;
        }

        return F.view(nodes, new P1<GridNode>() {
            @Override public boolean apply(GridNode e) {
                return prj.node(e.id()) != null;
            }
        });
    }

    /**
     * Registers classes.
     *
     * @throws GridException In case of error.
     */
    protected void registerClasses() throws GridException {
        // No-op.
    }

    /**
     * @return Copy of this query.
     */
    private GridCacheQueryAdapter<T> copy() {
        GridCacheQueryAdapter<T> cp = copy0();

        cp.log = log;
        cp.pageSize = pageSize;
        cp.timeout = timeout;
        cp.keepAll = keepAll;
        cp.incBackups = incBackups;
        cp.dedup = dedup;
        cp.prj = prj;
        cp.filter = filter;

        return cp;
    }

    /**
     * @return New instance.
     */
    protected abstract GridCacheQueryAdapter<T> copy0();
}
