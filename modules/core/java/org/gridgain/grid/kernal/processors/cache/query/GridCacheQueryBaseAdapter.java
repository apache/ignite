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
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheConfiguration.*;
import static org.gridgain.grid.cache.GridCacheFlag.*;

/**
 * Query adapter.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridCacheQueryBaseAdapter<K, V, T extends GridCacheQueryBase> implements
    GridCacheQueryBase<K, V, T> {
    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Sequence of query id. */
    protected static final AtomicInteger idGen = new AtomicInteger();

    /** Query id.  */
    protected final int id;

    /** */
    protected final GridCacheContext<K, V> cctx;

    /** Query activity logger. */
    protected final GridLogger qryLog;

    /** Default logger. */
    protected final GridLogger log;

    /** */
    private final GridCacheQueryType type;

    /** */
    private volatile String clause;

    /** */
    private volatile String clsName;

    /** */
    private volatile Class<?> cls;

    /** */
    private volatile GridPredicate<K> rmtKeyFilter;

    /** */
    private volatile GridPredicate<V> rmtValFilter;

    /** */
    private volatile Runnable beforeCb;

    /** */
    private volatile Runnable afterCb;

    /** */
    protected volatile GridProjection gridPrj;

    /** */
    private volatile GridPredicate<GridCacheEntry<K, V>> prjFilter;

    /** */
    private volatile int pageSize = GridCacheQuery.DFLT_PAGE_SIZE;

    /** */
    private volatile long timeout;

    /** */
    private volatile Object[] args;

    /** */
    private volatile boolean keepAll = true;

    /** False by default. */
    private volatile boolean incBackups;

    /** False by default. */
    private volatile boolean dedup;

    /** */
    private final boolean readThrough;

    /** */
    private final boolean clone;

    /** Query metrics.*/
    private volatile GridCacheQueryMetricsAdapter metrics;

    /** Sealed flag. Query cannot be modified once it's set to true. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private boolean sealed;

    /** */
    protected final Object mux = new Object();

    /**
     * @param cctx Cache registry.
     * @param type Query type.
     * @param clause Query clause.
     * @param cls Query class.
     * @param clsName Query class name.
     * @param prjFilter Projection filter.
     * @param prjFlags Projection flags.
     */
    protected GridCacheQueryBaseAdapter(GridCacheContext<K, V> cctx, @Nullable GridCacheQueryType type,
        @Nullable String clause, @Nullable Class<?> cls, @Nullable String clsName,
        GridPredicate<GridCacheEntry<K, V>> prjFilter, Collection<GridCacheFlag> prjFlags) {
        this(cctx, -1, type, clause, cls, clsName, prjFilter, prjFlags);
    }

    /**
     * @param cctx Cache registry.
     * @param qryId Query id. If it less than {@code 0} new query id will be created.
     * @param type Query type.
     * @param clause Query clause.
     * @param cls Query class.
     * @param clsName Query class name.
     * @param prjFilter Projection filter.
     * @param prjFlags Projection flags.
     */
    protected GridCacheQueryBaseAdapter(GridCacheContext<K, V> cctx, int qryId, @Nullable GridCacheQueryType type,
        @Nullable String clause, @Nullable Class<?> cls, @Nullable String clsName,
        GridPredicate<GridCacheEntry<K, V>> prjFilter, Collection<GridCacheFlag> prjFlags) {
        assert cctx != null;

        this.cctx = cctx;
        this.type = type;
        this.clause = clause;
        this.cls = U.box(cls);
        this.clsName = clsName != null ? clsName : this.cls != null ? this.cls.getName() : null;
        this.prjFilter = prjFilter;

        validateSql();

        log = U.logger(cctx.kernalContext(), logRef, GridCacheQueryBaseAdapter.class);

        qryLog = cctx.logger(DFLT_QUERY_LOGGER_NAME);

        clone = prjFlags.contains(CLONE);

        metrics = new GridCacheQueryMetricsAdapter(new GridCacheQueryMetricsKey(type, this.clsName, clause));

        id = qryId < 0 ? idGen.incrementAndGet() : qryId;

        timeout = cctx.config().getDefaultQueryTimeout();

        readThrough = false;
    }

    /**
     * @param qry Query to copy from.
     */
    protected GridCacheQueryBaseAdapter(GridCacheQueryBaseAdapter<K, V, T> qry) {
        cctx = qry.cctx;
        type = qry.type;
        clause = qry.clause;
        prjFilter = qry.prjFilter;
        clsName = qry.clsName;
        cls = qry.cls;
        rmtKeyFilter = qry.rmtKeyFilter;
        rmtValFilter = qry.rmtValFilter;
        beforeCb = qry.beforeCb;
        afterCb = qry.afterCb;
        args = qry.args;
        pageSize = qry.pageSize;
        timeout = qry.timeout;
        keepAll = qry.keepAll;
        incBackups = qry.incBackups;
        dedup = qry.dedup;
        readThrough = qry.readThrough;
        clone = qry.clone;
        gridPrj = qry.gridPrj;

        log = U.logger(cctx.kernalContext(), logRef, GridCacheQueryBaseAdapter.class);

        qryLog = cctx.kernalContext().config().getGridLogger().getLogger(DFLT_QUERY_LOGGER_NAME);

        metrics = qry.metrics;

        id = qry.id;
    }

    /**
     * Validates sql clause.
     */
    private void validateSql() {
        if (type == GridCacheQueryType.SQL) {
            if (clause == null)
                throw new IllegalArgumentException("SQL string cannot be null for query.");

            if (clause.startsWith("where"))
                throw new IllegalArgumentException("SQL string cannot start with 'where' ('where' keyword is assumed). " +
                    "Valid examples: \"col1 like '%val1%'\" or \"from MyClass1 c1, MyClass2 c2 where c1.col1 = c2.col1 " +
                    "and c1.col2 like '%val2%'");
        }
    }

    /**
     * @return Context.
     */
    protected GridCacheContext<K, V> context() {
        return cctx;
    }

    /**
     * Checks if metrics should be recreated and does it in this case.
     */
    private void checkMetrics() {
        synchronized (mux) {
            if (!F.eq(metrics.clause(), clause) || !F.eq(metrics.className(), clsName))
                metrics = new GridCacheQueryMetricsAdapter(new GridCacheQueryMetricsKey(type, clsName, clause));
        }
    }

    /** {@inheritDoc} */
    @Override public int id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public String clause() {
        return clause;
    }

    /** {@inheritDoc} */
    @Override public void clause(String clause) {
        synchronized (mux) {
            checkSealed();

            this.clause = clause;

            validateSql();

            checkMetrics();
        }
    }

    /** {@inheritDoc} */
    @Override public int pageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override public void pageSize(int pageSize) {
        synchronized (mux) {
            checkSealed();

            this.pageSize = pageSize < 1 ? GridCacheQuery.DFLT_PAGE_SIZE : pageSize;
        }
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public void timeout(long timeout) {
        synchronized (mux) {
            checkSealed();

            this.timeout = timeout;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean keepAll() {
        return keepAll;
    }

    /** {@inheritDoc} */
    @Override public void keepAll(boolean keepAll) {
        synchronized (mux) {
            checkSealed();

            this.keepAll = keepAll;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean includeBackups() {
        return incBackups;
    }

    /** {@inheritDoc} */
    @Override public void includeBackups(boolean incBackups) {
        synchronized (mux) {
            checkSealed();

            this.incBackups = incBackups;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean enableDedup() {
        return dedup;
    }

    /** {@inheritDoc} */
    @Override public void enableDedup(boolean dedup) {
        synchronized (mux) {
            checkSealed();

            this.dedup = dedup;
        }
    }

    /**
     * @return Clone values flag.
     */
    public boolean cloneValues() {
        return clone;
    }

    /** {@inheritDoc} */
    @Override public String className() {
        return clsName;
    }

    /** {@inheritDoc} */
    @Override public void className(String clsName) {
        synchronized (mux) {
            checkSealed();

            this.clsName = clsName;

            checkMetrics();
        }
    }

    /**
     * Gets query class.
     *
     * @param ldr Class loader.
     * @return Query class.
     * @throws ClassNotFoundException Thrown if class not found.
     */
    public Class<? extends V> queryClass(ClassLoader ldr) throws ClassNotFoundException {
        if (cls == null)
            cls = U.forName(clsName, ldr);

        return (Class<? extends V>)cls;
    }

    /**
     * @return Remote key filter.
     */
    public GridPredicate<K> remoteKeyFilter() {
        return rmtKeyFilter;
    }

    /**
     *
     * @param rmtKeyFilter Remote key filter
     */
    @Override public T remoteKeyFilter(GridPredicate<K> rmtKeyFilter) {
        GridCacheQueryBaseAdapter<K, V, T> cp = copy();

        cp.rmtKeyFilter = rmtKeyFilter;

        return (T)cp;
    }

    /**
     * @return Remote value filter.
     */
    public GridPredicate<V> remoteValueFilter() {
        return rmtValFilter;
    }

    /**
     * @param rmtValFilter Remote value filter.
     */
    @Override public T remoteValueFilter(GridPredicate<V> rmtValFilter) {
        GridCacheQueryBaseAdapter<K, V, T> cp = copy();

        cp.rmtValFilter = rmtValFilter;

        return (T)cp;
    }

    /**
     * @return Before execution callback.
     */
    public Runnable beforeCallback() {
        return beforeCb;
    }

    /**
     * @param beforeCb Before execution callback.
     */
    @Override public T beforeExecution(Runnable beforeCb) {
        GridCacheQueryBaseAdapter<K, V, T> cp = copy();

        cp.beforeCb = beforeCb;

        return (T)cp;
    }

    /**
     * @return After execution callback.
     */
    public Runnable afterCallback() {
        return afterCb;
    }

    /**
     * @param afterCb After execution callback.
     */
    @Override public T afterExecution(Runnable afterCb) {
        GridCacheQueryBaseAdapter<K, V, T> cp = copy();

        cp.afterCb = afterCb;

        return (T)cp;
    }

    /**
     * @param rmtKeyFilter Key filter.
     * @param rmtValFilter Value filter.
     * @param beforeCb Before callback.
     * @param afterCb After callback.
     * @param args Arguments.
     * @param incBackups Include backups flag.
     */
    void init(GridPredicate<K> rmtKeyFilter, GridPredicate<V> rmtValFilter, Runnable beforeCb, Runnable afterCb,
        Object[] args, boolean incBackups) {
        this.rmtKeyFilter = rmtKeyFilter;
        this.rmtValFilter = rmtValFilter;
        this.beforeCb = beforeCb;
        this.afterCb = afterCb;
        this.args = args;
        this.incBackups = incBackups;
    }

    /**
     * @return Projection filter.
     */
    public GridPredicate<GridCacheEntry<K, V>> projectionFilter() {
        return prjFilter;
    }

    /**
     * @param prjFilter Projection filter.
     */
    public void projectionFilter(GridPredicate<GridCacheEntry<K, V>> prjFilter) {
        synchronized (mux) {
            checkSealed();

            this.prjFilter = prjFilter;
        }
    }

    /** {@inheritDoc} */
    @Override public T projection(GridProjection prj) {
        GridCacheQueryBaseAdapter<K, V, T> cp = copy();

        cp.gridPrj = prj;

        return (T)cp;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridProjection projection() {
        return gridPrj;
    }

    /**
     * @param args Arguments.
     */
    public void arguments(@Nullable Object[] args) {
        this.args = args;
    }

    /**
     * @return Arguments.
     */
    public Object[] arguments() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryMetrics metrics() {
        return metrics;
    }

    /**
     * @throws GridException In case of error.
     */
    protected abstract void registerClasses() throws GridException;

    /**
     * @param nodes Nodes.
     * @param single {@code true} if single result requested, {@code false} if multiple.
     * @param rmtRdcOnly {@code true} for reduce query when using remote reducer only,
     *      otherwise it is always {@code false}.
     * @param pageLsnr Page listener.
     * @param vis Visitor predicate.
     * @param <R> Result type.
     * @return Future.
     */
    protected <R> GridCacheQueryFuture<R> execute(Collection<GridNode> nodes, boolean single, boolean rmtRdcOnly,
        @Nullable GridBiInClosure<UUID, Collection<R>> pageLsnr, @Nullable GridPredicate<?> vis) {
        // Seal the query.
        seal();

        if (log.isDebugEnabled())
            log.debug("Executing query [query=" + this + ", nodes=" + nodes + ']');

        if (cctx.deploymentEnabled()) {
            try {
                cctx.deploy().registerClasses(cls, rmtKeyFilter, rmtValFilter, beforeCb, afterCb, prjFilter);

                registerClasses();

                cctx.deploy().registerClasses(args);
            }
            catch (GridException e) {
                return new GridCacheErrorQueryFuture<>(cctx.kernalContext(), e);
            }
        }

        GridCacheQueryManager<K, V> qryMgr = cctx.queries();

        assert qryMgr != null;

        return nodes.size() == 1 && nodes.iterator().next().equals(cctx.discovery().localNode()) ?
            qryMgr.queryLocal((GridCacheQueryBaseAdapter<K, V, GridCacheQueryBase>)this, single, rmtRdcOnly, pageLsnr,
                vis) :
            qryMgr.queryDistributed((GridCacheQueryBaseAdapter<K, V, GridCacheQueryBase>)this, nodes, single,
                rmtRdcOnly, pageLsnr, vis);
    }

    /**
     * Check if this query is sealed.
     */
    protected void checkSealed() {
        assert Thread.holdsLock(mux);

        if (sealed)
            throw new IllegalStateException("Query cannot be modified after first execution: " + this);
    }

    /**
     * Seal this query so that it can't be modified.
     */
    protected void seal() {
        synchronized (mux) {
            sealed = true;
        }
    }

    /**
     * @param res Query result.
     * @param err Error or {@code null} if query executed successfully.
     * @param startTime Start time.
     * @param duration Duration.
     */
    public void onExecuted(Object res, Throwable err, long startTime, long duration) {
        boolean fail = err != null;

        // Update own metrics.
        metrics.onQueryExecute(startTime, duration, fail);

        // Update metrics in query manager.
        cctx.queries().onMetricsUpdate(metrics, startTime, duration, fail);

        if (qryLog.isDebugEnabled())
            qryLog.debug("Query execution finished [qry=" + this + ", startTime=" + startTime +
                ", duration=" + duration + ", fail=" + fail + ", res=" + res + ']');
    }

    /**
     * @return Predicates for nodes.
     */
    protected Collection<GridNode> nodes() {
        Collection<GridNode> nodes = CU.allNodes(cctx);

        if (gridPrj == null) {
            if (cctx.isReplicated())
                return Collections.singletonList(cctx.localNode());

            return nodes;
        }

        return F.view(
            nodes,
            new P1<GridNode>() {
                @Override
                public boolean apply(GridNode e) {
                    return gridPrj.node(e.id()) != null;
                }
            });
    }

    /**
     * @param nodes Nodes.
     * @return Short representation of query.
     */
    public String toShortString(Collection<? extends GridNode> nodes) {
        return "[id=" + id + ", clause=" + clause + ", type=" + type + ", clsName=" + clsName + ", nodes=" +
            U.toShortString(nodes) + ']';
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryBaseAdapter.class, this);
    }

    /**
     * @return Copy of this query.
     */
    protected abstract GridCacheQueryBaseAdapter<K, V, T> copy();

    /**
     * Future for single query result.
     *
     * @param <R> Result type.
     */
    protected class SingleFuture<R> extends GridFutureAdapter<R> {
        /** */
        private GridCacheQueryFuture<R> fut;

        /**
         * Required by {@link Externalizable}.
         */
        public SingleFuture() {
            super(cctx.kernalContext());
        }

        /**
         * @param nodes Nodes.
         */
        SingleFuture(Collection<GridNode> nodes) {
            super(cctx.kernalContext());

            fut = execute(nodes, true, false, new CI2<UUID, Collection<R>>() {
                @Override public void apply(UUID uuid, Collection<R> pageData) {
                    try {
                        if (!F.isEmpty(pageData))
                            onDone(pageData.iterator().next());
                    }
                    catch (Throwable e) {
                        onDone(e);
                    }
                }
            }, null);

            fut.listenAsync(new CI1<GridFuture<Collection<R>>>() {
                @Override public void apply(GridFuture<Collection<R>> t) {
                    try {
                        if (!fut.hasNextX())
                            onDone(null, null);
                    }
                    catch (Throwable e) {
                        onDone(e);
                    }
                }
            });
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws GridException {
            if (onCancelled()) {
                fut.cancel();

                return true;
            }

            return false;
        }
    }
}
