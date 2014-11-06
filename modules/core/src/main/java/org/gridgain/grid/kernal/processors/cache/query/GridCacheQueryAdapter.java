/* @java.file.header */

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
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.cache.query.GridCacheQueryType.*;

/**
 * Query adapter.
 */
public class GridCacheQueryAdapter<T> implements GridCacheQuery<T> {
    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final GridPredicate<GridCacheEntry<Object, Object>> prjPred;

    /** */
    private final GridCacheQueryType type;

    /** */
    private final GridLogger log;

    /** Class name in case of portable query. */
    private final String clsName;

    /** */
    private final String clause;

    /** */
    private final GridBiPredicate<Object, Object> filter;

    /** */
    private final boolean incMeta;

    /** */
    private volatile GridCacheQueryMetricsAdapter metrics;

    /** */
    private volatile int pageSize;

    /** */
    private volatile long timeout;

    /** */
    private volatile boolean keepAll;

    /** */
    private volatile boolean incBackups;

    /** */
    private volatile boolean dedup;

    /** */
    private volatile GridProjection prj;

    /** */
    private boolean keepPortable;

    /** */
    private UUID subjId;

    /** */
    private int taskHash;

    /**
     * @param cctx Context.
     * @param type Query type.
     * @param clsName Class name.
     * @param clause Clause.
     * @param filter Scan filter.
     * @param incMeta Include metadata flag.
     * @param keepPortable Keep portable flag.
     * @param prjPred Cache projection filter.
     */
    public GridCacheQueryAdapter(GridCacheContext<?, ?> cctx,
        GridCacheQueryType type,
        @Nullable GridPredicate<GridCacheEntry<Object, Object>> prjPred,
        @Nullable String clsName,
        @Nullable String clause,
        @Nullable GridBiPredicate<Object, Object> filter,
        boolean incMeta,
        boolean keepPortable) {
        assert cctx != null;
        assert type != null;

        this.cctx = cctx;
        this.type = type;
        this.clsName = clsName;
        this.clause = clause;
        this.prjPred = prjPred;
        this.filter = filter;
        this.incMeta = incMeta;
        this.keepPortable = keepPortable;

        log = cctx.logger(getClass());

        pageSize = DFLT_PAGE_SIZE;
        timeout = 0;
        keepAll = true;
        incBackups = false;
        dedup = false;
        prj = null;

        metrics = new GridCacheQueryMetricsAdapter();
    }

    /**
     * @param cctx Context.
     * @param prjPred Cache projection filter.
     * @param type Query type.
     * @param log Logger.
     * @param pageSize Page size.
     * @param timeout Timeout.
     * @param keepAll Keep all flag.
     * @param incBackups Include backups flag.
     * @param dedup Enable dedup flag.
     * @param prj Grid projection.
     * @param filter Key-value filter.
     * @param clsName Class name.
     * @param clause Clause.
     * @param incMeta Include metadata flag.
     * @param keepPortable Keep portable flag.
     * @param subjId Security subject ID.
     * @param taskHash Task hash.
     */
    public GridCacheQueryAdapter(GridCacheContext<?, ?> cctx,
        GridPredicate<GridCacheEntry<Object, Object>> prjPred,
        GridCacheQueryType type,
        GridLogger log,
        int pageSize,
        long timeout,
        boolean keepAll,
        boolean incBackups,
        boolean dedup,
        GridProjection prj,
        GridBiPredicate<Object, Object> filter,
        @Nullable String clsName,
        String clause,
        boolean incMeta,
        boolean keepPortable,
        UUID subjId,
        int taskHash) {
        this.cctx = cctx;
        this.prjPred = prjPred;
        this.type = type;
        this.log = log;
        this.pageSize = pageSize;
        this.timeout = timeout;
        this.keepAll = keepAll;
        this.incBackups = incBackups;
        this.dedup = dedup;
        this.prj = prj;
        this.filter = filter;
        this.clsName = clsName;
        this.clause = clause;
        this.incMeta = incMeta;
        this.keepPortable = keepPortable;
        this.subjId = subjId;
        this.taskHash = taskHash;
    }

    /**
     * @return cache projection filter.
     */
    @Nullable public GridPredicate<GridCacheEntry<Object, Object>> projectionFilter() {
        return prjPred;
    }

    /**
     * @return Type.
     */
    public GridCacheQueryType type() {
        return type;
    }

    /**
     * @return Class name.
     */
    @Nullable public String queryClassName() {
        return clsName;
    }

    /**
     * @return Clause.
     */
    @Nullable public String clause() {
        return clause;
    }

    /**
     * @return Include metadata flag.
     */
    public boolean includeMetadata() {
        return incMeta;
    }

    /**
     * @return {@code True} if portable should not be deserialized.
     */
    public boolean keepPortable() {
        return keepPortable;
    }

    /**
     * Forces query to keep portable object representation even if query was created on plain projection.
     *
     * @param keepPortable Keep portable flag.
     */
    public void keepPortable(boolean keepPortable) {
        this.keepPortable = keepPortable;
    }

    /**
     * @return Security subject ID.
     */
    public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task hash.
     */
    public int taskHash() {
        return taskHash;
    }

    /**
     * @param subjId Security subject ID.
     */
    public void subjectId(UUID subjId) {
        this.subjId = subjId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> pageSize(int pageSize) {
        A.ensure(pageSize > 0, "pageSize > 0");

        this.pageSize = pageSize;

        return this;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> timeout(long timeout) {
        A.ensure(timeout >= 0, "timeout >= 0");

        this.timeout = timeout;

        return this;
    }

    /**
     * @return Timeout.
     */
    public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> keepAll(boolean keepAll) {
        this.keepAll = keepAll;

        return this;
    }

    /**
     * @return Keep all flag.
     */
    public boolean keepAll() {
        return keepAll;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> includeBackups(boolean incBackups) {
        this.incBackups = incBackups;

        return this;
    }

    /**
     * @return Include backups.
     */
    public boolean includeBackups() {
        return incBackups;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> enableDedup(boolean dedup) {
        this.dedup = dedup;

        return this;
    }

    /**
     * @return Enable dedup flag.
     */
    public boolean enableDedup() {
        return dedup;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<T> projection(GridProjection prj) {
        this.prj = prj;

        return this;
    }

    /**
     * @return Grid projection.
     */
    public GridProjection projection() {
        return prj;
    }

    /**
     * @return Key-value filter.
     */
    @Nullable public <K, V> GridBiPredicate<K, V> scanFilter() {
        return (GridBiPredicate<K, V>)filter;
    }

    /**
     * @throws GridException If query is invalid.
     */
    public void validate() throws GridException {
        if (type != SCAN && !cctx.config().isQueryIndexEnabled())
            throw new GridException("Indexing is disabled for cache: " + cctx.cache().name());
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
        metrics.onQueryExecute(duration, fail);

        // Update metrics in query manager.
        cctx.queries().onMetricsUpdate(duration, fail);

        if (log.isDebugEnabled())
            log.debug("Query execution finished [qry=" + this + ", startTime=" + startTime +
                ", duration=" + duration + ", fail=" + fail + ", res=" + res + ']');
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

    @Override public GridCacheQueryMetrics metrics() {
        return metrics.copy();
    }

    @Override public void resetMetrics() {
        metrics = new GridCacheQueryMetricsAdapter();
    }

    /**
     * @param rmtReducer Optional reducer.
     * @param rmtTransform Optional transformer.
     * @param args Arguments.
     * @return Future.
     */
    @SuppressWarnings("IfMayBeConditional")
    private <R> GridCacheQueryFuture<R> execute(@Nullable GridReducer<T, R> rmtReducer,
        @Nullable GridClosure<T, R> rmtTransform, @Nullable Object... args) {
        Collection<GridNode> nodes = nodes();

        cctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        if (log.isDebugEnabled())
            log.debug("Executing query [query=" + this + ", nodes=" + nodes + ']');

        if (cctx.deploymentEnabled()) {
            try {
                cctx.deploy().registerClasses(filter, rmtReducer, rmtTransform);
                cctx.deploy().registerClasses(args);
            }
            catch (GridException e) {
                return new GridCacheQueryErrorFuture<>(cctx.kernalContext(), e);
            }
        }

        if (subjId == null)
            subjId = cctx.localNodeId();

        taskHash = cctx.kernalContext().job().currentTaskNameHash();

        GridCacheQueryBean bean = new GridCacheQueryBean(this, (GridReducer<Object, Object>)rmtReducer,
            (GridClosure<Object, Object>)rmtTransform, args);

        GridCacheQueryManager qryMgr = cctx.queries();

        boolean loc = nodes.size() == 1 && F.first(nodes).id().equals(cctx.localNodeId());

        if (type == SQL_FIELDS)
            return (GridCacheQueryFuture<R>)(loc ? qryMgr.queryFieldsLocal(bean) :
                qryMgr.queryFieldsDistributed(bean, nodes));
        else
            return (GridCacheQueryFuture<R>)(loc ? qryMgr.queryLocal(bean) : qryMgr.queryDistributed(bean, nodes));
    }

    /**
     * @return Nodes to execute on.
     */
    private Collection<GridNode> nodes() {
        Collection<GridNode> nodes = CU.allNodes(cctx);

        if (prj == null) {
            if (cctx.isReplicated())
                return Collections.singletonList(cctx.localNode());

            return nodes;
        }

        return F.view(nodes, new P1<GridNode>() {
            @Override public boolean apply(GridNode e) {
                return prj.node(e.id()) != null;
            }
        });
    }
}
