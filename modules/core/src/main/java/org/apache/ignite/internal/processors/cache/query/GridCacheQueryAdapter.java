/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;

import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.*;

/**
 * Query adapter.
 */
public class GridCacheQueryAdapter<T> implements CacheQuery<T> {
    /** Is local node predicate. */
    private static final IgnitePredicate<ClusterNode> IS_LOC_NODE = new IgnitePredicate<ClusterNode>() {
        @Override public boolean apply(ClusterNode n) {
            return n.isLocal();
        }
    };

    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final GridCacheQueryType type;

    /** */
    private final IgniteLogger log;

    /** Class name in case of portable query. */
    private final String clsName;

    /** */
    private final String clause;

    /** */
    private final IgniteBiPredicate<Object, Object> filter;

    /** Partition. */
    private Integer part;

    /** */
    private final boolean incMeta;

    /** */
    private volatile GridCacheQueryMetricsAdapter metrics;

    /** */
    private volatile int pageSize = Query.DFLT_PAGE_SIZE;

    /** */
    private volatile long timeout;

    /** */
    private volatile boolean keepAll = true;

    /** */
    private volatile boolean incBackups;

    /** */
    private volatile boolean dedup;

    /** */
    private volatile ClusterGroup prj;

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
     * @param part Partition.
     * @param incMeta Include metadata flag.
     * @param keepPortable Keep portable flag.
     */
    public GridCacheQueryAdapter(GridCacheContext<?, ?> cctx,
        GridCacheQueryType type,
        @Nullable String clsName,
        @Nullable String clause,
        @Nullable IgniteBiPredicate<Object, Object> filter,
        @Nullable Integer part,
        boolean incMeta,
        boolean keepPortable) {
        assert cctx != null;
        assert type != null;
        assert part == null || part >= 0;

        this.cctx = cctx;
        this.type = type;
        this.clsName = clsName;
        this.clause = clause;
        this.filter = filter;
        this.part = part;
        this.incMeta = incMeta;
        this.keepPortable = keepPortable;

        log = cctx.logger(getClass());

        metrics = new GridCacheQueryMetricsAdapter();
    }

    /**
     * @param cctx Context.
     * @param type Query type.
     * @param log Logger.
     * @param pageSize Page size.
     * @param timeout Timeout.
     * @param keepAll Keep all flag.
     * @param incBackups Include backups flag.
     * @param dedup Enable dedup flag.
     * @param prj Grid projection.
     * @param filter Key-value filter.
     * @param part Partition.
     * @param clsName Class name.
     * @param clause Clause.
     * @param incMeta Include metadata flag.
     * @param keepPortable Keep portable flag.
     * @param subjId Security subject ID.
     * @param taskHash Task hash.
     */
    public GridCacheQueryAdapter(GridCacheContext<?, ?> cctx,
        GridCacheQueryType type,
        IgniteLogger log,
        int pageSize,
        long timeout,
        boolean keepAll,
        boolean incBackups,
        boolean dedup,
        ClusterGroup prj,
        IgniteBiPredicate<Object, Object> filter,
        @Nullable Integer part,
        @Nullable String clsName,
        String clause,
        boolean incMeta,
        boolean keepPortable,
        UUID subjId,
        int taskHash) {
        this.cctx = cctx;
        this.type = type;
        this.log = log;
        this.pageSize = pageSize;
        this.timeout = timeout;
        this.keepAll = keepAll;
        this.incBackups = incBackups;
        this.dedup = dedup;
        this.prj = prj;
        this.filter = filter;
        this.part = part;
        this.clsName = clsName;
        this.clause = clause;
        this.incMeta = incMeta;
        this.keepPortable = keepPortable;
        this.subjId = subjId;
        this.taskHash = taskHash;
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
    @Override public CacheQuery<T> pageSize(int pageSize) {
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
    @Override public CacheQuery<T> timeout(long timeout) {
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
    @Override public CacheQuery<T> keepAll(boolean keepAll) {
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
    @Override public CacheQuery<T> includeBackups(boolean incBackups) {
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
    @Override public CacheQuery<T> enableDedup(boolean dedup) {
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
    @Override public CacheQuery<T> projection(ClusterGroup prj) {
        this.prj = prj;

        return this;
    }

    /**
     * @return Grid projection.
     */
    public ClusterGroup projection() {
        return prj;
    }

    /**
     * @return Key-value filter.
     */
    @Nullable public <K, V> IgniteBiPredicate<K, V> scanFilter() {
        return (IgniteBiPredicate<K, V>)filter;
    }

    /**
     * @return Partition.
     */
    @Nullable public Integer partition() {
        return part;
    }

    /**
     * @throws IgniteCheckedException If query is invalid.
     */
    public void validate() throws IgniteCheckedException {
        if ((type != SCAN && type != SET) && !GridQueryProcessor.isEnabled(cctx.config()))
            throw new IgniteCheckedException("Indexing is disabled for cache: " + cctx.cache().name());
    }

    /**
     * @param res Query result.
     * @param err Error or {@code null} if query executed successfully.
     * @param startTime Start time.
     * @param duration Duration.
     */
    public void onExecuted(Object res, Throwable err, long startTime, long duration) {
        GridQueryProcessor.onExecuted(cctx, metrics, res, err, startTime, duration, log);
    }

    /** {@inheritDoc} */
    @Override public CacheQueryFuture<T> execute(@Nullable Object... args) {
        return execute(null, null, args);
    }

    /** {@inheritDoc} */
    @Override public <R> CacheQueryFuture<R> execute(IgniteReducer<T, R> rmtReducer, @Nullable Object... args) {
        return execute(rmtReducer, null, args);
    }

    /** {@inheritDoc} */
    @Override public <R> CacheQueryFuture<R> execute(IgniteClosure<T, R> rmtTransform, @Nullable Object... args) {
        return execute(null, rmtTransform, args);
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics metrics() {
        return metrics.copy();
    }

    /** {@inheritDoc} */
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
    private <R> CacheQueryFuture<R> execute(@Nullable IgniteReducer<T, R> rmtReducer,
        @Nullable IgniteClosure<T, R> rmtTransform, @Nullable Object... args) {
        Collection<ClusterNode> nodes;

        try {
            nodes = nodes();
        }
        catch (IgniteCheckedException e) {
            return queryErrorFuture(cctx, e, log);
        }

        cctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (nodes.isEmpty())
            return queryErrorFuture(cctx, new ClusterGroupEmptyCheckedException(), log);

        if (log.isDebugEnabled())
            log.debug("Executing query [query=" + this + ", nodes=" + nodes + ']');

        if (cctx.deploymentEnabled()) {
            try {
                cctx.deploy().registerClasses(filter, rmtReducer, rmtTransform);
                cctx.deploy().registerClasses(args);
            }
            catch (IgniteCheckedException e) {
                return queryErrorFuture(cctx, e, log);
            }
        }

        if (subjId == null)
            subjId = cctx.localNodeId();

        taskHash = cctx.kernalContext().job().currentTaskNameHash();

        final GridCacheQueryBean bean = new GridCacheQueryBean(this, (IgniteReducer<Object, Object>)rmtReducer,
            (IgniteClosure<Object, Object>)rmtTransform, args);

        final GridCacheQueryManager qryMgr = cctx.queries();

        boolean loc = nodes.size() == 1 && F.first(nodes).id().equals(cctx.localNodeId());

        if (type == SQL_FIELDS || type == SPI)
            return (CacheQueryFuture<R>)(loc ? qryMgr.queryFieldsLocal(bean) :
                qryMgr.queryFieldsDistributed(bean, nodes));
        else if (type == SCAN && part != null && nodes.size() > 1)
            return new CacheQueryFallbackFuture<>(nodes, bean, qryMgr);
        else
            return (CacheQueryFuture<R>)(loc ? qryMgr.queryLocal(bean) : qryMgr.queryDistributed(bean, nodes));
    }

    /**
     * @return Nodes to execute on.
     */
    private Collection<ClusterNode> nodes() throws IgniteCheckedException {
        CacheMode cacheMode = cctx.config().getCacheMode();

        switch (cacheMode) {
            case LOCAL:
                if (prj != null)
                    U.warn(log, "Ignoring query projection because it's executed over LOCAL cache " +
                        "(only local node will be queried): " + this);

                if (type == SCAN && cctx.config().getCacheMode() == LOCAL &&
                    partition() != null && partition() >= cctx.affinity().partitions())
                    throw new IgniteCheckedException("Invalid partition number: " + partition());

                return Collections.singletonList(cctx.localNode());

            case REPLICATED:
                if (prj != null || partition() != null)
                    return nodes(cctx, prj, partition());

                return cctx.affinityNode() ?
                    Collections.singletonList(cctx.localNode()) :
                    Collections.singletonList(F.rand(nodes(cctx, null, partition())));

            case PARTITIONED:
                return nodes(cctx, prj, partition());

            default:
                throw new IllegalStateException("Unknown cache distribution mode: " + cacheMode);
        }
    }

    /**
     * @param cctx Cache context.
     * @param prj Projection (optional).
     * @return Collection of data nodes in provided projection (if any).
     */
    private static Collection<ClusterNode> nodes(final GridCacheContext<?, ?> cctx,
        @Nullable final ClusterGroup prj, @Nullable final Integer part) {
        assert cctx != null;

        final AffinityTopologyVersion topVer = cctx.affinity().affinityTopologyVersion();

        Collection<ClusterNode> affNodes = CU.affinityNodes(cctx);

        if (prj == null && part == null)
            return affNodes;

        final Set<ClusterNode> owners =
            part == null ? Collections.<ClusterNode>emptySet() : new HashSet<>(cctx.topology().owners(part, topVer));

        return F.view(affNodes, new P1<ClusterNode>() {
            @Override public boolean apply(ClusterNode n) {

                return cctx.discovery().cacheAffinityNode(n, cctx.name()) &&
                    (prj == null || prj.node(n.id()) != null) &&
                    (part == null || owners.contains(n));
            }
        });
    }

    /**
     * @param cctx Cache context.
     * @param e Exception.
     * @param log Logger.
     */
    private static <T> GridCacheQueryErrorFuture<T> queryErrorFuture(GridCacheContext<?, ?> cctx,
        Exception e, IgniteLogger log) {

        GridCacheQueryMetricsAdapter metrics = (GridCacheQueryMetricsAdapter)cctx.queries().metrics();

        GridQueryProcessor.onExecuted(cctx, metrics, null, e, 0, 0, log);

        return new GridCacheQueryErrorFuture<>(cctx.kernalContext(), e);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryAdapter.class, this);
    }

    /**
     * Wrapper for queries with fallback.
     */
    private static class CacheQueryFallbackFuture<R> extends GridFutureAdapter<Collection<R>>
        implements CacheQueryFuture<R> {
        /** Query future. */
        private volatile GridCacheQueryFutureAdapter<?, ?, R> fut;

        /** Backups. */
        private final Queue<ClusterNode> nodes;

        /** Bean. */
        private final GridCacheQueryBean bean;

        /** Query manager. */
        private final GridCacheQueryManager qryMgr;

        /**
         * @param nodes Backups.
         * @param bean Bean.
         * @param qryMgr Query manager.
         */
        public CacheQueryFallbackFuture(Collection<ClusterNode> nodes, GridCacheQueryBean bean,
            GridCacheQueryManager qryMgr) {
            this.nodes = fallbacks(nodes);
            this.bean = bean;
            this.qryMgr = qryMgr;

            init();
        }

        /**
         * @param nodes Nodes.
         * @return Nodes for query execution.
         */
        private Queue<ClusterNode> fallbacks(Collection<ClusterNode> nodes) {
            Queue<ClusterNode> fallbacks = new LinkedList<>();

            ClusterNode node = F.first(F.view(nodes, IS_LOC_NODE));

            if (node != null)
                fallbacks.add(node);

            fallbacks.addAll(node != null ? F.view(nodes, F.not(IS_LOC_NODE)) : nodes);

            return fallbacks;
        }

        /**
         *
         */
        @SuppressWarnings("unchecked")
        private void init() {
            ClusterNode node = nodes.poll();

            GridCacheQueryFutureAdapter<?, ?, R> fut0 = (GridCacheQueryFutureAdapter<?, ?, R>)(node.isLocal() ?
                qryMgr.queryLocal(bean) :
                qryMgr.queryDistributed(bean, Collections.singleton(node)));

            fut0.listen(new IgniteInClosure<IgniteInternalFuture<Collection<R>>>() {
                @Override public void apply(IgniteInternalFuture<Collection<R>> fut) {
                    try {
                        onDone(fut.get());
                    }
                    catch (IgniteClientDisconnectedCheckedException e) {
                        onDone(e);
                    }
                    catch (IgniteCheckedException e) {
                        if (F.isEmpty(nodes))
                            onDone(e);
                        else
                            init();
                    }
                }
            });

            fut = fut0;
        }

        /** {@inheritDoc} */
        @Override public int available() {
            return fut.available();
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteCheckedException {
            return fut.cancel();
        }

        /** {@inheritDoc} */
        @Override public R next() {
            return fut.next();
        }
    }
}
