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

import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtUnreservedPartitionException;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SET;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SPI;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;

/**
 * Query adapter.
 */
public class GridCacheQueryAdapter<T> implements CacheQuery<T> {
    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final GridCacheQueryType type;

    /** */
    private final IgniteLogger log;

    /** Class name in case of binary query. */
    private final String clsName;

    /** */
    @GridToStringInclude(sensitive = true)
    private final String clause;

    /** */
    private final IgniteBiPredicate<Object, Object> filter;

    /** Transformer. */
    private IgniteClosure<?, ?> transform;

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
    private boolean keepBinary;

    /** */
    private UUID subjId;

    /** */
    private int taskHash;

    /**
     * @param cctx Context.
     * @param type Query type.
     * @param filter Scan filter.
     * @param part Partition.
     * @param keepBinary Keep binary flag.
     */
    public GridCacheQueryAdapter(GridCacheContext<?, ?> cctx,
        GridCacheQueryType type,
        @Nullable IgniteBiPredicate<Object, Object> filter,
        @Nullable IgniteClosure<Map.Entry, Object> transform,
        @Nullable Integer part,
        boolean keepBinary) {
        assert cctx != null;
        assert type != null;
        assert part == null || part >= 0;

        this.cctx = cctx;
        this.type = type;
        this.filter = filter;
        this.transform = transform;
        this.part = part;
        this.keepBinary = keepBinary;

        log = cctx.logger(getClass());

        metrics = new GridCacheQueryMetricsAdapter();

        this.incMeta = false;
        this.clsName = null;
        this.clause = null;
    }

    /**
     * @param cctx Context.
     * @param type Query type.
     * @param clsName Class name.
     * @param clause Clause.
     * @param filter Scan filter.
     * @param part Partition.
     * @param incMeta Include metadata flag.
     * @param keepBinary Keep binary flag.
     */
    public GridCacheQueryAdapter(GridCacheContext<?, ?> cctx,
        GridCacheQueryType type,
        @Nullable String clsName,
        @Nullable String clause,
        @Nullable IgniteBiPredicate<Object, Object> filter,
        @Nullable Integer part,
        boolean incMeta,
        boolean keepBinary) {
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
        this.keepBinary = keepBinary;

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
     * @param keepBinary Keep binary flag.
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
        boolean keepBinary,
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
        this.keepBinary = keepBinary;
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
     * @return {@code True} if binary should not be deserialized.
     */
    public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * Forces query to keep binary object representation even if query was created on plain projection.
     *
     * @param keepBinary Keep binary flag.
     */
    public void keepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;
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
    @SuppressWarnings("unchecked")
    @Nullable public <K, V> IgniteBiPredicate<K, V> scanFilter() {
        return (IgniteBiPredicate<K, V>)filter;
    }

    /**
     * @return Transformer.
     */
    @SuppressWarnings("unchecked")
    @Nullable public <K, V> IgniteClosure<Map.Entry<K, V>, Object> transform() {
        return (IgniteClosure<Map.Entry<K, V>, Object>) transform;
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
        if ((type != SCAN && type != SET && type != SPI) && !GridQueryProcessor.isEnabled(cctx.config()))
            throw new IgniteCheckedException("Indexing is disabled for cache: " + cctx.cache().name());
    }

    /** {@inheritDoc} */
    @Override public CacheQueryFuture<T> execute(@Nullable Object... args) {
        return execute0(null, args);
    }

    /** {@inheritDoc} */
    @Override public <R> CacheQueryFuture<R> execute(IgniteReducer<T, R> rmtReducer, @Nullable Object... args) {
        return execute0(rmtReducer, args);
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
     * @param args Arguments.
     * @return Future.
     */
    @SuppressWarnings({"IfMayBeConditional", "unchecked"})
    private <R> CacheQueryFuture<R> execute0(@Nullable IgniteReducer<T, R> rmtReducer, @Nullable Object... args) {
        assert type != SCAN : this;

        Collection<ClusterNode> nodes;

        try {
            nodes = nodes();
        }
        catch (IgniteCheckedException e) {
            return new GridCacheQueryErrorFuture<>(cctx.kernalContext(), e);
        }

        cctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (nodes.isEmpty())
            return new GridCacheQueryErrorFuture<>(cctx.kernalContext(), new ClusterGroupEmptyCheckedException());

        if (log.isDebugEnabled())
            log.debug("Executing query [query=" + this + ", nodes=" + nodes + ']');

        if (cctx.deploymentEnabled()) {
            try {
                cctx.deploy().registerClasses(filter, rmtReducer);
                cctx.deploy().registerClasses(args);
            }
            catch (IgniteCheckedException e) {
                return new GridCacheQueryErrorFuture<>(cctx.kernalContext(), e);
            }
        }

        if (subjId == null)
            subjId = cctx.localNodeId();

        taskHash = cctx.kernalContext().job().currentTaskNameHash();

        final GridCacheQueryBean bean = new GridCacheQueryBean(this, (IgniteReducer<Object, Object>)rmtReducer,
            null, args);

        final GridCacheQueryManager qryMgr = cctx.queries();

        boolean loc = nodes.size() == 1 && F.first(nodes).id().equals(cctx.localNodeId());

        if (type == SQL_FIELDS || type == SPI)
            return (CacheQueryFuture<R>)(loc ? qryMgr.queryFieldsLocal(bean) :
                qryMgr.queryFieldsDistributed(bean, nodes));
        else
            return (CacheQueryFuture<R>)(loc ? qryMgr.queryLocal(bean) : qryMgr.queryDistributed(bean, nodes));
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional", "unchecked"})
    @Override public GridCloseableIterator executeScanQuery() throws IgniteCheckedException {
        assert type == SCAN : "Wrong processing of qyery: " + type;

        Collection<ClusterNode> nodes = nodes();

        cctx.checkSecurity(SecurityPermission.CACHE_READ);

        if (nodes.isEmpty() && part == null)
            return new GridEmptyCloseableIterator();

        if (log.isDebugEnabled())
            log.debug("Executing query [query=" + this + ", nodes=" + nodes + ']');

        if (cctx.deploymentEnabled())
            cctx.deploy().registerClasses(filter);

        if (subjId == null)
            subjId = cctx.localNodeId();

        taskHash = cctx.kernalContext().job().currentTaskNameHash();

        final GridCacheQueryManager qryMgr = cctx.queries();

        if (part != null && !cctx.isLocal())
            return new ScanQueryFallbackClosableIterator(part, this, qryMgr, cctx);
        else {
            boolean loc = nodes.size() == 1 && F.first(nodes).id().equals(cctx.localNodeId());

            return loc ? qryMgr.scanQueryLocal(this, true) : qryMgr.scanQueryDistributed(this, nodes);
        }
    }

    /**
     * @return Nodes to execute on.
     */
    private Collection<ClusterNode> nodes() throws IgniteCheckedException {
        CacheMode cacheMode = cctx.config().getCacheMode();

        Integer part = partition();

        switch (cacheMode) {
            case LOCAL:
                if (prj != null)
                    U.warn(log, "Ignoring query projection because it's executed over LOCAL cache " +
                        "(only local node will be queried): " + this);

                if (type == SCAN && cctx.config().getCacheMode() == LOCAL &&
                    part != null && part >= cctx.affinity().partitions())
                    throw new IgniteCheckedException("Invalid partition number: " + part);

                return Collections.singletonList(cctx.localNode());

            case REPLICATED:
                if (prj != null || part != null)
                    return nodes(cctx, prj, part);

                return cctx.affinityNode() ?
                    Collections.singletonList(cctx.localNode()) :
                    Collections.singletonList(F.rand(nodes(cctx, null, null)));

            case PARTITIONED:
                return nodes(cctx, prj, part);

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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryAdapter.class, this);
    }

    /**
     * Wrapper for queries with fallback.
     */
    private static class ScanQueryFallbackClosableIterator extends GridCloseableIteratorAdapter<Map.Entry> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Query future. */
        private volatile T2<GridCloseableIterator<Map.Entry>, GridCacheQueryFutureAdapter> tuple;

        /** Backups. */
        private volatile Queue<ClusterNode> nodes;

        /** Topology version of the last detected {@link GridDhtUnreservedPartitionException}. */
        private volatile AffinityTopologyVersion unreservedTopVer;

        /** Number of times to retry the query on the nodes failed with {@link GridDhtUnreservedPartitionException}. */
        private volatile int unreservedNodesRetryCnt = 5;

        /** Bean. */
        private final GridCacheQueryAdapter qry;

        /** Query manager. */
        private final GridCacheQueryManager qryMgr;

        /** Cache context. */
        private final GridCacheContext cctx;

        /** Partition. */
        private final int part;

        /** Flag indicating that a first item has been returned to a user. */
        private boolean firstItemReturned;

        /** */
        private Map.Entry cur;

        /**
         * @param part Partition.
         * @param qry Query.
         * @param qryMgr Query manager.
         * @param cctx Cache context.
         */
        private ScanQueryFallbackClosableIterator(int part, GridCacheQueryAdapter qry,
            GridCacheQueryManager qryMgr, GridCacheContext cctx) {
            this.qry = qry;
            this.qryMgr = qryMgr;
            this.cctx = cctx;
            this.part = part;

            nodes = fallbacks(cctx.discovery().topologyVersionEx());

            if (F.isEmpty(nodes))
                throw new ClusterTopologyException("Failed to execute the query " +
                    "(all affinity nodes left the grid) [cache=" + cctx.name() +
                    ", qry=" + qry +
                    ", startTopVer=" + cctx.versions().last().topologyVersion() +
                    ", curTopVer=" + qryMgr.queryTopologyVersion().topologyVersion() + ']');

            init();
        }

        /**
         * @param topVer Topology version.
         * @return Nodes for query execution.
         */
        private Queue<ClusterNode> fallbacks(AffinityTopologyVersion topVer) {
            Deque<ClusterNode> fallbacks = new LinkedList<>();
            Collection<ClusterNode> owners = new HashSet<>();

            for (ClusterNode node : cctx.topology().owners(part, topVer)) {
                if (node.isLocal())
                    fallbacks.addFirst(node);
                else
                    fallbacks.add(node);

                owners.add(node);
            }

            for (ClusterNode node : cctx.topology().moving(part)) {
                if (!owners.contains(node))
                    fallbacks.add(node);
            }

            return fallbacks;
        }

        /**
         *
         */
        @SuppressWarnings("unchecked")
        private void init() {
            final ClusterNode node = nodes.poll();

            if (node.isLocal()) {
                try {
                    GridCloseableIterator it = qryMgr.scanQueryLocal(qry, true);

                    tuple = new T2(it, null);
                }
                catch (IgniteClientDisconnectedCheckedException e) {
                    throw CU.convertToCacheException(e);
                }
                catch (IgniteCheckedException e) {
                    retryIfPossible(e);
                }
            }
            else {
                final GridCacheQueryBean bean = new GridCacheQueryBean(qry, null, null, null);

                GridCacheQueryFutureAdapter fut =
                    (GridCacheQueryFutureAdapter)qryMgr.queryDistributed(bean, Collections.singleton(node));

                tuple = new T2(null, fut);
            }
        }

        /** {@inheritDoc} */
        @Override protected Map.Entry onNext() throws IgniteCheckedException {
            if (!onHasNext())
                throw new NoSuchElementException();

            assert cur != null;

            Map.Entry e = cur;

            cur = null;

            return e;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            while (true) {
                if (cur != null)
                    return true;

                T2<GridCloseableIterator<Map.Entry>, GridCacheQueryFutureAdapter> t = tuple;

                GridCloseableIterator<Map.Entry> iter = t.get1();

                if (iter != null) {
                    boolean hasNext = iter.hasNext();

                    if (hasNext)
                        cur = iter.next();

                    return hasNext;
                }
                else {
                    GridCacheQueryFutureAdapter fut = t.get2();

                    assert fut != null;

                    if (firstItemReturned)
                        return (cur = (Map.Entry)fut.next()) != null;

                    try {
                        fut.awaitFirstPage();

                        firstItemReturned = true;

                        return (cur = (Map.Entry)fut.next()) != null;
                    }
                    catch (IgniteClientDisconnectedCheckedException e) {
                        throw CU.convertToCacheException(e);
                    }
                    catch (IgniteCheckedException e) {
                        retryIfPossible(e);
                    }
                }
            }
        }

        /**
         * @param e Exception for query run.
         */
        private void retryIfPossible(IgniteCheckedException e) {
            try {
                IgniteInternalFuture<?> retryFut;

                GridDhtUnreservedPartitionException partErr = X.cause(e, GridDhtUnreservedPartitionException.class);

                if (partErr != null) {
                    AffinityTopologyVersion waitVer = partErr.topologyVersion();

                    assert waitVer != null;

                    retryFut = cctx.affinity().affinityReadyFuture(waitVer);
                }
                else if (e.hasCause(ClusterTopologyCheckedException.class)) {
                    ClusterTopologyCheckedException topEx = X.cause(e, ClusterTopologyCheckedException.class);

                    retryFut = topEx.retryReadyFuture();
                }
                else if (e.hasCause(ClusterGroupEmptyCheckedException.class)) {
                    ClusterGroupEmptyCheckedException ex = X.cause(e, ClusterGroupEmptyCheckedException.class);

                    retryFut = ex.retryReadyFuture();
                }
                else
                    throw CU.convertToCacheException(e);

                if (F.isEmpty(nodes)) {
                    if (--unreservedNodesRetryCnt > 0) {
                        if (retryFut != null)
                            retryFut.get();

                        nodes = fallbacks(unreservedTopVer == null ? cctx.discovery().topologyVersionEx() : unreservedTopVer);

                        unreservedTopVer = null;

                        init();
                    }
                    else
                        throw CU.convertToCacheException(e);
                }
                else
                    init();
            }
            catch (IgniteCheckedException ex) {
                throw CU.convertToCacheException(ex);
            }
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            super.onClose();

            T2<GridCloseableIterator<Map.Entry>, GridCacheQueryFutureAdapter> t = tuple;

            if (t != null && t.get1() != null)
                t.get1().close();

            if (t != null && t.get2() != null)
                t.get2().cancel();
        }
    }
}
