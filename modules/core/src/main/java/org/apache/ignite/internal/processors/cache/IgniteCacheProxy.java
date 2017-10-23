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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheManager;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.QueryDetailMetrics;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SpiQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.AsyncSupportAdapter;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.query.CacheQueryFuture;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.Nullable;

/**
 * Cache proxy.
 */
public class IgniteCacheProxy<K, V> extends AsyncSupportAdapter<IgniteCache<K, V>>
    implements IgniteCache<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final IgniteBiPredicate ACCEPT_ALL = new IgniteBiPredicate() {
        /** */
        private static final long serialVersionUID = -1640538788290240617L;

        @Override public boolean apply(Object k, Object v) {
            return true;
        }
    };

    /** Context. */
    private GridCacheContext<K, V> ctx;

    /** Gateway. */
    private GridCacheGateway<K, V> gate;

    /** Delegate. */
    @GridToStringInclude
    private IgniteInternalCache<K, V> delegate;

    /** Operation context. */
    private CacheOperationContext opCtx;

    /** */
    @GridToStringExclude
    private GridCacheProxyImpl<K, V> internalProxy;

    /** */
    @GridToStringExclude
    private CacheManager cacheMgr;

    /** If {@code false} does not acquire read lock on gateway enter. */
    @GridToStringExclude
    private boolean lock;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public IgniteCacheProxy() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param delegate Delegate.
     * @param opCtx Operation context.
     * @param async Async support flag.
     */
    public IgniteCacheProxy(
        GridCacheContext<K, V> ctx,
        IgniteInternalCache<K, V> delegate,
        CacheOperationContext opCtx,
        boolean async
    ) {
        this(ctx, delegate, opCtx, async, true);
    }

    /**
     * @param ctx Context.
     * @param delegate Delegate.
     * @param opCtx Operation context.
     * @param async Async support flag.
     * @param lock If {@code false} does not acquire read lock on gateway enter.
     */
    private IgniteCacheProxy(
        GridCacheContext<K, V> ctx,
        IgniteInternalCache<K, V> delegate,
        @Nullable CacheOperationContext opCtx,
        boolean async,
        boolean lock
    ) {
        super(async);

        assert ctx != null;
        assert delegate != null;

        this.ctx = ctx;
        this.delegate = delegate;
        this.opCtx = opCtx;

        gate = ctx.gate();

        internalProxy = new GridCacheProxyImpl<>(ctx, delegate, opCtx);

        this.lock = lock;
    }

    /**
     * @return Operation context.
     */
    @Nullable public CacheOperationContext operationContext() {
        return opCtx;
    }

    /**
     * Gets cache proxy which does not acquire read lock on gateway enter, should be used only if grid read lock is
     * externally acquired.
     *
     * @return Ignite cache proxy with simple gate.
     */
    public IgniteCacheProxy<K, V> cacheNoGate() {
        return new IgniteCacheProxy<>(ctx, delegate, opCtx, isAsync(), false);
    }

    /**
     * @return Context.
     */
    public GridCacheContext<K, V> context() {
        return ctx;
    }

    /**
     * @return Gateway.
     */
    public GridCacheGateway<K, V> gate() {
        return gate;
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return ctx.cache().clusterMetrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics(ClusterGroup grp) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return ctx.cache().clusterMetrics(grp);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics localMetrics() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return ctx.cache().localMetrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return ctx.cache().clusterMxBean();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean localMxBean() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return ctx.cache().localMxBean();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        CacheConfiguration cfg = ctx.config();

        if (!clazz.isAssignableFrom(cfg.getClass()))
            throw new IllegalArgumentException();

        return clazz.cast(cfg);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Nullable @Override public Cache.Entry<K, V> randomEntry() {
        GridKernalContext kctx = ctx.kernalContext();

        if (kctx.isDaemon() || kctx.clientNode())
            throw new UnsupportedOperationException("Not applicable for daemon or client node.");

        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return ctx.cache().randomEntry();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            CacheOperationContext prj0 = opCtx != null ? opCtx.withExpiryPolicy(plc) :
                new CacheOperationContext(false, null, false, plc, false, null);

            return new IgniteCacheProxy<>(ctx, delegate, prj0, isAsync(), lock);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withSkipStore() {
        return skipStore();
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteCache<K1, V1> withKeepBinary() {
        return keepBinary();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withNoRetries() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            boolean noRetries = opCtx != null && opCtx.noRetries();

            if (noRetries)
                return this;

            CacheOperationContext opCtx0 = opCtx != null ? opCtx.setNoRetries(true) :
                new CacheOperationContext(false, null, false, null, true, null);

            return new IgniteCacheProxy<>(ctx,
                delegate,
                opCtx0,
                isAsync(),
                lock);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    if (ctx.cache().isLocal())
                        setFuture(ctx.cache().localLoadCacheAsync(p, args));
                    else
                        setFuture(ctx.cache().globalLoadCacheAsync(p, args));
                }
                else {
                    if (ctx.cache().isLocal())
                        ctx.cache().localLoadCache(p, args);
                    else
                        ctx.cache().globalLoadCache(p, args);
                }
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync())
                    setFuture(delegate.localLoadCacheAsync(p, args));
                else
                    delegate.localLoadCache(p, args);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPutIfAbsent(K key, V val) throws CacheException {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.getAndPutIfAbsentAsync(key, val));

                    return null;
                }
                else
                    return delegate.getAndPutIfAbsent(key, val);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Lock lock(K key) throws CacheException {
        return lockAll(Collections.singleton(key));
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(final Collection<? extends K> keys) {
        return new CacheLockImpl<>(gate, delegate, opCtx, keys);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocalLocked(K key, boolean byCurrThread) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return byCurrThread ? delegate.isLockedByThread(key) : delegate.isLocked(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /**
     * @param scanQry ScanQry.
     * @param transformer Transformer
     * @param grp Optional cluster group.
     * @return Cursor.
     */
    @SuppressWarnings("unchecked")
    private <T, R> QueryCursor<R> query(
        final ScanQuery scanQry,
        @Nullable final IgniteClosure<T, R> transformer,
        @Nullable ClusterGroup grp)
        throws IgniteCheckedException {

        final CacheQuery<R> qry;

        boolean isKeepBinary = opCtx != null && opCtx.isKeepBinary();

        IgniteBiPredicate<K, V> p = scanQry.getFilter();

        qry = ctx.queries().createScanQuery(p, transformer, scanQry.getPartition(), isKeepBinary);

        if (scanQry.getPageSize() > 0)
            qry.pageSize(scanQry.getPageSize());

        if (grp != null)
            qry.projection(grp);

        final GridCloseableIterator<R> iter = ctx.kernalContext().query().executeQuery(GridCacheQueryType.SCAN,
            ctx.name(), ctx, new IgniteOutClosureX<GridCloseableIterator<R>>() {
                @Override public GridCloseableIterator<R> applyx() throws IgniteCheckedException {
                    final GridCloseableIterator iter0 = qry.executeScanQuery();

                    final boolean needToConvert = transformer == null;

                    return new GridCloseableIteratorAdapter<R>() {
                        @Override protected R onNext() throws IgniteCheckedException {
                            Object next = iter0.nextX();

                            if (needToConvert) {
                                Map.Entry<K, V> entry = (Map.Entry<K, V>)next;

                                return (R) new CacheEntryImpl<>(entry.getKey(), entry.getValue());
                            }

                            return (R)next;
                        }

                        @Override protected boolean onHasNext() throws IgniteCheckedException {
                            return iter0.hasNextX();
                        }

                        @Override protected void onClose() throws IgniteCheckedException {
                            iter0.close();
                        }
                    };
                }
            }, true);

        return new QueryCursorImpl<>(iter);
    }

    /**
     * @param filter Filter.
     * @param grp Optional cluster group.
     * @return Cursor.
     */
    @SuppressWarnings("unchecked")
    private QueryCursor<Cache.Entry<K, V>> query(final Query filter, @Nullable ClusterGroup grp)
        throws IgniteCheckedException {
        final CacheQuery qry;

        boolean isKeepBinary = opCtx != null && opCtx.isKeepBinary();

        final CacheQueryFuture fut;

        if (filter instanceof TextQuery) {
            TextQuery p = (TextQuery)filter;

            qry = ctx.queries().createFullTextQuery(p.getType(), p.getText(), isKeepBinary);

            if (grp != null)
                qry.projection(grp);

            fut = ctx.kernalContext().query().executeQuery(GridCacheQueryType.TEXT, p.getText(), ctx,
                new IgniteOutClosureX<CacheQueryFuture<Map.Entry<K, V>>>() {
                    @Override public CacheQueryFuture<Map.Entry<K, V>> applyx() throws IgniteCheckedException {
                        return qry.execute();
                    }
                }, false);
        }
        else if (filter instanceof SpiQuery) {
            qry = ctx.queries().createSpiQuery(isKeepBinary);

            if (grp != null)
                qry.projection(grp);

            fut = ctx.kernalContext().query().executeQuery(GridCacheQueryType.SPI, filter.getClass().getSimpleName(),
                ctx, new IgniteOutClosureX<CacheQueryFuture<Cache.Entry<K, V>>>() {
                    @Override public CacheQueryFuture<Cache.Entry<K, V>> applyx() throws IgniteCheckedException {
                        return qry.execute(((SpiQuery)filter).getArgs());
                    }
                }, false);
        }
        else {
            if (filter instanceof SqlFieldsQuery)
                throw new CacheException("Use methods 'queryFields' and 'localQueryFields' for " +
                    SqlFieldsQuery.class.getSimpleName() + ".");

            throw new CacheException("Unsupported query type: " + filter);
        }

        return new QueryCursorImpl<>(new GridCloseableIteratorAdapter<Entry<K, V>>() {
            /** */
            private Cache.Entry<K, V> cur;

            @Override protected Entry<K, V> onNext() throws IgniteCheckedException {
                if (!onHasNext())
                    throw new NoSuchElementException();

                Cache.Entry<K, V> e = cur;

                cur = null;

                return e;
            }

            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (cur != null)
                    return true;

                Object next = fut.next();

                // Workaround a bug: if IndexingSpi is configured future represents Iterator<Cache.Entry>
                // instead of Iterator<Map.Entry> due to IndexingSpi interface.
                if (next == null)
                    return false;

                if (next instanceof Cache.Entry)
                    cur = (Cache.Entry)next;
                else {
                    Map.Entry e = (Map.Entry)next;

                    cur = new CacheEntryImpl(e.getKey(), e.getValue());
                }

                return true;
            }

            @Override protected void onClose() throws IgniteCheckedException {
                fut.cancel();
            }
        });
    }

    /**
     * @param loc Enforce local.
     * @return Local node cluster group.
     */
    private ClusterGroup projection(boolean loc) {
        if (loc || ctx.isLocal() || isReplicatedDataNode())
            return ctx.kernalContext().grid().cluster().forLocal();

        if (ctx.isReplicated())
            return ctx.kernalContext().grid().cluster().forDataNodes(ctx.name()).forRandom();

        return null;
    }

    /**
     * Executes continuous query.
     *
     * @param qry Query.
     * @param loc Local flag.
     * @return Initial iteration cursor.
     */
    @SuppressWarnings("unchecked")
    private QueryCursor<Cache.Entry<K, V>> queryContinuous(ContinuousQuery qry, boolean loc, boolean keepBinary) {
        if (qry.getInitialQuery() instanceof ContinuousQuery)
            throw new IgniteException("Initial predicate for continuous query can't be an instance of another " +
                "continuous query. Use SCAN or SQL query for initial iteration.");

        if (qry.getLocalListener() == null)
            throw new IgniteException("Mandatory local listener is not set for the query: " + qry);

        if (qry.getRemoteFilter() != null && qry.getRemoteFilterFactory() != null)
            throw new IgniteException("Should be used either RemoterFilter or RemoteFilterFactory.");

        try {
            final UUID routineId = ctx.continuousQueries().executeQuery(
                qry.getLocalListener(),
                qry.getRemoteFilter(),
                qry.getRemoteFilterFactory(),
                qry.getPageSize(),
                qry.getTimeInterval(),
                qry.isAutoUnsubscribe(),
                loc,
                keepBinary,
                qry.isIncludeExpired());

            final QueryCursor<Cache.Entry<K, V>> cur =
                qry.getInitialQuery() != null ? query(qry.getInitialQuery()) : null;

            return new QueryCursor<Cache.Entry<K, V>>() {
                @Override public Iterator<Cache.Entry<K, V>> iterator() {
                    return cur != null ? cur.iterator() : new GridEmptyIterator<Cache.Entry<K, V>>();
                }

                @Override public List<Cache.Entry<K, V>> getAll() {
                    return cur != null ? cur.getAll() : Collections.<Cache.Entry<K, V>>emptyList();
                }

                @Override public void close() {
                    if (cur != null)
                        cur.close();

                    try {
                        ctx.kernalContext().continuous().stopRoutine(routineId).get();
                    }
                    catch (IgniteCheckedException e) {
                        throw U.convertException(e);
                    }
                }
            };
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        A.notNull(qry, "qry");

        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            ctx.checkSecurity(SecurityPermission.CACHE_READ);

            validate(qry);

            convertToBinary(qry);

            final CacheOperationContext opCtxCall = ctx.operationContextPerCall();

            if (qry instanceof ContinuousQuery)
                return (QueryCursor<R>)queryContinuous((ContinuousQuery<K, V>)qry, qry.isLocal(),
                    opCtxCall != null && opCtxCall.isKeepBinary());

            if (qry instanceof SqlQuery) {
                if (isReplicatedDataNode() && ((SqlQuery)qry).isDistributedJoins())
                    throw new CacheException("Queries using distributed JOINs have to be run on partitioned cache, " +
                        "not on replicated.");

                final SqlQuery p = (SqlQuery)qry;

                if (isReplicatedDataNode() || ctx.isLocal() || qry.isLocal())
                    return (QueryCursor<R>)new QueryCursorImpl<>(new Iterable<Cache.Entry<K, V>>() {
                        @Override public Iterator<Cache.Entry<K, V>> iterator() {
                            return ctx.kernalContext().query().queryLocal(ctx, p,
                                opCtxCall != null && opCtxCall.isKeepBinary());
                        }
                    });

                return (QueryCursor<R>)ctx.kernalContext().query().queryTwoStep(ctx, p);
            }

            if (qry instanceof SqlFieldsQuery) {
                if (isReplicatedDataNode() && ((SqlFieldsQuery)qry).isDistributedJoins())
                    throw new CacheException("Queries using distributed JOINs have to be run on partitioned cache, " +
                        "not on replicated.");

                SqlFieldsQuery p = (SqlFieldsQuery)qry;

                if (isReplicatedDataNode() || ctx.isLocal() || qry.isLocal())
                    return (QueryCursor<R>)ctx.kernalContext().query().queryLocalFields(ctx, p);

                return (QueryCursor<R>)ctx.kernalContext().query().queryTwoStep(ctx, p);
            }

            if (qry instanceof ScanQuery)
                return query((ScanQuery)qry, null, projection(qry.isLocal()));

            return (QueryCursor<R>)query(qry, projection(qry.isLocal()));
        }
        catch (Exception e) {
            if (e instanceof CacheException)
                throw (CacheException)e;

            throw new CacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> QueryCursor<R> query(Query<T> qry, IgniteClosure<T, R> transformer) {
        A.notNull(qry, "qry");
        A.notNull(transformer, "transformer");

        if (!(qry instanceof ScanQuery))
            throw new UnsupportedOperationException("Transformers are supported only for SCAN queries.");

        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            ctx.checkSecurity(SecurityPermission.CACHE_READ);

            validate(qry);

            return query((ScanQuery<K, V>)qry, transformer, projection(qry.isLocal()));
        }
        catch (Exception e) {
            if (e instanceof CacheException)
                throw (CacheException)e;

            throw new CacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /**
     * Convert query arguments to BinaryObjects if binary marshaller used.
     *
     * @param qry Query.
     */
    private void convertToBinary(final Query qry) {
        if (ctx.binaryMarshaller()) {
            if (qry instanceof SqlQuery) {
                final SqlQuery sqlQry = (SqlQuery) qry;

                convertToBinary(sqlQry.getArgs());
            }
            else if (qry instanceof SpiQuery) {
                final SpiQuery spiQry = (SpiQuery) qry;

                convertToBinary(spiQry.getArgs());
            }
            else if (qry instanceof SqlFieldsQuery) {
                final SqlFieldsQuery fieldsQry = (SqlFieldsQuery) qry;

                convertToBinary(fieldsQry.getArgs());
            }
        }
    }

    /**
     * Converts query arguments to BinaryObjects if binary marshaller used.
     *
     * @param args Arguments.
     */
    private void convertToBinary(final Object[] args) {
        if (args == null)
            return;

        for (int i = 0; i < args.length; i++)
            args[i] = ctx.cacheObjects().binary().toBinary(args[i]);
    }

    /**
     * @return {@code true} If this is a replicated cache and we are on a data node.
     */
    private boolean isReplicatedDataNode() {
        return ctx.isReplicated() && ctx.affinityNode();
    }

    /**
     * Checks query.
     *
     * @param qry Query
     * @throws CacheException If query indexing disabled for sql query.
     */
    private void validate(Query qry) {
        if (!GridQueryProcessor.isEnabled(ctx.config()) && !(qry instanceof ScanQuery) &&
            !(qry instanceof ContinuousQuery) && !(qry instanceof SpiQuery))
            throw new CacheException("Indexing is disabled for cache: " + ctx.cache().name() +
                ". Use setIndexedTypes or setTypeMetadata methods on CacheConfiguration to enable.");

        if (!ctx.kernalContext().query().moduleEnabled() &&
            (qry instanceof SqlQuery || qry instanceof SqlFieldsQuery || qry instanceof TextQuery))
            throw new CacheException("Failed to execute query. Add module 'ignite-indexing' to the classpath " +
                "of all Ignite nodes.");
    }

    /** {@inheritDoc} */
    @Override public Iterable<Cache.Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localEntries(peekModes);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics queryMetrics() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.context().queries().metrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void resetQueryMetrics() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.context().queries().resetMetrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends QueryDetailMetrics> queryDetailMetrics() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.context().queries().detailMetrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void resetQueryDetailMetrics() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.context().queries().resetDetailMetrics();
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.evictAll(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public V localPeek(K key, CachePeekMode... peekModes) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localPeek(key, peekModes, null);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localPromote(Set<? extends K> keys) throws CacheException {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                delegate.promoteAll(keys);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            if (isAsync()) {
                setFuture(delegate.sizeAsync(peekModes));

                return 0;
            }
            else
                return delegate.size(peekModes);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode... peekModes) throws CacheException {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            if (isAsync()) {
                setFuture(delegate.sizeLongAsync(peekModes));

                return 0;
            }
            else
                return delegate.sizeLong(peekModes);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int part, CachePeekMode... peekModes) throws CacheException {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            if (isAsync()) {
                setFuture(delegate.sizeLongAsync(part, peekModes));

                return 0;
            }
            else
                return delegate.sizeLong(part, peekModes);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localSize(peekModes);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode... peekModes) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localSizeLong(peekModes);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int part, CachePeekMode... peekModes) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.localSizeLong(part, peekModes);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.getAsync(key));

                    return null;
                }
                else
                    return delegate.get(key);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheEntry<K, V> getEntry(K key) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.getEntryAsync(key));

                    return null;
                }
                else
                    return delegate.getEntry(key);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.getAllAsync(keys));

                    return null;
                }
                else
                    return delegate.getAll(keys);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<K, V>> getEntries(Set<? extends K> keys) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.getEntriesAsync(keys));

                    return null;
                }
                else
                    return delegate.getEntries(keys);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAllOutTx(Set<? extends K> keys) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.getAllOutTxAsync(keys));

                    return null;
                }
                else
                    return delegate.getAllOutTx(keys);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /**
     * @param keys Keys.
     * @return Values map.
     */
    public Map<K, V> getAll(Collection<? extends K> keys) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.getAllAsync(keys));

                    return null;
                }
                else
                    return delegate.getAll(keys);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /**
     * Gets entry set containing internal entries.
     *
     * @param filter Filter.
     * @return Entry set.
     */
    public Set<Cache.Entry<K, V>> entrySetx(CacheEntryPredicate... filter) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return delegate.entrySetx(filter);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            if (isAsync()) {
                setFuture(delegate.containsKeyAsync(key));

                return false;
            }
            else
                return delegate.containsKey(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            if (isAsync()) {
                setFuture(delegate.containsKeysAsync(keys));

                return false;
            }
            else
                return delegate.containsKeys(keys);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadAll(
        Set<? extends K> keys,
        boolean replaceExisting,
        @Nullable final CompletionListener completionLsnr
    ) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            IgniteInternalFuture<?> fut = ctx.cache().loadAll(keys, replaceExisting);

            if (completionLsnr != null) {
                fut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> fut) {
                        try {
                            fut.get();

                            completionLsnr.onCompletion();
                        }
                        catch (IgniteCheckedException e) {
                            completionLsnr.onException(cacheException(e));
                        }
                    }
                });
            }
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    IgniteInternalFuture<Boolean> fut = delegate.putAsync(key, val);

                    IgniteInternalFuture<Void> fut0 = fut.chain(new CX1<IgniteInternalFuture<Boolean>, Void>() {
                        @Override public Void applyx(IgniteInternalFuture<Boolean> fut) throws IgniteCheckedException {
                            try {
                                fut.get();
                            }
                            catch (RuntimeException e) {
                                throw new GridClosureException(e);
                            }

                            return null;
                        }
                    });

                    setFuture(fut0);
                }
                else
                    delegate.put(key, val);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.getAndPutAsync(key, val));

                    return null;
                }
                else
                    return delegate.getAndPut(key, val);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync())
                    setFuture(delegate.putAllAsync(map));
                else
                    delegate.putAll(map);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.putIfAbsentAsync(key, val));

                    return false;
                }
                else
                    return delegate.putIfAbsent(key, val);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.removeAsync(key));

                    return false;
                }
                else
                    return delegate.remove(key);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.removeAsync(key, oldVal));

                    return false;
                }
                else
                    return delegate.remove(key, oldVal);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.getAndRemoveAsync(key));

                    return null;
                }
                else
                    return delegate.getAndRemove(key);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.replaceAsync(key, oldVal, newVal));

                    return false;
                }
                else
                    return delegate.replace(key, oldVal, newVal);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.replaceAsync(key, val));

                    return false;
                }
                else
                    return delegate.replace(key, val);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.getAndReplaceAsync(key, val));

                    return null;
                }
                else
                    return delegate.getAndReplace(key, val);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync())
                    setFuture(delegate.removeAllAsync(keys));
                else
                    delegate.removeAll(keys);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            if (isAsync())
                setFuture(delegate.removeAllAsync());
            else
                delegate.removeAll();
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            if (isAsync())
                setFuture(delegate.clearAsync(key));
            else
                delegate.clear(key);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            if (isAsync())
                setFuture(delegate.clearAllAsync(keys));
            else
                delegate.clearAll(keys);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            if (isAsync())
                setFuture(delegate.clearAsync());
            else
                delegate.clear();
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localClear(K key) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            delegate.clearLocally(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void localClearAll(Set<? extends K> keys) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            for (K key : keys)
                delegate.clearLocally(key);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... args)
        throws EntryProcessorException {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    IgniteInternalFuture<EntryProcessorResult<T>> fut = delegate.invokeAsync(key, entryProcessor, args);

                    IgniteInternalFuture<T> fut0 = fut.chain(new CX1<IgniteInternalFuture<EntryProcessorResult<T>>, T>() {
                        @Override public T applyx(IgniteInternalFuture<EntryProcessorResult<T>> fut)
                            throws IgniteCheckedException {
                            try {
                                EntryProcessorResult<T> res = fut.get();

                                return res != null ? res.get() : null;
                            }
                            catch (RuntimeException e) {
                                throw new GridClosureException(e);
                            }
                        }
                    });

                    setFuture(fut0);

                    return null;
                }
                else {
                    EntryProcessorResult<T> res = delegate.invoke(key, entryProcessor, args);

                    return res != null ? res.get() : null;
                }
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, CacheEntryProcessor<K, V, T> entryProcessor, Object... args)
        throws EntryProcessorException {
        return invoke(key, (EntryProcessor<K, V, T>)entryProcessor, args);
    }

    /**
     * @param topVer Locked topology version.
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param args Arguments.
     * @return Invoke result.
     */
    public <T> T invoke(@Nullable AffinityTopologyVersion topVer,
        K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync())
                    throw new UnsupportedOperationException();
                else {
                    EntryProcessorResult<T> res = delegate.invoke(topVer, key, entryProcessor, args);

                    return res != null ? res.get() : null;
                }
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.invokeAllAsync(keys, entryProcessor, args));

                    return null;
                }
                else
                    return delegate.invokeAll(keys, entryProcessor, args);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.invokeAllAsync(keys, entryProcessor, args));

                    return null;
                }
                else
                    return delegate.invokeAll(keys, entryProcessor, args);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                if (isAsync()) {
                    setFuture(delegate.invokeAllAsync(map, args));

                    return null;
                }
                else
                    return delegate.invokeAll(map, args);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return delegate.name();
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        return cacheMgr;
    }

    /**
     * @param cacheMgr Cache manager.
     */
    public void setCacheManager(CacheManager cacheMgr) {
        this.cacheMgr = cacheMgr;
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        GridCacheGateway<K, V> gate = this.gate;

        if (!onEnterIfNoStop(gate))
            return;

        IgniteInternalFuture<?> fut;

        try {
            fut = ctx.kernalContext().cache().dynamicDestroyCache(ctx.name(), true);
        }
        finally {
            onLeave(gate);
        }

        try {
            fut.get();
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        GridCacheGateway<K, V> gate = this.gate;

        if (!onEnterIfNoStop(gate))
            return;

        IgniteInternalFuture<?> fut;

        try {
            fut = ctx.kernalContext().cache().dynamicCloseCache(ctx.name());
        }
        finally {
            onLeave(gate);
        }

        try {
            fut.get();
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        GridCacheGateway<K, V> gate = this.gate;

        if (!onEnterIfNoStop(gate))
            return true;

        try {
            return ctx.kernalContext().cache().context().closed(ctx);
        }
        finally {
            onLeave(gate);
        }
    }

    /**
     * @return Proxy delegate.
     */
    public IgniteInternalCache delegate() {
        return delegate;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass()))
            return (T)this;
        else if (clazz.isAssignableFrom(IgniteEx.class))
            return (T)ctx.grid();

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + clazz);
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            CacheOperationContext opCtx = ctx.operationContextPerCall();

            ctx.continuousQueries().executeJCacheQuery(lsnrCfg, false, opCtx != null && opCtx.isKeepBinary());
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            ctx.continuousQueries().cancelJCacheQuery(lsnrCfg);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            return ctx.cache().igniteIterator();
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<K, V> createAsyncInstance() {
        return new IgniteCacheProxy<>(ctx, delegate, opCtx, true, lock);
    }

    /**
     * Creates projection that will operate with binary objects. <p> Projection returned by this method will force
     * cache not to deserialize binary objects, so keys and values will be returned from cache API methods without
     * changes. Therefore, signature of the projection can contain only following types: <ul> <li>{@code BinaryObject}
     * for binary classes</li> <li>All primitives (byte, int, ...) and there boxed versions (Byte, Integer, ...)</li>
     * <li>Arrays of primitives (byte[], int[], ...)</li> <li>{@link String} and array of {@link String}s</li>
     * <li>{@link UUID} and array of {@link UUID}s</li> <li>{@link Date} and array of {@link Date}s</li> <li>{@link
     * java.sql.Timestamp} and array of {@link java.sql.Timestamp}s</li> <li>Enums and array of enums</li> <li> Maps,
     * collections and array of objects (but objects inside them will still be converted if they are binary) </li>
     * </ul> <p> For example, if you use {@link Integer} as a key and {@code Value} class as a value (which will be
     * stored in binary format), you should acquire following projection to avoid deserialization:
     * <pre>
     * IgniteInternalCache<Integer, GridBinaryObject> prj = cache.keepBinary();
     *
     * // Value is not deserialized and returned in binary format.
     * GridBinaryObject po = prj.get(1);
     * </pre>
     * <p> Note that this method makes sense only if cache is working in binary mode ({@code
     * CacheConfiguration#isBinaryEnabled()} returns {@code true}. If not, this method is no-op and will return
     * current projection.
     *
     * @return Projection for binary objects.
     */
    @SuppressWarnings("unchecked")
    public <K1, V1> IgniteCache<K1, V1> keepBinary() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            CacheOperationContext opCtx0 =
                new CacheOperationContext(
                    opCtx != null && opCtx.skipStore(),
                    opCtx != null ? opCtx.subjectId() : null,
                    true,
                    opCtx != null ? opCtx.expiry() : null,
                    opCtx != null && opCtx.noRetries(),
                    opCtx != null ? opCtx.dataCenterId() : null);

            return new IgniteCacheProxy<>((GridCacheContext<K1, V1>)ctx,
                (GridCacheAdapter<K1, V1>)delegate,
                opCtx0,
                isAsync(),
                lock);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /**
     * @return Projection for data center id.
     */
    @SuppressWarnings("unchecked")
    public IgniteCache<K, V> withDataCenterId(byte dataCenterId) {
        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            Byte prevDataCenterId = opCtx != null ? opCtx.dataCenterId() : null;

            if (prevDataCenterId != null && dataCenterId == prevDataCenterId)
                return this;

            CacheOperationContext opCtx0 =
                new CacheOperationContext(
                    opCtx != null && opCtx.skipStore(),
                    opCtx != null ? opCtx.subjectId() : null,
                    opCtx != null && opCtx.isKeepBinary(),
                    opCtx != null ? opCtx.expiry() : null,
                    opCtx != null && opCtx.noRetries(),
                    dataCenterId);

            return new IgniteCacheProxy<>(ctx,
                delegate,
                opCtx0,
                isAsync(),
                lock);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /**
     * @return Cache with skip store enabled.
     */
    public IgniteCache<K, V> skipStore() {
        GridCacheGateway<K, V> gate = this.gate;

        CacheOperationContext prev = onEnter(gate, opCtx);

        try {
            boolean skip = opCtx != null && opCtx.skipStore();

            if (skip)
                return this;

            CacheOperationContext opCtx0 =
                new CacheOperationContext(true,
                    opCtx != null ? opCtx.subjectId() : null,
                    opCtx != null && opCtx.isKeepBinary(),
                    opCtx != null ? opCtx.expiry() : null,
                    opCtx != null && opCtx.noRetries(),
                    opCtx != null ? opCtx.dataCenterId() : null);

            return new IgniteCacheProxy<>(ctx,
                delegate,
                opCtx0,
                isAsync(),
                lock);
        }
        finally {
            onLeave(gate, prev);
        }
    }

    /**
     * @param e Checked exception.
     * @return Cache exception.
     */
    private RuntimeException cacheException(IgniteCheckedException e) {
        return CU.convertToCacheException(e);
    }

    /**
     * @param fut Future for async operation.
     */
    private <R> void setFuture(IgniteInternalFuture<R> fut) {
        curFut.set(new IgniteCacheFutureImpl<>(fut));
    }

    /**
     * @return Internal proxy.
     */
    public GridCacheProxyImpl<K, V> internalProxy() {
        return internalProxy;
    }

    /**
     * @return {@code True} if proxy was closed.
     */
    public boolean proxyClosed() {
        return !gate.getClass().equals(GridCacheGateway.class);
    }

    /**
     * Closes this proxy instance.
     */
    public void closeProxy() {
        gate = new GridCacheGateway<K, V>(ctx) {
            @Override public void enter() {
                throw new IllegalStateException("Cache has been closed: " + ctx.name());
            }

            @Override public boolean enterIfNotStopped() {
                return false;
            }

            @Override public boolean enterIfNotStoppedNoLock() {
                return false;
            }

            @Override public void leaveNoLock() {
                assert false;
            }

            @Override public void leave() {
                assert false;
            }

            @Nullable @Override public CacheOperationContext enter(@Nullable CacheOperationContext opCtx) {
                throw new IllegalStateException("Cache has been closed: " + ctx.name());
            }

            @Nullable @Override public CacheOperationContext enterNoLock(@Nullable CacheOperationContext opCtx) {
                throw new IllegalStateException("Cache has been closed: " + ctx.name());
            }

            @Override public void leave(CacheOperationContext prev) {
                assert false;
            }

            @Override public void leaveNoLock(CacheOperationContext prev) {
                assert false;
            }

            @Override public void stopped() {
                // No-op.
            }

            @Override public void onStopped() {
                // No-op.
            }
        };
    }

    /**
     * @param gate Cache gateway.
     * @param opCtx Cache operation context to guard.
     * @return Previous projection set on this thread.
     */
    private CacheOperationContext onEnter(GridCacheGateway<K, V> gate, CacheOperationContext opCtx) {
        if (lock)
            return gate.enter(opCtx);
        else
            return gate.enterNoLock(opCtx);
    }

    /**
     * @param gate Cache gateway.
     * @return {@code True} if enter successful.
     */
    private boolean onEnterIfNoStop(GridCacheGateway<K, V> gate) {
        if (lock)
            return gate.enterIfNotStopped();
        else
            return gate.enterIfNotStoppedNoLock();
    }

    /**
     * @param gate Cache gateway.
     * @param opCtx Operation context to guard.
     */
    private void onLeave(GridCacheGateway<K, V> gate, CacheOperationContext opCtx) {
        if (lock)
            gate.leave(opCtx);
        else
            gate.leaveNoLock(opCtx);
    }

    /**
     * @param gate Cache gateway.
     */
    private void onLeave(GridCacheGateway<K, V> gate) {
        if (lock)
            gate.leave();
        else
            gate.leaveNoLock();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);

        out.writeObject(delegate);

        out.writeObject(opCtx);

        out.writeBoolean(lock);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridCacheContext<K, V>)in.readObject();

        delegate = (IgniteInternalCache<K, V>)in.readObject();

        opCtx = (CacheOperationContext)in.readObject();

        gate = ctx.gate();

        lock = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> rebalance() {
        return new IgniteFutureImpl<>(ctx.preloader().forceRebalance());
    }

    /**
     * Gets value without waiting for toplogy changes.
     *
     * @param key Key.
     * @return Value.
     */
    public V getTopologySafe(K key) {
        try {
            GridCacheGateway<K, V> gate = this.gate;

            CacheOperationContext prev = onEnter(gate, opCtx);

            try {
                return delegate.getTopologySafe(key);
            }
            finally {
                onLeave(gate, prev);
            }
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteCacheProxy.class, this);
    }
}
