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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCacheRestartingException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheManager;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cache.query.AbstractContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer.EventListener;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.IndexQuery;
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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.AsyncSupportAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.query.CacheQueryFuture;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Cache proxy implementation.
 */
@SuppressWarnings("unchecked")
public class IgniteCacheProxyImpl<K, V> extends AsyncSupportAdapter<IgniteCache<K, V>>
    implements IgniteCacheProxy<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Ignite version that introduce {@link ContinuousQueryWithTransformer} feature.
     */
    private static final IgniteProductVersion CONT_QRY_WITH_TRANSFORMER_SINCE =
        IgniteProductVersion.fromString("2.5.0");

    /** Cache name. */
    private String cacheName;

    /** Context. */
    private volatile GridCacheContext<K, V> ctx;

    /** Old context. */
    private transient volatile GridCacheContext<K, V> oldContext;

    /** Delegate. */
    @GridToStringInclude
    private volatile IgniteInternalCache<K, V> delegate;

    /** Cached proxy wrapper. */
    private volatile IgniteCacheProxy<K, V> cachedProxy;

    /** */
    @GridToStringExclude
    private CacheManager cacheMgr;

    /** Future indicates that cache is under restarting. */
    private final AtomicReference<RestartFuture> restartFut;

    /** Flag indicates that proxy is closed. */
    private volatile boolean closed;

    /** Proxy initialization latch used for await final completion after proxy created, as an example,
     * a proxy may be created but the exchange is not completed and if we try to perform some cache
     * the operation we get last finished exchange future (need for validation)
     * for the previous version but not for current.
     */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public IgniteCacheProxyImpl() {
        restartFut = new AtomicReference<>(null);
    }

    /**
     * @param ctx Context.
     * @param delegate Delegate.
     * @param async Async support flag.
     */
    public IgniteCacheProxyImpl(
            @NotNull GridCacheContext<K, V> ctx,
            @NotNull IgniteInternalCache<K, V> delegate,
            boolean async
    ) {
        this(ctx, delegate, new AtomicReference<>(null), async);
    }

    /**
     * @param ctx Context.
     * @param delegate Delegate.
     * @param async Async support flag.
     */
    private IgniteCacheProxyImpl(
        @NotNull GridCacheContext<K, V> ctx,
        @NotNull IgniteInternalCache<K, V> delegate,
        @NotNull AtomicReference<RestartFuture> restartFut,
        boolean async
    ) {
        super(async);

        assert ctx != null;
        assert delegate != null;

        cacheName = ctx.name();

        assert cacheName.equals(delegate.name()) : "ctx.name=" + cacheName + ", delegate.name=" + delegate.name();

        this.ctx = ctx;
        this.delegate = delegate;

        this.restartFut = restartFut;
    }

    /**
     *
     * @return Init latch.
     */
    public CountDownLatch getInitLatch() {
        return initLatch;
    }

    /**
     * @return Context.
     */
    @Override public GridCacheContext<K, V> context() {
        return getContextSafe();
    }

    /**
     * @return Context or throw restart exception.
     */
    private GridCacheContext<K, V> getContextSafe() {
        while (true) {
            GridCacheContext<K, V> ctx = this.ctx;

            if (ctx == null) {
                checkRestart();

                if (Thread.currentThread().isInterrupted())
                    throw new IgniteException(new InterruptedException());
            }
            else
                return ctx;
        }
    }

    /**
     * @return Delegate or throw restart exception.
     */
    private IgniteInternalCache<K, V> getDelegateSafe() {
        while (true) {
            IgniteInternalCache<K, V> delegate = this.delegate;

            if (delegate == null) {
                checkRestart();

                if (Thread.currentThread().isInterrupted())
                    throw new IgniteException(new InterruptedException());
            }
            else
                return delegate;
        }
    }

    /**
     * @return Context.
     */
    public GridCacheContext<K, V> context0() {
        GridCacheContext<K, V> ctx = this.ctx;

        if (ctx == null) {
            synchronized (this) {
                ctx = this.ctx;

                if (ctx == null) {
                    ctx = oldContext;

                    assert ctx != null;

                    return ctx;
                }
            }
        }

        return ctx;
    }

    /** {@inheritDoc} */
    @Override public IgniteCacheProxy<K, V> cacheNoGate() {
        return new GatewayProtectedCacheProxy<>(this, new CacheOperationContext(), false);
    }

    /**
     * @return Default cached proxy wrapper {@link GatewayProtectedCacheProxy}.
     */
    public IgniteCacheProxy<K, V> gatewayWrapper() {
        if (cachedProxy != null)
            return cachedProxy;

        cachedProxy = new GatewayProtectedCacheProxy<>(this, new CacheOperationContext(), true);

        return cachedProxy;
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics() {
        GridCacheContext<K, V> ctx = getContextSafe();

        return ctx.cache().clusterMetrics();
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics(ClusterGroup grp) {
        GridCacheContext<K, V> ctx = getContextSafe();

        return ctx.cache().clusterMetrics(grp);
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics localMetrics() {
        GridCacheContext<K, V> ctx = getContextSafe();

        return ctx.cache().localMetrics();
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        GridCacheContext<K, V> ctx = getContextSafe();

        CacheConfiguration cfg = ctx.config();

        if (!clazz.isAssignableFrom(cfg.getClass()))
            throw new IllegalArgumentException();

        return clazz.cast(cfg);
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withSkipStore() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <K1, V1> IgniteCache<K1, V1> withKeepBinary() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withNoRetries() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withPartitionRecover() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withReadRepair(ReadRepairStrategy strategy) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        GridCacheContext<K, V> ctx = getContextSafe();

        try {
            if (isAsync()) {
                setFuture(ctx.cache().globalLoadCacheAsync(p, args));
            }
            else {
                ctx.cache().globalLoadCache(p, args);
            }
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> loadCacheAsync(@Nullable IgniteBiPredicate<K, V> p,
        @Nullable Object... args) throws CacheException {
        GridCacheContext<K, V> ctx = getContextSafe();

        try {
            return (IgniteFuture<Void>)createFuture(ctx.cache().globalLoadCacheAsync(p, args));
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync())
                setFuture(delegate.localLoadCacheAsync(p, args));
            else
                delegate.localLoadCache(p, args);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> localLoadCacheAsync(@Nullable IgniteBiPredicate<K, V> p,
        @Nullable Object... args) throws CacheException {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return (IgniteFuture<Void>)createFuture(delegate.localLoadCacheAsync(p, args));
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPutIfAbsent(K key, V val) throws CacheException {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.getAndPutIfAbsentAsync(key, val));

                return null;
            }
            else
                return delegate.getAndPutIfAbsent(key, val);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndPutIfAbsentAsync(K key, V val) throws CacheException {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.getAndPutIfAbsentAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public Lock lock(K key) throws CacheException {
        return lockAll(Collections.singleton(key));
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(final Collection<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();
        GridCacheContext<K, V> ctx = getContextSafe();

        return new CacheLockImpl<>(ctx.gate(), delegate, ctx.operationContextPerCall(), keys);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocalLocked(K key, boolean byCurrThread) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return byCurrThread ? delegate.isLockedByThread(key) : delegate.isLocked(key);
    }

    /**
     * @param scanQry ScanQry.
     * @param transformer Transformer
     * @param grp Optional cluster group.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private <R> QueryCursor<R> query(
        final ScanQuery<K, V> scanQry,
        @Nullable final IgniteClosure<Cache.Entry<K, V>, R> transformer,
        @Nullable ClusterGroup grp
    ) throws IgniteCheckedException {
        GridCacheContext<K, V> ctx = getContextSafe();

        CacheOperationContext opCtxCall = ctx.operationContextPerCall();

        boolean isKeepBinary = opCtxCall != null && opCtxCall.isKeepBinary();

        IgniteBiPredicate<K, V> p = scanQry.getFilter();

        IgniteBiTuple<Set<KeyCacheObject>, List<IgniteTxEntry>> txChanges = transactionChanges(ctx, scanQry.getPartition());

        final CacheQuery<R> qry = ctx.queries().createScanQuery(
            p, transformer, scanQry.getPartition(), isKeepBinary, scanQry.isLocal(), null, txChanges.get1());

        if (scanQry.getPageSize() > 0)
            qry.pageSize(scanQry.getPageSize());

        if (grp != null)
            qry.projection(grp);

        GridCloseableIterator<R> res = ctx.kernalContext().query().executeQuery(GridCacheQueryType.SCAN,
            cacheName, ctx, new IgniteOutClosureX<GridCloseableIterator<R>>() {
                @Override public GridCloseableIterator<R> applyx() throws IgniteCheckedException {
                    return qry.executeScanQuery();
                }
            }, true);

        return new QueryCursorImpl<>(F.isEmpty(txChanges.get2())
            ? res
            : iteratorWithTxData(scanQry.getFilter(), transformer, res, txChanges)
        );
    }

    /** */
    private <R> @NotNull GridCloseableIterator<R> iteratorWithTxData(
        @Nullable IgniteBiPredicate<K, V> filter,
        @Nullable IgniteClosure<Entry<K, V>, R> transformer,
        final GridCloseableIterator<R> iter,
        IgniteBiTuple<Set<KeyCacheObject>, List<IgniteTxEntry>> txChanges
    ) {
        final GridIterator<Entry<K, V>> entryIter =
            F.iterator(txChanges.get2(), e -> new CacheEntryImpl<>((K)e.key(), (V)e.value(), e.explicitVersion()), true);

        final GridIterator<R> txIter = F.iterator(
            (Iterable<Entry<K, V>>)entryIter,
            e -> (R)(transformer == null ? e : transformer.apply(e)),
            true,
            e -> filter == null || filter.apply(e.getKey(), e.getValue())
        );

        return new GridCloseableIteratorAdapter<>() {
            /** {@inheritDoc} */
            @Override protected R onNext() {
                return iter.hasNext() ? iter.next() : txIter.next();
            }

            /** {@inheritDoc} */
            @Override protected boolean onHasNext() {
                return iter.hasNext() || txIter.hasNext();
            }

            /** {@inheritDoc} */
            @Override protected void onClose() throws IgniteCheckedException {
                iter.close();

                super.onClose();
            }
        };
    }

    /**
     * @param query Query.
     * @param grp Optional cluster group.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private QueryCursor<Cache.Entry<K, V>> query(final Query query, @Nullable ClusterGroup grp)
        throws IgniteCheckedException {
        GridCacheContext<K, V> ctx = getContextSafe();

        final CacheQuery qry;

        CacheOperationContext opCtxCall = ctx.operationContextPerCall();

        boolean isKeepBinary = opCtxCall != null && opCtxCall.isKeepBinary();

        final CacheQueryFuture fut;

        if (query instanceof TextQuery) {
            TextQuery q = (TextQuery)query;

            qry = ctx.queries().createFullTextQuery(q.getType(), q.getText(), q.getLimit(), q.getPageSize(), isKeepBinary);

            if (grp != null)
                qry.projection(grp);

            fut = ctx.kernalContext().query().executeQuery(GridCacheQueryType.TEXT, q.getText(), ctx,
                new IgniteOutClosureX<CacheQueryFuture<Map.Entry<K, V>>>() {
                    @Override public CacheQueryFuture<Map.Entry<K, V>> applyx() {
                        return qry.execute();
                    }
                }, false);
        }
        else if (query instanceof SpiQuery) {
            qry = ctx.queries().createSpiQuery(isKeepBinary);

            if (grp != null)
                qry.projection(grp);

            fut = ctx.kernalContext().query().executeQuery(GridCacheQueryType.SPI, query.getClass().getSimpleName(),
                ctx, new IgniteOutClosureX<CacheQueryFuture<Map.Entry<K, V>>>() {
                    @Override public CacheQueryFuture<Map.Entry<K, V>> applyx() {
                        return qry.execute(((SpiQuery)query).getArgs());
                    }
                }, false);
        }
        else if (query instanceof IndexQuery) {
            IndexQuery q = (IndexQuery)query;

            qry = ctx.queries().createIndexQuery(q, isKeepBinary);

            if (q.getPageSize() > 0)
                qry.pageSize(q.getPageSize());

            if (grp != null)
                qry.projection(grp);

            if (q.getLimit() > 0)
                qry.limit(q.getLimit());

            if (query.isLocal()) {
                final GridCloseableIterator iter = ctx.kernalContext().query().executeQuery(GridCacheQueryType.INDEX,
                    cacheName, ctx, new IgniteOutClosureX<GridCloseableIterator>() {
                        @Override public GridCloseableIterator applyx() throws IgniteCheckedException {
                            return ctx.queries().indexQueryLocal((GridCacheQueryAdapter)qry);
                        }
                    }, true);

                return new QueryCursorImpl(iter);
            }

            fut = ctx.kernalContext().query().executeQuery(GridCacheQueryType.INDEX, q.getValueType(), ctx,
                new IgniteOutClosureX<CacheQueryFuture<Map.Entry<K, V>>>() {
                    @Override public CacheQueryFuture<Map.Entry<K, V>> applyx() {
                        return qry.execute();
                    }
                }, false);
        }
        else {
            if (query instanceof SqlFieldsQuery)
                throw new CacheException("Use methods 'queryFields' and 'localQueryFields' for " +
                    SqlFieldsQuery.class.getSimpleName() + ".");

            throw new CacheException("Unsupported query type: " + query);
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
        GridCacheContext<K, V> ctx = getContextSafe();

        return loc ? ctx.kernalContext().grid().cluster().forLocal() : null;
    }

    /**
     * Executes continuous query.
     *
     * @param qry Query.
     * @param loc Local flag.
     * @param keepBinary Keep binary flag.
     * @return Initial iteration cursor.
     */
    @SuppressWarnings("unchecked")
    private QueryCursor<Cache.Entry<K, V>> queryContinuous(AbstractContinuousQuery qry, boolean loc, boolean keepBinary) {
        GridCacheContext<K, V> ctx = getContextSafe();

        assert qry instanceof ContinuousQuery || qry instanceof ContinuousQueryWithTransformer;

        if (qry.getInitialQuery() instanceof ContinuousQuery ||
            qry.getInitialQuery() instanceof ContinuousQueryWithTransformer) {
            throw new IgniteException("Initial predicate for continuous query can't be an instance of another " +
                "continuous query. Use SCAN or SQL query for initial iteration.");
        }

        CacheEntryUpdatedListener locLsnr = null;

        EventListener locTransLsnr = null;

        CacheEntryEventSerializableFilter rmtFilter = null;

        Factory<? extends IgniteClosure> rmtTransFactory = null;

        if (qry instanceof ContinuousQuery) {
            ContinuousQuery<K, V> qry0 = (ContinuousQuery<K, V>)qry;

            if (qry0.getLocalListener() == null &&
                qry0.getRemoteFilterFactory() == null &&
                qry0.getRemoteFilter() == null) {
                throw new IgniteException("LocalListener, RemoterFilter " +
                    "or RemoteFilterFactory must be specified for the query: " + qry);
            }

            if (qry0.getRemoteFilter() != null && qry0.getRemoteFilterFactory() != null)
                throw new IgniteException("Should be used either RemoterFilter or RemoteFilterFactory.");

            locLsnr = qry0.getLocalListener();

            rmtFilter = qry0.getRemoteFilter();
        }
        else {
            ContinuousQueryWithTransformer<K, V, ?> qry0 = (ContinuousQueryWithTransformer<K, V, ?>)qry;

            if (qry0.getLocalListener() == null && qry0.getRemoteFilterFactory() == null) {
                throw new IgniteException("LocalListener " +
                    "or RemoteFilterFactory must be specified for the query: " + qry);
            }

            if (qry0.getRemoteTransformerFactory() == null)
                throw new IgniteException("Mandatory RemoteTransformerFactory is not set for the query: " + qry);

            Collection<ClusterNode> nodes = context().grid().cluster().nodes();

            for (ClusterNode node : nodes) {
                if (node.version().compareTo(CONT_QRY_WITH_TRANSFORMER_SINCE) < 0) {
                    throw new IgniteException("Can't start ContinuousQueryWithTransformer, " +
                        "because some nodes in cluster doesn't support this feature: " + node);
                }
            }

            locTransLsnr = qry0.getLocalListener();

            rmtTransFactory = qry0.getRemoteTransformerFactory();
        }

        try {
            final UUID routineId = ctx.continuousQueries().executeQuery(
                locLsnr,
                locTransLsnr,
                rmtFilter,
                qry.getRemoteFilterFactory(),
                rmtTransFactory,
                qry.getPageSize(),
                qry.getTimeInterval(),
                qry.isAutoUnsubscribe(),
                loc,
                keepBinary,
                qry.isIncludeExpired());

            try {
                final QueryCursor<Cache.Entry<K, V>> cur =
                        qry.getInitialQuery() != null ? query(qry.getInitialQuery()) : null;

                return new QueryCursorEx<Entry<K, V>>() {
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

                    @Override public void getAll(Consumer<Entry<K, V>> c) {
                        // No-op.
                    }

                    @Override public List<GridQueryFieldMetadata> fieldsMeta() {
                        //noinspection rawtypes
                        return cur instanceof QueryCursorEx ? ((QueryCursorEx)cur).fieldsMeta() : null;
                    }

                    @Override public boolean isQuery() {
                        return false;
                    }
                };
            }
            catch (Throwable t) {
                // Initial query failed: stop the routine.
                ctx.kernalContext().continuous().stopRoutine(routineId).get();

                throw t;
            }
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        return (FieldsQueryCursor<List<?>>)query((Query)qry);
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> queryMultipleStatements(SqlFieldsQuery qry) {
        GridCacheContext<K, V> ctx = getContextSafe();

        A.notNull(qry, "qry");
        try {
            ctx.checkSecurity(SecurityPermission.CACHE_READ);

            validate(qry);

            convertToBinary(qry);

            CacheOperationContext opCtxCall = ctx.operationContextPerCall();

            boolean keepBinary = opCtxCall != null && opCtxCall.isKeepBinary();

            return ctx.kernalContext().query().querySqlFields(ctx, qry, null, keepBinary, false);
        }
        catch (Exception e) {
            if (e instanceof CacheException)
                throw (CacheException)e;

            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        GridCacheContext<K, V> ctx = getContextSafe();

        A.notNull(qry, "qry");
        try {
            ctx.checkSecurity(SecurityPermission.CACHE_READ);

            validate(qry);

            convertToBinary(qry);

            CacheOperationContext opCtxCall = ctx.operationContextPerCall();

            boolean keepBinary = opCtxCall != null && opCtxCall.isKeepBinary();

            if (qry instanceof ContinuousQuery || qry instanceof ContinuousQueryWithTransformer)
                return (QueryCursor<R>)queryContinuous((AbstractContinuousQuery)qry, qry.isLocal(), keepBinary);

            if (qry instanceof SqlQuery)
                return (QueryCursor<R>)ctx.kernalContext().query().querySql(ctx, (SqlQuery)qry, keepBinary);

            if (qry instanceof SqlFieldsQuery)
                return (FieldsQueryCursor<R>)ctx.kernalContext().query().querySqlFields(ctx, (SqlFieldsQuery)qry,
                    null, keepBinary, true).get(0);

            if (qry instanceof ScanQuery)
                return query((ScanQuery<K, V>)qry, null, projection(qry.isLocal()));

            return (QueryCursor<R>)query(qry, projection(qry.isLocal()));
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
        catch (Exception e) {
            if (e instanceof CacheException)
                throw (CacheException)e;

            throw new CacheException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> QueryCursor<R> query(Query<T> qry, IgniteClosure<T, R> transformer) {
        GridCacheContext<K, V> ctx = getContextSafe();

        A.notNull(qry, "qry");
        A.notNull(transformer, "transformer");

        if (!(qry instanceof ScanQuery))
            throw new UnsupportedOperationException("Transformers are supported only for SCAN queries.");

        try {
            ctx.checkSecurity(SecurityPermission.CACHE_READ);

            validate(qry);

            return query((ScanQuery<K, V>)qry, (IgniteClosure<Cache.Entry<K, V>, R>)transformer, projection(qry.isLocal()));
        }
        catch (Exception e) {
            if (e instanceof CacheException)
                throw (CacheException)e;

            throw new CacheException(e);
        }
    }

    /**
     * Convert query arguments to BinaryObjects if binary marshaller used.
     *
     * @param qry Query.
     */
    private void convertToBinary(final Query qry) {
        GridCacheContext<K, V> ctx = getContextSafe();

        if (ctx.binaryMarshaller()) {
            if (qry instanceof SqlQuery) {
                final SqlQuery sqlQry = (SqlQuery)qry;

                convertToBinary(sqlQry.getArgs());
            }
            else if (qry instanceof SpiQuery) {
                final SpiQuery spiQry = (SpiQuery)qry;

                convertToBinary(spiQry.getArgs());
            }
            else if (qry instanceof SqlFieldsQuery) {
                final SqlFieldsQuery fieldsQry = (SqlFieldsQuery)qry;

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

        GridCacheContext<K, V> ctx = getContextSafe();

        for (int i = 0; i < args.length; i++)
            args[i] = ctx.cacheObjects().binary().toBinary(args[i]);
    }

    /**
     * Checks query.
     *
     * @param qry Query
     * @throws CacheException If query indexing disabled for sql query.
     */
    private void validate(Query qry) {
        GridCacheContext<K, V> ctx = getContextSafe();

        if (!QueryUtils.isEnabled(ctx.config()) && !(qry instanceof ScanQuery) &&
            !(qry instanceof ContinuousQuery) && !(qry instanceof ContinuousQueryWithTransformer) &&
            !(qry instanceof SpiQuery) && !(qry instanceof SqlQuery) && !(qry instanceof SqlFieldsQuery) &&
            !(qry instanceof IndexQuery))
            throw new CacheException("Indexing is disabled for cache: " + cacheName +
                    ". Use setIndexedTypes or setTypeMetadata methods on CacheConfiguration to enable.");

        if (!ctx.kernalContext().query().moduleEnabled() &&
            (qry instanceof SqlQuery || qry instanceof SqlFieldsQuery || qry instanceof TextQuery))
            throw new CacheException("Failed to execute query. Add module 'ignite-indexing' to the classpath " +
                    "of all Ignite nodes or configure any query engine.");

        if (qry instanceof ScanQuery) {
            if (!U.isTxAwareQueriesEnabled(ctx))
                return;

            IgniteInternalTx tx = ctx.cache().context().tm().tx();

            if (tx == null)
                return;

            IgniteTxManager.ensureTransactionModeSupported(tx.isolation());
        }
    }

    /** {@inheritDoc} */
    @Override public Iterable<Cache.Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            return delegate.localEntries(peekModes);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics queryMetrics() {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return delegate.context().queries().metrics();
    }

    /** {@inheritDoc} */
    @Override public void resetQueryMetrics() {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        delegate.context().queries().resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends QueryDetailMetrics> queryDetailMetrics() {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return delegate.context().queries().detailMetrics();
    }

    /** {@inheritDoc} */
    @Override public void resetQueryDetailMetrics() {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        delegate.context().queries().resetDetailMetrics();
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        delegate.evictAll(keys);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V localPeek(K key, CachePeekMode... peekModes) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            return delegate.localPeek(key, peekModes);
        }
        catch (IgniteException | IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.sizeAsync(peekModes));

                return 0;
            }
            else
                return delegate.size(peekModes);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Integer> sizeAsync(CachePeekMode... peekModes) throws CacheException {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.sizeAsync(peekModes));
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode... peekModes) throws CacheException {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.sizeLongAsync(peekModes));

                return 0;
            }
            else
                return delegate.sizeLong(peekModes);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Long> sizeLongAsync(CachePeekMode... peekModes) throws CacheException {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.sizeLongAsync(peekModes));
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int part, CachePeekMode... peekModes) throws CacheException {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.sizeLongAsync(part, peekModes));

                return 0;
            }
            else
                return delegate.sizeLong(part, peekModes);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Long> sizeLongAsync(int part, CachePeekMode... peekModes) throws CacheException {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.sizeLongAsync(part, peekModes));
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            return delegate.localSize(peekModes);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode... peekModes) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            return delegate.localSizeLong(peekModes);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int part, CachePeekMode... peekModes) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            return delegate.localSizeLong(part, peekModes);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.getAsync(key));

                return null;
            }
            else
                return delegate.get(key);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAsync(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.getAsync(key));
    }

    /** {@inheritDoc} */
    @Override public CacheEntry<K, V> getEntry(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.getEntryAsync(key));

                return null;
            }
            else
                return delegate.getEntry(key);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<CacheEntry<K, V>> getEntryAsync(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.getEntryAsync(key));
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.getAllAsync(keys));

                return null;
            }
            else
                return delegate.getAll(keys);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllAsync(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.getAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<K, V>> getEntries(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.getEntriesAsync(keys));

                return null;
            }
            else
                return delegate.getEntries(keys);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Collection<CacheEntry<K, V>>> getEntriesAsync(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.getEntriesAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAllOutTx(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.getAllOutTxAsync(keys));

                return null;
            }
            else
                return delegate.getAllOutTx(keys);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Map<K, V>> getAllOutTxAsync(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.getAllOutTxAsync(keys));
    }

    /**
     * @param keys Keys.
     * @return Values map.
     */
    public Map<K, V> getAll(Collection<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.getAllAsync(keys));

                return null;
            }
            else
                return delegate.getAll(keys);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        if (isAsync()) {
            setFuture(delegate.containsKeyAsync(key));

            return false;
        }
        else
            return delegate.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeyAsync(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.containsKeyAsync(key));
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        if (isAsync()) {
            setFuture(delegate.containsKeysAsync(keys));

            return false;
        }
        else
            return delegate.containsKeys(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeysAsync(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.containsKeysAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public void loadAll(
        Set<? extends K> keys,
        boolean replaceExisting,
        @Nullable final CompletionListener completionLsnr
    ) {
        GridCacheContext<K, V> ctx = getContextSafe();

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

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync())
                setFuture(putAsync0(key, val));
            else
                delegate.put(key, val);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> putAsync(K key, V val) {
        return createFuture(putAsync0(key, val));
    }

    /**
     * Put async internal operation implementation.
     *
     * @param key Key.
     * @param val Value.
     * @return Internal future.
     */
    private IgniteInternalFuture<Void> putAsync0(K key, V val) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        IgniteInternalFuture<Boolean> fut = delegate.putAsync(key, val);

        return fut.chain(new CX1<IgniteInternalFuture<Boolean>, Void>() {
            @Override public Void applyx(IgniteInternalFuture<Boolean> fut1) throws IgniteCheckedException {
                try {
                    fut1.get();
                }
                catch (RuntimeException e) {
                    throw new GridClosureException(e);
                }

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.getAndPutAsync(key, val));

                return null;
            }
            else
                return delegate.getAndPut(key, val);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndPutAsync(K key, V val) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.getAndPutAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync())
                setFuture(delegate.putAllAsync(map));
            else
                delegate.putAll(map);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> putAllAsync(Map<? extends K, ? extends V> map) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return (IgniteFuture<Void>)createFuture(delegate.putAllAsync(map));
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.putIfAbsentAsync(key, val));

                return false;
            }
            else
                return delegate.putIfAbsent(key, val);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> putIfAbsentAsync(K key, V val) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.putIfAbsentAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.removeAsync(key));

                return false;
            }
            else
                return delegate.remove(key);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.removeAsync(key));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.removeAsync(key, oldVal));

                return false;
            }
            else
                return delegate.remove(key, oldVal);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> removeAsync(K key, V oldVal) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.removeAsync(key, oldVal));
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.getAndRemoveAsync(key));

                return null;
            }
            else
                return delegate.getAndRemove(key);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndRemoveAsync(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.getAndRemoveAsync(key));
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.replaceAsync(key, oldVal, newVal));

                return false;
            }
            else
                return delegate.replace(key, oldVal, newVal);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.replaceAsync(key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.replaceAsync(key, val));

                return false;
            }
            else
                return delegate.replace(key, val);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> replaceAsync(K key, V val) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.replaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.getAndReplaceAsync(key, val));

                return null;
            }
            else
                return delegate.getAndReplace(key, val);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<V> getAndReplaceAsync(K key, V val) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.getAndReplaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync())
                setFuture(delegate.removeAllAsync(keys));
            else
                delegate.removeAll(keys);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> removeAllAsync(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return (IgniteFuture<Void>)createFuture(delegate.removeAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync())
                setFuture(delegate.removeAllAsync());
            else
                delegate.removeAll();
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> removeAllAsync() {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return (IgniteFuture<Void>)createFuture(delegate.removeAllAsync());
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync())
                setFuture(delegate.clearAsync(key));
            else
                delegate.clear(key);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAsync(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return (IgniteFuture<Void>)createFuture(delegate.clearAsync(key));
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync())
                setFuture(delegate.clearAllAsync(keys));
            else
                delegate.clearAll(keys);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAllAsync(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return (IgniteFuture<Void>)createFuture(delegate.clearAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync())
                setFuture(delegate.clearAsync());
            else
                delegate.clear();
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> clearAsync() {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return (IgniteFuture<Void>)createFuture(delegate.clearAsync());
    }

    /** {@inheritDoc} */
    @Override public void localClear(K key) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        delegate.clearLocally(key);
    }

    /** {@inheritDoc} */
    @Override public void localClearAll(Set<? extends K> keys) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        for (K key : keys)
            delegate.clearLocally(key);
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... args)
        throws EntryProcessorException {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(invokeAsync0(key, entryProcessor, args));

                return null;
            }
            else {
                EntryProcessorResult<T> res = delegate.invoke(key, entryProcessor, args);

                return res != null ? res.get() : null;
            }
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> invokeAsync(K key, EntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        return createFuture(invokeAsync0(key, entryProcessor, args));
    }

    /**
     * Invoke async operation internal implementation.
     *
     * @param key Key.
     * @param entryProcessor Processor.
     * @param args Arguments.
     * @return Internal future.
     */
    private <T> IgniteInternalFuture<T> invokeAsync0(K key, EntryProcessor<K, V, T> entryProcessor, Object[] args) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        IgniteInternalFuture<EntryProcessorResult<T>> fut = delegate.invokeAsync(key, entryProcessor, args);

        return fut.chain(new CX1<IgniteInternalFuture<EntryProcessorResult<T>>, T>() {
            @Override public T applyx(IgniteInternalFuture<EntryProcessorResult<T>> fut1)
                throws IgniteCheckedException {
                try {
                    return fut1.get().get();
                }
                catch (RuntimeException e) {
                    throw new GridClosureException(e);
                }
            }
        });
    }


    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, CacheEntryProcessor<K, V, T> entryProcessor, Object... args)
        throws EntryProcessorException {
        return invoke(key, (EntryProcessor<K, V, T>)entryProcessor, args);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> invokeAsync(K key, CacheEntryProcessor<K, V, T> entryProcessor,
        Object... args) {
        return invokeAsync(key, (EntryProcessor<K, V, T>)entryProcessor, args);
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
        Object... args
    ) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync())
                throw new UnsupportedOperationException();
            else {
                EntryProcessorResult<T> res = delegate.invoke(topVer, key, entryProcessor, args);

                return res != null ? res.get() : null;
            }
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args
    ) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.invokeAllAsync(keys, entryProcessor, args));

                return null;
            }
            else
                return delegate.invokeAll(keys, entryProcessor, args);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor, Object... args) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.invokeAllAsync(keys, entryProcessor, args));
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor,
        Object... args
    ) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.invokeAllAsync(keys, entryProcessor, args));

                return null;
            }
            else
                return delegate.invokeAll(keys, entryProcessor, args);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.invokeAllAsync(keys, entryProcessor, args));
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            if (isAsync()) {
                setFuture(delegate.invokeAllAsync(map, args));

                return null;
            }
            else
                return delegate.invokeAll(map, args);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map, Object... args) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return createFuture(delegate.invokeAllAsync(map, args));
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return cacheName;
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
        destroyAsync().get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> destroyAsync() {
        GridCacheContext<K, V> ctx = getContextSafe();

        return new IgniteFutureImpl<>(ctx.kernalContext().cache().dynamicDestroyCache(cacheName, false, true, false, null), exec());
    }

    /** {@inheritDoc} */
    @Override public void close() {
        closeAsync().get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> closeAsync() {
        GridCacheContext<K, V> ctx = getContextSafe();

        return new IgniteFutureImpl<>(ctx.kernalContext().cache().dynamicCloseCache(cacheName), exec());
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        GridCacheContext<K, V> ctx = getContextSafe();

        return isProxyClosed() || ctx.kernalContext().cache().context().closed(ctx);
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass()))
            return (T)this;
        else if (clazz.isAssignableFrom(IgniteEx.class)) {
            GridCacheContext<K, V> ctx = getContextSafe();

            return (T)ctx.grid();
        }

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + clazz);
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        GridCacheContext<K, V> ctx = getContextSafe();

        try {
            CacheOperationContext opCtx = ctx.operationContextPerCall();

            ctx.continuousQueries().executeJCacheQuery(lsnrCfg, false, opCtx != null && opCtx.isKeepBinary());
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> lsnrCfg) {
        GridCacheContext<K, V> ctx = getContextSafe();

        try {
            ctx.continuousQueries().cancelJCacheQuery(lsnrCfg);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        GridCacheContext<K, V> ctx = getContextSafe();

        try {
            return ctx.cache().igniteIterator();
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<K, V> createAsyncInstance() {
        GridCacheContext<K, V> ctx = getContextSafe();
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return new IgniteCacheProxyImpl<K, V>(
                ctx,
                delegate,
                true
        );
    }

    /**
     * Creates projection that will operate with binary objects.
     * <p> Projection returned by this method will force cache not to deserialize binary objects,
     * so keys and values will be returned from cache API methods without changes.
     * Therefore, signature of the projection can contain only following types:
     * <ul>
     *     <li>{@code BinaryObject} for binary classes</li>
     *     <li>All primitives (byte, int, ...) and there boxed versions (Byte, Integer, ...)</li>
     *     <li>Arrays of primitives (byte[], int[], ...)</li>
     *     <li>{@link String} and array of {@link String}s</li>
     *     <li>{@link UUID} and array of {@link UUID}s</li>
     *     <li>{@link Date} and array of {@link Date}s</li>
     *     <li>{@link java.sql.Timestamp} and array of {@link java.sql.Timestamp}s</li>
     *     <li>Enums and array of enums</li>
     *     <li> Maps, collections and array of objects (but objects inside them will still be converted if they are binary) </li>
     * </ul>
     * <p> For example, if you use {@link Integer} as a key and {@code Value} class as a value (which will be
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
    @Override public <K1, V1> IgniteCache<K1, V1> keepBinary() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param dataCenterId Data center ID.
     * @return Projection for data center id.
     */
    @Override public IgniteCache<K, V> withDataCenterId(byte dataCenterId) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return Cache with skip store enabled.
     */
    @Override public IgniteCache<K, V> skipStore() {
        throw new UnsupportedOperationException();
    }

    /**
     * Method converts exception to IgniteCacheRestartingException in case of cache restarting
     * or to CacheException in other cases.
     *
     * @param e {@code IgniteCheckedException} or {@code IgniteException}.
     * @return Cache exception.
     */
    private RuntimeException cacheException(Exception e) {
        GridFutureAdapter<Void> restartFut = this.restartFut.get();

        if (X.hasCause(e, IgniteCacheRestartingException.class)) {
            IgniteCacheRestartingException restartingEx = X.cause(e, IgniteCacheRestartingException.class);

            if (restartingEx.restartFuture() == null) {
                if (restartFut == null)
                    restartFut = suspend();

                assert restartFut != null;

                throw new IgniteCacheRestartingException(new IgniteFutureImpl<>(restartFut, exec()), cacheName);
            }
            else
                throw restartingEx;
        }

        if (restartFut != null) {
            if (X.hasCause(e, CacheStoppedException.class) || X.hasSuppressed(e, CacheStoppedException.class))
                throw new IgniteCacheRestartingException(new IgniteFutureImpl<>(restartFut, exec()), "Cache is restarting: " +
                        cacheName, e);
        }

        if (e instanceof IgniteException && X.hasCause(e, CacheException.class))
            e = X.cause(e, CacheException.class);

        if (e instanceof IgniteCheckedException)
            return CU.convertToCacheException((IgniteCheckedException)e);

        if (X.hasCause(e, CacheStoppedException.class))
            return CU.convertToCacheException(X.cause(e, CacheStoppedException.class));

        if (e instanceof RuntimeException)
            return (RuntimeException)e;

        throw new IllegalStateException("Unknown exception", e);
    }

    /**
     * @param fut Future for async operation.
     */
    private <R> void setFuture(IgniteInternalFuture<R> fut) {
        curFut.set(createFuture(fut));
    }

    /** {@inheritDoc} */
    @Override protected <R> IgniteFuture<R> createFuture(IgniteInternalFuture<R> fut) {
        return new IgniteCacheFutureImpl<>(fut, exec());
    }

    /**
     * @return Internal proxy.
     */
    @Override public GridCacheProxyImpl<K, V> internalProxy() {
        GridCacheContext<K, V> ctx = getContextSafe();
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return new GridCacheProxyImpl<>(ctx, delegate, ctx.operationContextPerCall());
    }

    /**
     * @return {@code True} if proxy was closed.
     */
    @Override public boolean isProxyClosed() {
        return closed;
    }

    /**
     * Closes this proxy instance.
     */
    @Override public void closeProxy() {
        closed = true;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> lostPartitions() {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        return delegate.lostPartitions();
    }

    /** {@inheritDoc} */
    @Override public void enableStatistics(boolean enabled) {
        GridCacheContext<K, V> ctx = getContextSafe();

        try {
            ctx.kernalContext().cache().enableStatistics(Collections.singleton(getName()), enabled);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearStatistics() {
        GridCacheContext<K, V> ctx = getContextSafe();

        try {
            ctx.kernalContext().cache().clearStatistics(Collections.singleton(getName()));
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void preloadPartition(int part) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            delegate.preloadPartition(part);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> preloadPartitionAsync(int part) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            return (IgniteFuture<Void>)createFuture(delegate.preloadPartitionAsync(part));
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean localPreloadPartition(int part) {
        IgniteInternalCache<K, V> delegate = getDelegateSafe();

        try {
            return delegate.localPreloadPartition(part);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);

        out.writeObject(delegate);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridCacheContext<K, V>)in.readObject();

        delegate = (IgniteInternalCache<K, V>)in.readObject();

        cacheName = ctx.name();

        assert cacheName.equals(delegate.name()) : "ctx.name=" + cacheName + ", delegate.name=" + delegate.name();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> rebalance() {
        GridCacheContext<K, V> ctx = getContextSafe();

        return new IgniteFutureImpl<>(ctx.preloader().forceRebalance(), exec());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> indexReadyFuture() {
        GridCacheContext<K, V> ctx = getContextSafe();

        IgniteInternalFuture fut = ctx.shared().kernalContext().query().indexRebuildFuture(ctx.cacheId());

        if (fut == null)
            return new IgniteFinishedFutureImpl<>();

        return new IgniteFutureImpl<>(fut, exec());
    }

    /**
     * Throws {@code IgniteCacheRestartingException} if proxy is restarting.
     */
    public void checkRestart() {
        checkRestart(false);
    }

    /**
     * Throws {@code IgniteCacheRestartingException} if proxy is restarting.
     */
    public void checkRestart(boolean noWait) {
        RestartFuture curFut = restartFut.get();

        if (curFut != null) {
            try {
                if (!noWait) {
                    curFut.get(1, TimeUnit.SECONDS);

                    return;
                }
            }
            catch (IgniteCheckedException ignore) {
                //do nothing
            }

            throw new IgniteCacheRestartingException(new IgniteFutureImpl<>(curFut, exec()), cacheName);
        }
    }

    /**
     * @return True if proxy is restarting, false in other case.
     */
    public boolean isRestarting() {
        return restartFut.get() != null;
    }

    /**
     * Suspend this cache proxy.
     * To make cache proxy active again, it's needed to restart it.
     */
    public RestartFuture suspend() {
        while (true) {
            RestartFuture curFut = this.restartFut.get();

            if (curFut == null) {
                RestartFuture restartFut = new RestartFuture(cacheName);

                if (this.restartFut.compareAndSet(null, restartFut)) {
                    synchronized (this) {
                        if (!restartFut.isDone()) {
                            if (oldContext == null) {
                                oldContext = ctx;
                                delegate = null;
                                ctx = null;
                            }
                        }
                    }

                    return restartFut;
                }
            }
            else
                return curFut;
        }
    }

    /**
     * @param fut Finish restart future.
     */
    public void registrateFutureRestart(GridFutureAdapter<?> fut) {
        RestartFuture curFut = restartFut.get();

        if (curFut != null)
            curFut.addRestartFinishedFuture(fut);
    }

    /**
     * If proxy is already being restarted, returns future to wait on, else restarts this cache proxy.
     *
     * @param cache To use for restart proxy.
     */
    public void opportunisticRestart(IgniteInternalCache<K, V> cache) {
        RestartFuture restartFut = new RestartFuture(cacheName);

        while (true) {
            if (this.restartFut.compareAndSet(null, restartFut)) {
                onRestarted(cache.context(), cache.context().cache());

                return;
            }

            GridFutureAdapter<Void> curFut = this.restartFut.get();

            if (curFut != null) {
                try {
                    curFut.get();
                }
                catch (IgniteCheckedException ignore) {
                    // Do notrhing.
                }

                return;
            }
        }
    }

    /**
     * Mark this proxy as restarted.
     *
     * @param ctx New cache context.
     * @param delegate New delegate.
     */
    public void onRestarted(GridCacheContext ctx, IgniteInternalCache delegate) {
        RestartFuture restartFut = this.restartFut.get();

        assert restartFut != null;

        synchronized (this) {
            this.restartFut.compareAndSet(restartFut, null);

            this.ctx = ctx;
            oldContext = null;
            this.delegate = delegate;

            restartFut.onDone();
        }

        assert delegate == null || cacheName.equals(delegate.name()) && cacheName.equals(ctx.name()) :
                "ctx.name=" + ctx.name() + ", delegate.name=" + delegate.name() + ", cacheName=" + cacheName;
    }

    /**
     * Async continuation executor.
     */
    private Executor exec() {
        return context().kernalContext().getAsyncContinuationExecutor();
    }

    /**
     * @param ctx Cache context.
     * @param part Partition.
     * @return First, set of object changed in transaction, second, list of transaction data in required format.
     * @param <R> Required type.
     */
    private <R> IgniteBiTuple<Set<KeyCacheObject>, List<IgniteTxEntry>> transactionChanges(GridCacheContext<K, V> ctx, Integer part) {
        if (!U.isTxAwareQueriesEnabled(ctx))
            return F.t(Collections.emptySet(), Collections.emptyList());

        IgniteInternalTx tx = ctx.tm().tx();

        if (tx == null)
            return F.t(Collections.emptySet(), Collections.emptyList());

        IgniteTxManager.ensureTransactionModeSupported(tx.isolation());

        Set<KeyCacheObject> changedKeys = new HashSet<>();
        List<IgniteTxEntry> newAndUpdatedRows = new ArrayList<>();

        for (IgniteTxEntry e : tx.writeEntries()) {
            if (e.cacheId() != ctx.cacheId())
                continue;

            int epart = e.key().partition();

            assert epart != -1;

            if (part != null && epart != part)
                continue;

            changedKeys.add(e.key());

            // TODO: check entry processor.
            // Mix only updated or inserted entries. In case val == null entry removed.
            if (e.value() != null)
                newAndUpdatedRows.add(e);
        }

        return F.t(changedKeys, newAndUpdatedRows);
    }

    /**
     *
     */
    private class RestartFuture extends GridFutureAdapter<Void> {
        /** */
        private final String name;

        /** */
        private volatile GridFutureAdapter<?> restartFinishFut;

        /** */
        private RestartFuture(String name) {
            this.name = name;
        }

        /**
         *
         */
        void checkRestartOrAwait() {
            GridFutureAdapter<?> fut = restartFinishFut;

            if (fut != null) {
                try {
                    fut.get();
                }
                catch (IgniteCheckedException e) {
                    throw U.convertException(e);
                }

                return;
            }

            throw new IgniteCacheRestartingException(
                new IgniteFutureImpl<>(this, exec()),
                "Cache is restarting: " + name
            );
        }

        /**
         *
         */
        void addRestartFinishedFuture(GridFutureAdapter<?> fut) {
            restartFinishFut = fut;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteCacheProxyImpl.class, this);
    }
}
