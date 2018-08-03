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
import org.apache.ignite.cache.query.AbstractContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer.EventListener;
import org.apache.ignite.cache.query.FieldsQueryCursor;
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
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.IgniteOutClosureX;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
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

    /** Context. */
    private volatile GridCacheContext<K, V> ctx;

    /** Delegate. */
    @GridToStringInclude
    private volatile IgniteInternalCache<K, V> delegate;

    /** Cached proxy wrapper. */
    private volatile IgniteCacheProxy<K, V> cachedProxy;

    /** */
    @GridToStringExclude
    private CacheManager cacheMgr;

    /** Future indicates that cache is under restarting. */
    private final AtomicReference<GridFutureAdapter<Void>> restartFut;

    /** Flag indicates that proxy is closed. */
    private volatile boolean closed;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public IgniteCacheProxyImpl() {
        restartFut = new AtomicReference<GridFutureAdapter<Void>>(null);
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
        this(ctx, delegate, new AtomicReference<GridFutureAdapter<Void>>(null), async);
    }

    /**
     * @param ctx Context.
     * @param delegate Delegate.
     * @param async Async support flag.
     */
    private IgniteCacheProxyImpl(
        @NotNull GridCacheContext<K, V> ctx,
        @NotNull IgniteInternalCache<K, V> delegate,
        @NotNull AtomicReference<GridFutureAdapter<Void>> restartFut,
        boolean async
    ) {
        super(async);

        assert ctx != null;
        assert delegate != null;

        this.ctx = ctx;
        this.delegate = delegate;

        this.restartFut = restartFut;
    }

    /**
     * @return Context.
     */
    @Override public GridCacheContext<K, V> context() {
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
        return ctx.cache().clusterMetrics();
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics metrics(ClusterGroup grp) {
        return ctx.cache().clusterMetrics(grp);
    }

    /** {@inheritDoc} */
    @Override public CacheMetrics localMetrics() {
        return ctx.cache().localMetrics();
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean mxBean() {
        return ctx.cache().clusterMxBean();
    }

    /** {@inheritDoc} */
    @Override public CacheMetricsMXBean localMxBean() {
        return ctx.cache().localMxBean();
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
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
    @Override public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
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
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> loadCacheAsync(@Nullable IgniteBiPredicate<K, V> p,
        @Nullable Object... args) throws CacheException {
        try {
            if (ctx.cache().isLocal())
                return (IgniteFuture<Void>)createFuture(ctx.cache().localLoadCacheAsync(p, args));

            return (IgniteFuture<Void>)createFuture(ctx.cache().globalLoadCacheAsync(p, args));
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
   }

    /** {@inheritDoc} */
    @Override public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) {
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
        return (IgniteFuture<Void>)createFuture(delegate.localLoadCacheAsync(p, args));
    }

    /** {@inheritDoc} */
    @Nullable @Override public V getAndPutIfAbsent(K key, V val) throws CacheException {
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
        return createFuture(delegate.getAndPutIfAbsentAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public Lock lock(K key) throws CacheException {
        return lockAll(Collections.singleton(key));
    }

    /** {@inheritDoc} */
    @Override public Lock lockAll(final Collection<? extends K> keys) {
        return new CacheLockImpl<>(ctx.gate(), delegate, ctx.operationContextPerCall(), keys);
    }

    /** {@inheritDoc} */
    @Override public boolean isLocalLocked(K key, boolean byCurrThread) {
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
    private <T, R> QueryCursor<R> query(
        final ScanQuery scanQry,
        @Nullable final IgniteClosure<T, R> transformer,
        @Nullable ClusterGroup grp)
        throws IgniteCheckedException {

        CacheOperationContext opCtxCall = ctx.operationContextPerCall();

        boolean isKeepBinary = opCtxCall != null && opCtxCall.isKeepBinary();

        IgniteBiPredicate<K, V> p = scanQry.getFilter();

        final CacheQuery<R> qry = ctx.queries().createScanQuery(p, transformer, scanQry.getPartition(), isKeepBinary);

        if (scanQry.getPageSize() > 0)
            qry.pageSize(scanQry.getPageSize());

        if (grp != null)
            qry.projection(grp);

        final GridCloseableIterator<R> iter = ctx.kernalContext().query().executeQuery(GridCacheQueryType.SCAN,
            ctx.name(), ctx, new IgniteOutClosureX<GridCloseableIterator<R>>() {
                @Override public GridCloseableIterator<R> applyx() throws IgniteCheckedException {
                    return qry.executeScanQuery();
                }
            }, true);

        return new QueryCursorImpl<>(iter);
    }

    /**
     * @param filter Filter.
     * @param grp Optional cluster group.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private QueryCursor<Cache.Entry<K, V>> query(final Query filter, @Nullable ClusterGroup grp)
        throws IgniteCheckedException {
        final CacheQuery qry;

        CacheOperationContext opCtxCall = ctx.operationContextPerCall();

        boolean isKeepBinary = opCtxCall != null && opCtxCall.isKeepBinary();

        final CacheQueryFuture fut;

        if (filter instanceof TextQuery) {
            TextQuery p = (TextQuery)filter;

            qry = ctx.queries().createFullTextQuery(p.getType(), p.getText(), isKeepBinary);

            if (grp != null)
                qry.projection(grp);

            fut = ctx.kernalContext().query().executeQuery(GridCacheQueryType.TEXT, p.getText(), ctx,
                new IgniteOutClosureX<CacheQueryFuture<Map.Entry<K, V>>>() {
                    @Override public CacheQueryFuture<Map.Entry<K, V>> applyx() {
                        return qry.execute();
                    }
                }, false);
        }
        else if (filter instanceof SpiQuery) {
            qry = ctx.queries().createSpiQuery(isKeepBinary);

            if (grp != null)
                qry.projection(grp);

            fut = ctx.kernalContext().query().executeQuery(GridCacheQueryType.SPI, filter.getClass().getSimpleName(),
                ctx, new IgniteOutClosureX<CacheQueryFuture<Map.Entry<K, V>>>() {
                    @Override public CacheQueryFuture<Map.Entry<K, V>> applyx() {
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
        if (loc || ctx.isLocal() || ctx.isReplicatedAffinityNode())
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
     * @param keepBinary Keep binary flag.
     * @return Initial iteration cursor.
     */
    @SuppressWarnings("unchecked")
    private QueryCursor<Cache.Entry<K, V>> queryContinuous(AbstractContinuousQuery qry, boolean loc, boolean keepBinary) {
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

            if (qry0.getLocalListener() == null)
                throw new IgniteException("Mandatory local listener is not set for the query: " + qry);

            if (qry0.getRemoteFilter() != null && qry0.getRemoteFilterFactory() != null)
                throw new IgniteException("Should be used either RemoterFilter or RemoteFilterFactory.");

            locLsnr = qry0.getLocalListener();

            rmtFilter = qry0.getRemoteFilter();
        }
        else {
            ContinuousQueryWithTransformer<K, V, ?> qry0 = (ContinuousQueryWithTransformer<K, V, ?>)qry;

            if (qry0.getLocalListener() == null)
                throw new IgniteException("Mandatory local transformed event listener is not set for the query: " + qry);

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
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        return (FieldsQueryCursor<List<?>>)query((Query)qry);
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> queryMultipleStatements(SqlFieldsQuery qry) {
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
    @SuppressWarnings("unchecked")
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
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
                return query((ScanQuery)qry, null, projection(qry.isLocal()));

            return (QueryCursor<R>)query(qry, projection(qry.isLocal()));
        }
        catch (Exception e) {
            if (e instanceof CacheException)
                throw (CacheException)e;

            throw new CacheException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> QueryCursor<R> query(Query<T> qry, IgniteClosure<T, R> transformer) {
        A.notNull(qry, "qry");
        A.notNull(transformer, "transformer");

        if (!(qry instanceof ScanQuery))
            throw new UnsupportedOperationException("Transformers are supported only for SCAN queries.");

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
     * Checks query.
     *
     * @param qry Query
     * @throws CacheException If query indexing disabled for sql query.
     */
    private void validate(Query qry) {
        if (!QueryUtils.isEnabled(ctx.config()) && !(qry instanceof ScanQuery) &&
            !(qry instanceof ContinuousQuery) && !(qry instanceof ContinuousQueryWithTransformer) &&
            !(qry instanceof SpiQuery) && !(qry instanceof SqlQuery) && !(qry instanceof SqlFieldsQuery))
            throw new CacheException("Indexing is disabled for cache: " + ctx.cache().name() +
                    ". Use setIndexedTypes or setTypeMetadata methods on CacheConfiguration to enable.");

        if (!ctx.kernalContext().query().moduleEnabled() &&
            (qry instanceof SqlQuery || qry instanceof SqlFieldsQuery || qry instanceof TextQuery))
            throw new CacheException("Failed to execute query. Add module 'ignite-indexing' to the classpath " +
                    "of all Ignite nodes.");

        if (qry.isLocal() && (qry instanceof SqlQuery) && ctx.kernalContext().clientNode())
            throw new CacheException("Execution of local sql query on client node disallowed.");
    }

    /** {@inheritDoc} */
    @Override public Iterable<Cache.Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException {
        try {
            return delegate.localEntries(peekModes);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics queryMetrics() {
        return delegate.context().queries().metrics();
    }

    /** {@inheritDoc} */
    @Override public void resetQueryMetrics() {
        delegate.context().queries().resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends QueryDetailMetrics> queryDetailMetrics() {
        return delegate.context().queries().detailMetrics();
    }

    /** {@inheritDoc} */
    @Override public void resetQueryDetailMetrics() {
        delegate.context().queries().resetDetailMetrics();
    }

    /** {@inheritDoc} */
    @Override public void localEvict(Collection<? extends K> keys) {
        delegate.evictAll(keys);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V localPeek(K key, CachePeekMode... peekModes) {
        try {
            return delegate.localPeek(key, peekModes, null);
        }
        catch (IgniteException | IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int size(CachePeekMode... peekModes) throws CacheException {
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
        return createFuture(delegate.sizeAsync(peekModes));
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(CachePeekMode... peekModes) throws CacheException {
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
        return createFuture(delegate.sizeLongAsync(peekModes));
    }

    /** {@inheritDoc} */
    @Override public long sizeLong(int part, CachePeekMode... peekModes) throws CacheException {
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
        return createFuture(delegate.sizeLongAsync(part, peekModes));
    }

    /** {@inheritDoc} */
    @Override public int localSize(CachePeekMode... peekModes) {
        try {
            return delegate.localSize(peekModes);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(CachePeekMode... peekModes) {
        try {
            return delegate.localSizeLong(peekModes);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long localSizeLong(int part, CachePeekMode... peekModes) {
        try {
            return delegate.localSizeLong(part, peekModes);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
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
        return createFuture(delegate.getAsync(key));
    }

    /** {@inheritDoc} */
    @Override public CacheEntry<K, V> getEntry(K key) {
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
        return createFuture(delegate.getEntryAsync(key));
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
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
        return createFuture(delegate.getAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheEntry<K, V>> getEntries(Set<? extends K> keys) {
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
        return createFuture(delegate.getEntriesAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAllOutTx(Set<? extends K> keys) {
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
        return createFuture(delegate.getAllOutTxAsync(keys));
    }

    /**
     * @param keys Keys.
     * @return Values map.
     */
    public Map<K, V> getAll(Collection<? extends K> keys) {
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
        if (isAsync()) {
            setFuture(delegate.containsKeyAsync(key));

            return false;
        }
        else
            return delegate.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeyAsync(K key) {
        return createFuture(delegate.containsKeyAsync(key));
    }

    /** {@inheritDoc} */
    @Override public boolean containsKeys(Set<? extends K> keys) {
        if (isAsync()) {
            setFuture(delegate.containsKeysAsync(keys));

            return false;
        }
        else
            return delegate.containsKeys(keys);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> containsKeysAsync(Set<? extends K> keys) {
        return createFuture(delegate.containsKeysAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public void loadAll(
        Set<? extends K> keys,
        boolean replaceExisting,
        @Nullable final CompletionListener completionLsnr
    ) {
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
        return createFuture(delegate.getAndPutAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
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
        return (IgniteFuture<Void>)createFuture(delegate.putAllAsync(map));
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
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
        return createFuture(delegate.putIfAbsentAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
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
        return createFuture(delegate.removeAsync(key));
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
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
        return createFuture(delegate.removeAsync(key, oldVal));
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
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
        return createFuture(delegate.getAndRemoveAsync(key));
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
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
        return createFuture(delegate.replaceAsync(key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
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
        return createFuture(delegate.replaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
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
        return createFuture(delegate.getAndReplaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) {
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
        return (IgniteFuture<Void>)createFuture(delegate.removeAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
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
        return (IgniteFuture<Void>)createFuture(delegate.removeAllAsync());
    }

    /** {@inheritDoc} */
    @Override public void clear(K key) {
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
        return (IgniteFuture<Void>)createFuture(delegate.clearAsync(key));
    }

    /** {@inheritDoc} */
    @Override public void clearAll(Set<? extends K> keys) {
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
        return (IgniteFuture<Void>)createFuture(delegate.clearAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public void clear() {
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
        return (IgniteFuture<Void>)createFuture(delegate.clearAsync());
    }

    /** {@inheritDoc} */
    @Override public void localClear(K key) {
        delegate.clearLocally(key);
    }

    /** {@inheritDoc} */
    @Override public void localClearAll(Set<? extends K> keys) {
        for (K key : keys)
            delegate.clearLocally(key);
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... args)
        throws EntryProcessorException {
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
        Object... args) {
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
        Object... args) {
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
        return createFuture(delegate.invokeAllAsync(keys, entryProcessor, args));
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor,
        Object... args) {
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
        return createFuture(delegate.invokeAllAsync(keys, entryProcessor, args));
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) {
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
        return createFuture(delegate.invokeAllAsync(map, args));
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
        destroyAsync().get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> destroyAsync() {
        return new IgniteFutureImpl<>(ctx.kernalContext().cache().dynamicDestroyCache(ctx.name(), false, true, false));
    }

    /** {@inheritDoc} */
    @Override public void close() {
        closeAsync().get();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> closeAsync() {
        return new IgniteFutureImpl<>(ctx.kernalContext().cache().dynamicCloseCache(ctx.name()));
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return ctx.kernalContext().cache().context().closed(ctx);
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
        try {
            ctx.continuousQueries().cancelJCacheQuery(lsnrCfg);
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        try {
            return ctx.cache().igniteIterator();
        }
        catch (IgniteCheckedException | IgniteException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<K, V> createAsyncInstance() {
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
    @SuppressWarnings("unchecked")
    @Override public <K1, V1> IgniteCache<K1, V1> keepBinary() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param dataCenterId Data center ID.
     * @return Projection for data center id.
     */
    @SuppressWarnings("unchecked")
    @Override public IgniteCache<K, V> withDataCenterId(byte dataCenterId) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return Cache with skip store enabled.
     */
    @Override public IgniteCache<K, V> skipStore() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withAllowAtomicOpsInTx() {
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

        if (restartFut != null) {
            if (X.hasCause(e, CacheStoppedException.class) || X.hasSuppressed(e, CacheStoppedException.class))
                throw new IgniteCacheRestartingException(new IgniteFutureImpl<>(restartFut), "Cache is restarting: " +
                        ctx.name(), e);
        }

        if (e instanceof IgniteException && X.hasCause(e, CacheException.class))
            e = X.cause(e, CacheException.class);

        if (e instanceof IgniteCheckedException)
            return CU.convertToCacheException((IgniteCheckedException) e);

        if (X.hasCause(e, CacheStoppedException.class))
            return CU.convertToCacheException(X.cause(e, CacheStoppedException.class));

        if (e instanceof RuntimeException)
            return (RuntimeException) e;

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
        return new IgniteCacheFutureImpl<>(fut);
    }

    /**
     * @return Internal proxy.
     */
    @Override public GridCacheProxyImpl<K, V> internalProxy() {
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
        return delegate.lostPartitions();
    }

    /** {@inheritDoc} */
    @Override public void enableStatistics(boolean enabled) {
        try {
            ctx.kernalContext().cache().enableStatistics(Collections.singleton(getName()), enabled);
        }
        catch (IgniteCheckedException e) {
            throw cacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearStatistics() {
        try {
            ctx.kernalContext().cache().clearStatistics(Collections.singleton(getName()));
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
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridCacheContext<K, V>)in.readObject();

        delegate = (IgniteInternalCache<K, V>)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> rebalance() {
        return new IgniteFutureImpl<>(ctx.preloader().forceRebalance());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> indexReadyFuture() {
        IgniteInternalFuture fut = ctx.shared().database().indexRebuildFuture(ctx.cacheId());

        if (fut == null)
            return new IgniteFinishedFutureImpl<>();

        return new IgniteFutureImpl<>(fut);
    }

    /**
     * Throws {@code IgniteCacheRestartingException} if proxy is restarting.
     */
    public void checkRestart() {
        GridFutureAdapter<Void> currentFut = this.restartFut.get();

        if (currentFut != null)
            throw new IgniteCacheRestartingException(new IgniteFutureImpl<>(currentFut), "Cache is restarting: " +
                    context().name());
    }

    /**
     * @return True if proxy is restarting, false in other case.
     */
    public boolean isRestarting() {
        return restartFut.get() != null;
    }

    /**
     * Restarts this cache proxy.
     */
    public boolean restart() {
        GridFutureAdapter<Void> restartFut = new GridFutureAdapter<>();

        final GridFutureAdapter<Void> curFut = this.restartFut.get();

        boolean changed = this.restartFut.compareAndSet(curFut, restartFut);

        if (changed && curFut != null)
            restartFut.listen(new IgniteInClosure<IgniteInternalFuture<Void>>() {
                @Override public void apply(IgniteInternalFuture<Void> fut) {
                    if (fut.error() != null)
                        curFut.onDone(fut.error());
                    else
                        curFut.onDone();
                }
            });

        return changed;
    }

    /**
     * If proxy is already being restarted, returns future to wait on, else restarts this cache proxy.
     *
     * @return Future to wait on, or null.
     */
    public GridFutureAdapter<Void> opportunisticRestart() {
        GridFutureAdapter<Void> restartFut = new GridFutureAdapter<>();

        while (true) {
            if (this.restartFut.compareAndSet(null, restartFut))
                return null;

            GridFutureAdapter<Void> curFut = this.restartFut.get();

            if (curFut != null)
                return curFut;
        }
    }

    /**
     * Mark this proxy as restarted.
     *
     * @param ctx New cache context.
     * @param delegate New delegate.
     */
    public void onRestarted(GridCacheContext ctx, IgniteInternalCache delegate) {
        GridFutureAdapter<Void> restartFut = this.restartFut.get();

        assert restartFut != null;

        this.ctx = ctx;
        this.delegate = delegate;

        this.restartFut.compareAndSet(restartFut, null);

        restartFut.onDone();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteCacheProxyImpl.class, this);
    }
}
