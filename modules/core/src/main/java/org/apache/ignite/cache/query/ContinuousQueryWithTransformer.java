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

package org.apache.ignite.cache.query;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteClosure;

/**
 * API for configuring continuous cache queries with transformer.
 * <p>
 * Continuous queries allow to register a remote filter and a local listener
 * for cache updates. If an update event passes the filter, it will be transformed with transformer and sent to
 * the node that executed the query and local listener will be notified.
 * <p>
 * Additionally, you can execute initial query to get currently existing data.
 * Query can be of any type (SQL, TEXT or SCAN) and can be set via {@link #setInitialQuery(Query)}
 * method.
 * <p>
 * Query can be executed either on all nodes in topology using {@link IgniteCache#query(Query)}
 * method, or only on the local node, if {@link Query#setLocal(boolean)} parameter is set to {@code true}.
 * Note that in case query is distributed and a new node joins, it will get the remote
 * filter for the query during discovery process before it actually joins topology,
 * so no updates will be missed.
 * This will execute query on all nodes that have cache you are working with and
 * listener will start to receive notifications for cache updates.
 * <p>
 * To stop receiving updates call {@link QueryCursor#close()} method.
 * Note that this works even if you didn't provide initial query. Cursor will
 * be empty in this case, but it will still unregister listeners when {@link QueryCursor#close()}
 * is called.
 * <p>
 * {@link IgniteAsyncCallback} annotation is supported for {@link CacheEntryEventFilter}
 * (see {@link #setRemoteFilterFactory(Factory)}) and {@link CacheEntryUpdatedListener}
 * (see {@link #setRemoteTransformerFactory(Factory)}) and {@link CacheEntryUpdatedListener}
 * (see {@link #setLocalListener(EventListener)} and {@link EventListener}).
 * If filter and/or listener are annotated with {@link IgniteAsyncCallback} then annotated callback
 * is executed in async callback pool (see {@link IgniteConfiguration#getAsyncCallbackPoolSize()})
 * and notification order is kept the same as update order for given cache key.
 *
 * @see ContinuousQuery
 * @see IgniteAsyncCallback
 * @see IgniteConfiguration#getAsyncCallbackPoolSize()
 */
public final class ContinuousQueryWithTransformer<K, V, T> extends AbstractContinuousQuery<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Remote transformer factory. */
    private Factory<? extends IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, T>> rmtTransFactory;

    /** Local listener of transformed event */
    private EventListener<T> locLsnr;

    /**
     * Creates new continuous query with transformer.
     */
    public ContinuousQueryWithTransformer() {
        setPageSize(DFLT_PAGE_SIZE);
    }

    /** {@inheritDoc} */
    @Override public ContinuousQueryWithTransformer<K, V, T> setInitialQuery(Query<Cache.Entry<K, V>> initQry) {
        return (ContinuousQueryWithTransformer<K, V, T>)super.setInitialQuery(initQry);
    }

    /** {@inheritDoc} */
    @Override public ContinuousQueryWithTransformer<K, V, T> setRemoteFilterFactory(
        Factory<? extends CacheEntryEventFilter<K, V>> rmtFilterFactory) {
        return (ContinuousQueryWithTransformer<K, V, T>)super.setRemoteFilterFactory(rmtFilterFactory);
    }

    /**
     * Sets transformer factory. This factory produces transformer is called after and only if entry passes the filter.
     * <p>
     * <b>WARNING:</b> all operations that involve any kind of JVM-local or distributed locking
     * (e.g., synchronization or transactional cache operations), should be executed asynchronously
     * without blocking the thread that called the filter. Otherwise, you can get deadlocks.
     * <p>
     *
     * @param factory Remote transformer factory.
     * @return {@code this} for chaining.
     */
    public ContinuousQueryWithTransformer<K, V, T> setRemoteTransformerFactory(
        Factory<? extends IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, T>> factory) {
        this.rmtTransFactory = factory;

        return this;
    }

    /**
     * Gets remote transformer factory
     *
     * @return Remote Transformer Factory
     */
    public Factory<? extends IgniteClosure<CacheEntryEvent<? extends K, ? extends V>, T>> getRemoteTransformerFactory() {
        return rmtTransFactory;
    }

    /**
     * Sets local callback. This callback is called only in local node when new updates are received.
     * <p>
     * The callback predicate accepts results of transformed by {@link #getRemoteFilterFactory()} events
     * <p>
     * <b>WARNING:</b> all operations that involve any kind of JVM-local or distributed locking (e.g.,
     * synchronization or transactional cache operations), should be executed asynchronously without
     * blocking the thread that called the callback. Otherwise, you can get deadlocks.
     * <p>
     * If local listener are annotated with {@link IgniteAsyncCallback} then it is executed in async callback pool
     * (see {@link IgniteConfiguration#getAsyncCallbackPoolSize()}) that allow to perform a cache operations.
     *
     * @param locLsnr Local callback.
     * @return {@code this} for chaining.
     *
     * @see IgniteAsyncCallback
     * @see IgniteConfiguration#getAsyncCallbackPoolSize()
     * @see ContinuousQuery#setLocalListener(CacheEntryUpdatedListener)
     */
    public ContinuousQueryWithTransformer<K, V, T> setLocalListener(EventListener<T> locLsnr) {
        this.locLsnr = locLsnr;

        return this;
    }

    /**
     * Gets local transformed event listener
     *
     * @return local transformed event listener
     */
    public EventListener<T> getLocalListener() {
        return locLsnr;
    }

    /** {@inheritDoc} */
    @Override public ContinuousQueryWithTransformer<K, V, T> setTimeInterval(long timeInterval) {
        return (ContinuousQueryWithTransformer<K, V, T>)super.setTimeInterval(timeInterval);
    }

    /** {@inheritDoc} */
    @Override public ContinuousQueryWithTransformer<K, V, T> setAutoUnsubscribe(boolean autoUnsubscribe) {
        return (ContinuousQueryWithTransformer<K, V, T>)super.setAutoUnsubscribe(autoUnsubscribe);
    }

    /** {@inheritDoc} */
    @Override public ContinuousQueryWithTransformer<K, V, T> setPageSize(int pageSize) {
        return (ContinuousQueryWithTransformer<K, V, T>)super.setPageSize(pageSize);
    }

    /** {@inheritDoc} */
    @Override public ContinuousQueryWithTransformer<K, V, T> setLocal(boolean loc) {
        return (ContinuousQueryWithTransformer<K, V, T>)super.setLocal(loc);
    }

    /**
     * Interface for local listener of {@link ContinuousQueryWithTransformer} to implement.
     * Invoked if an cache entry is updated, created or if a batch call is made,
     * after the entries are updated and transformed.
     *
     * @param <T> type of data produced by transformer {@link ContinuousQueryWithTransformer#getRemoteTransformerFactory()}.
     * @see ContinuousQueryWithTransformer
     * @see ContinuousQueryWithTransformer#setLocalListener(EventListener)
     */
    public interface EventListener<T> {
        /**
         * Called after one or more entries have been updated.
         *
         * @param events The entries just updated that transformed with remote transformer of {@link ContinuousQueryWithTransformer}.
         */
        void onUpdated(Iterable<? extends T> events);
    }
}
