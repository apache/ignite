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
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.EventType;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteAsyncCallback;

/**
 * Base class for continuous query.
 *
 * @see ContinuousQuery
 * @see ContinuousQueryWithTransformer
 */
public abstract class AbstractContinuousQuery<K, V> extends Query<Cache.Entry<K, V>> {
    /**
     * Default page size. Size of {@code 1} means that all entries
     * will be sent to master node immediately (buffering is disabled).
     */
    public static final int DFLT_PAGE_SIZE = 1;

    /** Maximum default time interval after which buffer will be flushed (if buffering is enabled). */
    public static final long DFLT_TIME_INTERVAL = 0;

    /**
     * Default value for automatic unsubscription flag. Remote filters
     * will be unregistered by default if master node leaves topology.
     */
    public static final boolean DFLT_AUTO_UNSUBSCRIBE = true;

    /** Initial query. */
    private Query<Cache.Entry<K, V>> initQry;

    /** Remote filter factory. */
    private Factory<? extends CacheEntryEventFilter<K, V>> rmtFilterFactory;

    /** Time interval. */
    private long timeInterval = DFLT_TIME_INTERVAL;

    /** Automatic unsubscription flag. */
    private boolean autoUnsubscribe = DFLT_AUTO_UNSUBSCRIBE;

    /** Whether to notify about {@link EventType#EXPIRED} events. */
    private boolean includeExpired;

    /**
     * Sets initial query.
     * <p>
     * This query will be executed before continuous listener is registered
     * which allows to iterate through entries which already existed at the
     * time continuous query is executed.
     *
     * @param initQry Initial query.
     * @return {@code this} for chaining.
     */
    public AbstractContinuousQuery<K, V> setInitialQuery(Query<Cache.Entry<K, V>> initQry) {
        this.initQry = initQry;

        return this;
    }

    /**
     * Gets initial query.
     *
     * @return Initial query.
     */
    public Query<Cache.Entry<K, V>> getInitialQuery() {
        return initQry;
    }

    /**
     * Sets optional key-value filter factory. This factory produces filter is called before entry is
     * sent to the master node.
     * <p>
     * <b>WARNING:</b> all operations that involve any kind of JVM-local or distributed locking
     * (e.g., synchronization or transactional cache operations), should be executed asynchronously
     * without blocking the thread that called the filter. Otherwise, you can get deadlocks.
     * <p>
     * If remote filter are annotated with {@link IgniteAsyncCallback} then it is executed in async callback
     * pool (see {@link IgniteConfiguration#getAsyncCallbackPoolSize()}) that allow to perform a cache operations.
     *
     * @param rmtFilterFactory Key-value filter factory.
     * @return {@code this} for chaining.
     * @see IgniteAsyncCallback
     * @see IgniteConfiguration#getAsyncCallbackPoolSize()
     */
    public AbstractContinuousQuery<K, V> setRemoteFilterFactory(
        Factory<? extends CacheEntryEventFilter<K, V>> rmtFilterFactory) {
        this.rmtFilterFactory = rmtFilterFactory;

        return this;
    }

    /**
     * Gets remote filter.
     *
     * @return Remote filter.
     */
    public Factory<? extends CacheEntryEventFilter<K, V>> getRemoteFilterFactory() {
        return rmtFilterFactory;
    }

    /**
     * Sets time interval.
     * <p>
     * When a cache update happens, entry is first put into a buffer. Entries from buffer will
     * be sent to the master node only if the buffer is full (its size can be provided via {@link #setPageSize(int)}
     * method) or time provided via this method is exceeded.
     * <p>
     * Default time interval is {@code 0} which means that
     * time check is disabled and entries will be sent only when buffer is full.
     *
     * @param timeInterval Time interval.
     * @return {@code this} for chaining.
     */
    public AbstractContinuousQuery<K, V> setTimeInterval(long timeInterval) {
        if (timeInterval < 0)
            throw new IllegalArgumentException("Time interval can't be negative.");

        this.timeInterval = timeInterval;

        return this;
    }

    /**
     * Gets time interval.
     *
     * @return Time interval.
     */
    public long getTimeInterval() {
        return timeInterval;
    }

    /**
     * Sets automatic unsubscribe flag.
     * <p>
     * This flag indicates that query filters on remote nodes should be
     * automatically unregistered if master node (node that initiated the query) leaves topology. If this flag is
     * {@code false}, filters will be unregistered only when the query is cancelled from master node, and won't ever be
     * unregistered if master node leaves grid.
     * <p>
     * Default value for this flag is {@code true}.
     *
     * @param autoUnsubscribe Automatic unsubscription flag.
     * @return {@code this} for chaining.
     */
    public AbstractContinuousQuery<K, V> setAutoUnsubscribe(boolean autoUnsubscribe) {
        this.autoUnsubscribe = autoUnsubscribe;

        return this;
    }

    /**
     * Gets automatic unsubscription flag value.
     *
     * @return Automatic unsubscription flag.
     */
    public boolean isAutoUnsubscribe() {
        return autoUnsubscribe;
    }

    /**
     * Sets the flag value defining whether to notify about {@link EventType#EXPIRED} events.
     * If {@code true}, then the remote listener will get notifications about entries
     * expired in cache. Otherwise, only {@link EventType#CREATED}, {@link EventType#UPDATED}
     * and {@link EventType#REMOVED} events will be fired in the remote listener.
     * <p>
     * This flag is {@code false} by default, so {@link EventType#EXPIRED} events are disabled.
     *
     * @param includeExpired Whether to notify about {@link EventType#EXPIRED} events.
     */
    public void setIncludeExpired(boolean includeExpired) {
        this.includeExpired = includeExpired;
    }

    /**
     * Gets the flag value defining whether to notify about {@link EventType#EXPIRED} events.
     *
     * @return Whether to notify about {@link EventType#EXPIRED} events.
     */
    public boolean isIncludeExpired() {
        return includeExpired;
    }
}
