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
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.CacheEntryListenerException;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.IgniteCache;
import javax.cache.event.EventType;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteAsyncCallback;

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
 * <h1 class="header">Example</h1>
 * As an example, suppose we have cache with {@code 'Person'} objects and we need
 * to query all persons with salary above 1000.
 * <p>
 * Here is the {@code Person} class:
 * <pre name="code" class="java">
 * public class Person {
 *     // Name.
 *     private String name;
 *
 *     // Salary.
 *     private double salary;
 *
 *     ...
 * }
 * </pre>
 * <p>
 * You can create and execute continuous query like so:
 * <pre name="code" class="java">
 * // Create new continuous query.
 * ContinuousQuery&lt;Long, Person&gt; qry = new ContinuousQuery&lt;&gt;();
 *
 * // Initial iteration query will return all persons with salary above 1000.
 * qry.setInitialQuery(new ScanQuery&lt;&gt;((id, p) -> p.getSalary() &gt; 1000));
 *
 *
 * // Callback that is called locally when update notifications are received.
 * // It simply prints out information about all created persons.
 * qry.setLocalListener((evts) -> {
 *     for (CacheEntryEvent&lt;? extends Long, ? extends Person&gt; e : evts) {
 *         Person p = e.getValue();
 *
 *         System.out.println(p.getFirstName() + " " + p.getLastName() + "'s salary is " + p.getSalary());
 *     }
 * });
 *
 * // Continuous listener will be notified for persons with salary above 1000.
 * qry.setRemoteFilter(evt -> evt.getValue().getSalary() &gt; 1000);
 *
 * // Execute query and get cursor that iterates through initial data.
 * QueryCursor&lt;Cache.Entry&lt;Long, Person&gt;&gt; cur = cache.query(qry);
 * </pre>
 * This will execute query on all nodes that have cache you are working with and
 * listener will start to receive notifications for cache updates.
 * <p>
 * To stop receiving updates call {@link QueryCursor#close()} method:
 * <pre name="code" class="java">
 * cur.close();
 * </pre>
 * Note that this works even if you didn't provide initial query. Cursor will
 * be empty in this case, but it will still unregister listeners when {@link QueryCursor#close()}
 * is called.
 * <p>
 * {@link IgniteAsyncCallback} annotation is supported for {@link CacheEntryEventFilter}
 * (see {@link #setRemoteFilterFactory(Factory)}) and {@link CacheEntryUpdatedListener}
 * (see {@link #setRemoteTransformerFactory(Factory)}) and {@link CacheEntryUpdatedListener}
 * (see {@link #setLocalTransformedEventListener(TransformedEventListener)} and {@link TransformedEventListener}).
 * If filter and/or listener are annotated with {@link IgniteAsyncCallback} then annotated callback
 * is executed in async callback pool (see {@link IgniteConfiguration#getAsyncCallbackPoolSize()})
 * and notification order is kept the same as update order for given cache key.
 *
 * @see ContinuousQuery
 * @see IgniteAsyncCallback
 * @see IgniteConfiguration#getAsyncCallbackPoolSize()
 */
public final class ContinuousQueryWithTransformer<K, V, T> extends Query<Cache.Entry<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default page size. Size of {@code 1} means that all entries
     * will be sent to master node immediately (buffering is disabled).
     */
    private static final int DFLT_PAGE_SIZE = 1;

    /** Maximum default time interval after which buffer will be flushed (if buffering is enabled). */
    private static final long DFLT_TIME_INTERVAL = 0;

    /**
     * Default value for automatic unsubscription flag. Remote filters
     * will be unregistered by default if master node leaves topology.
     */
    private static final boolean DFLT_AUTO_UNSUBSCRIBE = true;

    /** Initial query. */
    private Query<Cache.Entry<K, V>> initQry;

    /** Remote filter factory. */
    private Factory<? extends CacheEntryEventFilter<K, V>> rmtFilterFactory;

    /** Remote transformer factory. */
    private Factory<? extends IgniteBiClosure<K, V, T>> rmtTransFactory;

    /** Local listener of transformed event */
    private TransformedEventListener<T> locTransEvtLsnr;

    /** Time interval. */
    private long timeInterval = DFLT_TIME_INTERVAL;

    /** Automatic unsubscription flag. */
    private boolean autoUnsubscribe = DFLT_AUTO_UNSUBSCRIBE;

    /** Whether to notify about {@link EventType#EXPIRED} events. */
    private boolean includeExpired;

    /**
     * Creates new continuous query.
     */
    public ContinuousQueryWithTransformer() {
        setPageSize(DFLT_PAGE_SIZE);
    }

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
    public ContinuousQueryWithTransformer<K, V, T> setInitialQuery(Query<Cache.Entry<K, V>> initQry) {
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
    public ContinuousQueryWithTransformer<K, V, T> setRemoteFilterFactory(
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
    public ContinuousQueryWithTransformer<K, V, T> setTimeInterval(long timeInterval) {
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
    public ContinuousQueryWithTransformer<K, V, T> setAutoUnsubscribe(boolean autoUnsubscribe) {
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

    public ContinuousQueryWithTransformer<K, V, T> setRemoteTransformerFactory(
        Factory<? extends IgniteBiClosure<K, V, T>> factory) {
        this.rmtTransFactory = factory;
        return this;
    }

    /**
     * Gets remote transformer factory
     *
     * @return Remote Transformer Factory
     */
    public Factory<? extends IgniteBiClosure<K, V, T>> getRemoteTransformerFactory() {
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
     * @param locTransEvtLsnr Local callback.
     * @return {@code this} for chaining.
     *
     * @see IgniteAsyncCallback
     * @see IgniteConfiguration#getAsyncCallbackPoolSize()
     * @see ContinuousQuery#setLocalListener(CacheEntryUpdatedListener)
     */
    public ContinuousQueryWithTransformer<K, V, T> setLocalTransformedEventListener(
        TransformedEventListener<T> locTransEvtLsnr) {
        this.locTransEvtLsnr = locTransEvtLsnr;
        return this;
    }

    /**
     * Gets local transformed event listener
     *
     * @return local transformed event listener
     */
    public TransformedEventListener<T> getLocalTransformedEventListener() {
        return locTransEvtLsnr;
    }

    /**
     * Interface for listener to implement
     *
     * @param <T> type of data produced by transformer {@link #getRemoteTransformerFactory()}
     */
    public interface TransformedEventListener<T> {
        /**
         * Called after one or more entries have been updated.
         *
         * @param events The entries just updated transformed with #rmtTrans or #rmtTransFactory
         * @throws CacheEntryListenerException if there is problem executing the listener
         */
        void onUpdated(Iterable<? extends T> events) throws CacheEntryListenerException;
    }
}
