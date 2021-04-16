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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer.EventListener;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteAsyncCallback;

/**
 * API for configuring continuous cache queries.
 * <p>
 * Continuous queries allow registering a remote filter and a local listener
 * for cache updates. If an update event passes the filter, it will be sent to
 * the node that executed the query, and local listener will be notified.
 * <p>
 * Additionally, you can execute an initial query to get currently existing data.
 * Query can be of any type (SQL, TEXT or SCAN) and can be set via {@link #setInitialQuery(Query)}
 * method.
 * <p>
 * Query can be executed either on all nodes in topology using {@link IgniteCache#query(Query)}
 * method, or only on the local node, if {@link Query#setLocal(boolean)} parameter is set to {@code true}.
 * Note that if the query is distributed and a new node joins, it will get the remote
 * filter for the query during discovery process before it actually joins a topology,
 * so no updates will be missed.
 * <h1 class="header">Example</h1>
 * As an example, suppose we have a cache with {@code 'Person'} objects and we need
 * to query for all people with salary above 1000.
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
 * You can create and execute a continuous query like so:
 * <pre name="code" class="java">
 * // Create a new continuous query.
 * ContinuousQuery&lt;Long, Person&gt; qry = new ContinuousQuery&lt;&gt;();
 *
 * // Initial iteration query will return all people with salary above 1000.
 * qry.setInitialQuery(new ScanQuery&lt;&gt;((id, p) -> p.getSalary() &gt; 1000));
 *
 *
 * // Callback that is called locally when update notifications are received.
 * // It simply prints out information about all created or modified records.
 * qry.setLocalListener((evts) -> {
 *     for (CacheEntryEvent&lt;? extends Long, ? extends Person&gt; e : evts) {
 *         Person p = e.getValue();
 *
 *         System.out.println(p.getFirstName() + " " + p.getLastName() + "'s salary is " + p.getSalary());
 *     }
 * });
 *
 * // The continuous listener will be notified for people with salary above 1000.
 * qry.setRemoteFilter(evt -> evt.getValue().getSalary() &gt; 1000);
 *
 * // Execute the query and get a cursor that iterates through the initial data.
 * QueryCursor&lt;Cache.Entry&lt;Long, Person&gt;&gt; cur = cache.query(qry);
 * </pre>
 * This will execute query on all nodes that have the cache you are working with and
 * listener will start receiving notifications for cache updates.
 * <p>
 * To stop receiving updates call {@link QueryCursor#close()} method:
 * <pre name="code" class="java">
 * cur.close();
 * </pre>
 * Note that this works even if you didn't provide the initial query. Cursor will
 * be empty in this case, but it will still unregister listeners when {@link QueryCursor#close()}
 * is called.
 * <p>
 * {@link IgniteAsyncCallback} annotation is supported for {@link CacheEntryEventFilter}
 * (see {@link #setRemoteFilterFactory(Factory)}) and {@link CacheEntryUpdatedListener}
 * (see {@link #setLocalListener(CacheEntryUpdatedListener)}).
 * If a filter and/or listener are annotated with {@link IgniteAsyncCallback} then the annotated callback
 * is executed in an async callback pool (see {@link IgniteConfiguration#getAsyncCallbackPoolSize()})
 * and a notification order is kept the same as an update order for a given cache key.
 *
 * @see ContinuousQueryWithTransformer
 * @see IgniteAsyncCallback
 * @see IgniteConfiguration#getAsyncCallbackPoolSize()
 */
public final class ContinuousQuery<K, V> extends AbstractContinuousQuery<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Local listener. */
    private CacheEntryUpdatedListener<K, V> locLsnr;

    /** Remote filter. */
    private CacheEntryEventSerializableFilter<K, V> rmtFilter;

    /**
     * Creates new continuous query.
     */
    public ContinuousQuery() {
        setPageSize(DFLT_PAGE_SIZE);
    }

    /** {@inheritDoc} */
    @Override public ContinuousQuery<K, V> setInitialQuery(Query<Cache.Entry<K, V>> initQry) {
        return (ContinuousQuery<K, V>)super.setInitialQuery(initQry);
    }

    /**
     * Sets a local callback. This callback is called only on local node when new updates are received.
     * <p>
     * The callback predicate accepts ID of the node from where updates are received and a collection
     * of the received entries. Note that for removed entries values will be {@code null}.
     * <p>
     * If the predicate returns {@code false}, query execution will be cancelled.
     * <p>
     * <b>WARNING:</b> all operations that involve any kind of JVM-local or distributed locking (e.g.,
     * synchronization or transactional cache operations), should be executed asynchronously without
     * blocking the thread that called the callback. Otherwise, you can get deadlocks.
     * <p>
     * If local listener are annotated with {@link IgniteAsyncCallback} then it is executed in an async callback pool
     * (see {@link IgniteConfiguration#getAsyncCallbackPoolSize()}) that allow to perform a cache operations.
     *
     * @param locLsnr Local callback.
     * @return {@code this} for chaining.
     * @see IgniteAsyncCallback
     * @see IgniteConfiguration#getAsyncCallbackPoolSize()
     * @see ContinuousQueryWithTransformer#setLocalListener(EventListener)
     */
    public ContinuousQuery<K, V> setLocalListener(CacheEntryUpdatedListener<K, V> locLsnr) {
        this.locLsnr = locLsnr;

        return this;
    }

    /**
     * @return Local listener.
     */
    public CacheEntryUpdatedListener<K, V> getLocalListener() {
        return locLsnr;
    }

    /**
     * Sets optional key-value filter. This filter is called before entry is sent to the master node.
     * <p>
     * <b>WARNING:</b> all operations that involve any kind of JVM-local or distributed locking
     * (e.g., synchronization or transactional cache operations), should be executed asynchronously
     * without blocking the thread that called the filter. Otherwise, you can get deadlocks.
     * <p>
     * If remote filter are annotated with {@link IgniteAsyncCallback} then it is executed in async callback
     * pool (see {@link IgniteConfiguration#getAsyncCallbackPoolSize()}) that allow to perform a cache operations.
     *
     * @param rmtFilter Key-value filter.
     * @return {@code this} for chaining.
     *
     * @deprecated Use {@link #setRemoteFilterFactory(Factory)} instead.
     * @see IgniteAsyncCallback
     * @see IgniteConfiguration#getAsyncCallbackPoolSize()
     */
    @Deprecated
    public ContinuousQuery<K, V> setRemoteFilter(CacheEntryEventSerializableFilter<K, V> rmtFilter) {
        this.rmtFilter = rmtFilter;

        return this;
    }

    /**
     * Gets remote filter.
     *
     * @return Remote filter.
     */
    public CacheEntryEventSerializableFilter<K, V> getRemoteFilter() {
        return rmtFilter;
    }

    /** {@inheritDoc} */
    @Override public ContinuousQuery<K, V> setTimeInterval(long timeInterval) {
        return (ContinuousQuery<K, V>)super.setTimeInterval(timeInterval);
    }

    /** {@inheritDoc} */
    @Override public ContinuousQuery<K, V> setAutoUnsubscribe(boolean autoUnsubscribe) {
        return (ContinuousQuery<K, V>)super.setAutoUnsubscribe(autoUnsubscribe);
    }

    /** {@inheritDoc} */
    @Override public ContinuousQuery<K, V> setPageSize(int pageSize) {
        return (ContinuousQuery<K, V>)super.setPageSize(pageSize);
    }

    /**
     * Sets whether this query should be executed on a local node only.
     *
     * Note: backup event queues are not kept for local continuous queries. It may lead to loss of notifications in case
     * of node failures. Use {@link ContinuousQuery#setRemoteFilterFactory(Factory)} to register cache event listeners
     * on all cache nodes, if delivery guarantee is required.
     *
     * @param loc Local flag.
     * @return {@code this} for chaining.
     */
    @Override public ContinuousQuery<K, V> setLocal(boolean loc) {
        return (ContinuousQuery<K, V>)super.setLocal(loc);
    }
}
