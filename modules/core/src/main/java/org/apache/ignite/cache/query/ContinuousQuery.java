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

import org.apache.ignite.*;

import javax.cache.event.*;

/**
 * API for configuring continuous cache queries.
 * <p>
 * Continuous queries allow to register a remote filter and a local listener
 * for cache updates. If an update event passes the filter, it will be sent to
 * the node that executed the query and local listener will be notified.
 * <p>
 * Additionally, you can execute initial query to get currently existing data.
 * Query can be of any type (SQL, TEXT or SCAN) and can be set via {@link #setInitialPredicate(Query)}
 * method.
 * <p>
 * Query can be executed either on all nodes in topology using {@link IgniteCache#query(Query)}
 * method of only on the local node using {@link IgniteCache#localQuery(Query)} method.
 * Note that in case query is distributed and a new node joins, it will get the remote
 * filter for the query during discovery process before it actually joins topology,
 * so no updates will be missed.
 * <p>
 * To create a new instance of continuous query use {@link Query#continuous()} factory method.
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
 * ContinuousQuery qry = Query.continuous();
 *
 * // Initial iteration query will return all persons with salary above 1000.
 * qry.setInitialPredicate(Query.scan(new IgniteBiPredicate&lt;UUID, Person&gt;() {
 *     &#64;Override public boolean apply(UUID id, Person p) {
 *         return p.getSalary() &gt; 1000;
 *     }
 * }));
 *
 *
 * // Callback that is called locally when update notifications are received.
 * // It simply prints out information about all created persons.
 * qry.setLocalListener(new CacheEntryUpdatedListener&lt;UUID, Person&gt;() {
 *     &#64;Override public void onUpdated(Iterable&lt;CacheEntryEvent&lt;? extends UUID, ? extends Person&gt;&gt; evts) {
 *         for (CacheEntryEvent&lt;? extends UUID, ? extends Person&gt; e : evts) {
 *             Person p = e.getValue();
 *
 *             X.println("&gt;&gt;&gt;");
 *             X.println("&gt;&gt;&gt; " + p.getFirstName() + " " + p.getLastName() +
 *                 "'s salary is " + p.getSalary());
 *             X.println("&gt;&gt;&gt;");
 *         }
 *     }
 * });
 *
 * // Continuous listener will be notified for persons with salary above 1000.
 * qry.setRemoteFilter(new CacheEntryEventFilter&lt;UUID, Person&gt;() {
 *     &#64;Override public boolean evaluate(CacheEntryEvent&lt;? extends UUID, ? extends Person&gt; e) {
 *         return e.getValue().getSalary() &gt; 1000;
 *     }
 * });
 *
 * // Execute query and get cursor that iterates through initial data.
 * QueryCursor&lt;Cache.Entry&lt;UUID, Person&gt;&gt; cur = cache.query(qry);
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
 */
public final class ContinuousQuery<K, V> extends Query<ContinuousQuery<K,V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default buffer size. Size of {@code 1} means that all entries
     * will be sent to master node immediately (buffering is disabled).
     */
    public static final int DFLT_BUF_SIZE = 1;

    /** Maximum default time interval after which buffer will be flushed (if buffering is enabled). */
    public static final long DFLT_TIME_INTERVAL = 0;

    /**
     * Default value for automatic unsubscription flag. Remote filters
     * will be unregistered by default if master node leaves topology.
     */
    public static final boolean DFLT_AUTO_UNSUBSCRIBE = true;

    /** Initial filter. */
    private Query initFilter;

    /** Local listener. */
    private CacheEntryUpdatedListener<K, V> locLsnr;

    /** Remote filter. */
    private CacheEntryEventFilter<K, V> rmtFilter;

    /** Buffer size. */
    private int bufSize = DFLT_BUF_SIZE;

    /** Time interval. */
    private long timeInterval = DFLT_TIME_INTERVAL;

    /** Automatic unsubscription flag. */
    private boolean autoUnsubscribe = DFLT_AUTO_UNSUBSCRIBE;

    /**
     * Sets initial query.
     * <p>
     * This query will be executed before continuous listener is registered
     * which allows to iterate through entries which already existed at the
     * time continuous query is executed.
     *
     * @param initFilter Initial query.
     * @return {@code this} for chaining.
     */
    public ContinuousQuery<K, V> setInitialPredicate(Query initFilter) {
        this.initFilter = initFilter;

        return this;
    }

    /**
     * Gets initial query.
     *
     * @return Initial query.
     */
    public Query getInitialPredicate() {
        return initFilter;
    }

    /**
     * Sets local callback. This callback is called only in local node when new updates are received.
     * <p>
     * The callback predicate accepts ID of the node from where updates are received and collection
     * of received entries. Note that for removed entries value will be {@code null}.
     * <p>
     * If the predicate returns {@code false}, query execution will be cancelled.
     * <p>
     * <b>WARNING:</b> all operations that involve any kind of JVM-local or distributed locking (e.g.,
     * synchronization or transactional cache operations), should be executed asynchronously without
     * blocking the thread that called the callback. Otherwise, you can get deadlocks.
     *
     * @param locLsnr Local callback.
     * @return {@code this} for chaining.
     */
    public ContinuousQuery<K, V> setLocalListener(CacheEntryUpdatedListener<K, V> locLsnr) {
        this.locLsnr = locLsnr;

        return this;
    }

    /**
     * Gets local listener.
     *
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
     *
     * @param rmtFilter Key-value filter.
     * @return {@code this} for chaining.
     */
    public ContinuousQuery<K, V> setRemoteFilter(CacheEntryEventFilter<K, V> rmtFilter) {
        this.rmtFilter = rmtFilter;

        return this;
    }

    /**
     * Gets remote filter.
     *
     * @return Remote filter.
     */
    public CacheEntryEventFilter<K, V> getRemoteFilter() {
        return rmtFilter;
    }

    /**
     * Sets buffer size.
     * <p>
     * When a cache update happens, entry is first put into a buffer. Entries from buffer will be
     * sent to the master node only if the buffer is full or time provided via {@link #setTimeInterval(long)} method is
     * exceeded.
     * <p>
     * Default buffer size is {@code 1} which means that entries will be sent immediately (buffering is
     * disabled).
     *
     * @param bufSize Buffer size.
     * @return {@code this} for chaining.
     */
    public ContinuousQuery<K, V> setBufferSize(int bufSize) {
        if (bufSize <= 0)
            throw new IllegalArgumentException("Buffer size must be above zero.");

        this.bufSize = bufSize;

        return this;
    }

    /**
     * Gets buffer size.
     *
     * @return Buffer size.
     */
    public int getBufferSize() {
        return bufSize;
    }

    /**
     * Sets time interval.
     * <p>
     * When a cache update happens, entry is first put into a buffer. Entries from buffer will
     * be sent to the master node only if the buffer is full (its size can be provided via {@link #setBufferSize(int)}
     * method) or time provided via this method is exceeded.
     * <p>
     * Default time interval is {@code 0} which means that
     * time check is disabled and entries will be sent only when buffer is full.
     *
     * @param timeInterval Time interval.
     * @return {@code this} for chaining.
     */
    public ContinuousQuery<K, V> setTimeInterval(long timeInterval) {
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
    public ContinuousQuery<K, V> setAutoUnsubscribe(boolean autoUnsubscribe) {
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
}
