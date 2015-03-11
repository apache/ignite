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

package org.apache.ignite;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.mxbean.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Main entry point for all <b>Data Grid APIs.</b> You can get a named cache by calling {@link Ignite#jcache(String)}
 * method.
 * <h1 class="header">Functionality</h1>
 * This API extends {@link CacheProjection} API which contains vast majority of cache functionality
 * and documentation. In addition to {@link CacheProjection} functionality this API provides:
 * <ul>
 * <li>
 *  Various {@code 'loadCache(..)'} methods to load cache either synchronously or asynchronously.
 *  These methods don't specify any keys to load, and leave it to the underlying storage to load cache
 *  data based on the optionally passed in arguments.
 * </li>
 * <li>
 *  Methods like {@code 'tx{Un}Synchronize(..)'} witch allow to get notifications for transaction state changes.
 *  This feature is very useful when integrating cache transactions with some other in-house transactions.
 * </li>
 * <li>Method {@link #metrics()} to provide metrics for the whole cache.</li>
 * <li>Method {@link #getConfiguration(Class)}} to provide cache configuration bean.</li>
 * </ul>
 *
 * @param <K> Cache key type.
 * @param <V> Cache value type.
 */
public interface IgniteCache<K, V> extends javax.cache.Cache<K, V>, IgniteAsyncSupport {
    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withAsync();

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz);

    /**
     * Gets a random entry out of cache. In the worst cache scenario this method
     * has complexity of <pre>O(S * N/64)</pre> where {@code N} is the size of internal hash
     * table and {@code S} is the number of hash table buckets to sample, which is {@code 5}
     * by default. However, if the table is pretty dense, with density factor of {@code N/64},
     * which is true for near fully populated caches, this method will generally perform significantly
     * faster with complexity of O(S) where {@code S = 5}.
     *
     * @return Random entry, or {@code null} if cache is empty.
     */
    public Entry<K, V> randomEntry();

    public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc);

    /**
     * @return Cache with read-through write-through behavior disabled.
     */
    public IgniteCache<K, V> withSkipStore();

    /**
     * Executes {@link #localLoadCache(IgniteBiPredicate, Object...)} on all cache nodes.
     *
     * @param p Optional predicate (may be {@code null}). If provided, will be used to
     *      filter values to be put into cache.
     * @param args Optional user arguments to be passed into
     *      {@link CacheStore#loadCache(IgniteBiInClosure, Object...)} method.
     * @throws CacheException If loading failed.
     */
    @IgniteAsyncSupported
    public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException;

    /**
     * Delegates to {@link CacheStore#loadCache(IgniteBiInClosure,Object...)} method
     * to load state from the underlying persistent storage. The loaded values
     * will then be given to the optionally passed in predicate, and, if the predicate returns
     * {@code true}, will be stored in cache. If predicate is {@code null}, then
     * all loaded values will be stored in cache.
     * <p>
     * Note that this method does not receive keys as a parameter, so it is up to
     * {@link CacheStore} implementation to provide all the data to be loaded.
     * <p>
     * This method is not transactional and may end up loading a stale value into
     * cache if another thread has updated the value immediately after it has been
     * loaded. It is mostly useful when pre-loading the cache from underlying
     * data store before start, or for read-only caches.
     *
     * @param p Optional predicate (may be {@code null}). If provided, will be used to
     *      filter values to be put into cache.
     * @param args Optional user arguments to be passed into
     *      {@link CacheStore#loadCache(IgniteBiInClosure, Object...)} method.
     * @throws CacheException If loading failed.
     */
    @IgniteAsyncSupported
    public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException;

    /**
     * Stores given key-value pair in cache only if cache had no previous mapping for it. If cache
     * previously contained value for the given key, then this value is returned.
     * In case of {@link CacheMode#PARTITIONED} or {@link CacheMode#REPLICATED} caches,
     * the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, {@link CacheStore#load(Object)} method will be used.
     * <p>
     * If the returned value is not needed, method {@link #putIfAbsent(Object, Object)} should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via {@link CacheStore#write(Cache.Entry)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link CacheFlag#LOCAL}, {@link CacheFlag#READ}.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Previously contained value regardless of whether put happened or not ({@code null} if there was no
     *      previous value).
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws CacheException If put operation failed.
     * @throws CacheFlagException If projection flags validation failed.
     */
    @IgniteAsyncSupported
    public V getAndPutIfAbsent(K key, V val) throws CacheException;

    /**
     * Creates a {@link Lock} instance associated with passed key.
     * This method does not acquire lock immediately, you have to call appropriate method on returned instance.
     * Returned lock does not support {@link Lock#newCondition()} method,
     * other methods defined in {@link Lock} are supported.
     *
     * @param key Key for lock.
     * @return New lock instance associated with passed key.
     * @see Lock#lock()
     * @see Lock#tryLock(long, TimeUnit)
     */
    public Lock lock(K key);

    /**
     * Creates a {@link Lock} instance associated with passed keys.
     * This method does not acquire lock immediately, you have to call appropriate method on returned instance.
     * Returned lock does not support {@link Lock#newCondition()} method,
     * other methods defined in {@link Lock} are supported.
     *
     * @param keys Keys for lock.
     * @return New lock instance associated with passed key.
     * @see Lock#lock()
     * @see Lock#tryLock(long, TimeUnit)
     */
    public Lock lockAll(Collection<? extends K> keys);

    /**
     * Checks if specified key is locked.
     * <p>
     * This is a local in-VM operation and does not involve any network trips
     * or access to persistent storage in any way.
     *
     * @param key Key to check.
     * @param byCurrThread If {@code true} method will check that current thread owns a lock on this key, other vise
     *     will check that any thread on any node owns a lock on this key.
     * @return {@code True} if lock is owned by some node.
     */
    public boolean isLocalLocked(K key, boolean byCurrThread);

    /**
     * Queries cache. Accepts any subclass of {@link Query}.
     *
     * @param qry Query.
     * @return Cursor.
     * @see ScanQuery
     * @see SqlQuery
     * @see TextQuery
     * @see SpiQuery
     */
    public QueryCursor<Entry<K, V>> query(Query qry);

    /**
     * Queries separate entry fields.
     *
     * @param qry SQL Query.
     * @return Cursor.
     */
    public QueryCursor<List<?>> queryFields(SqlFieldsQuery qry);

    /**
     * Queries cache locally. Accepts any subclass of {@link Query}.
     *
     * @param qry Query.
     * @return Cursor.
     * @see ScanQuery
     * @see SqlQuery
     * @see TextQuery
     * @see SpiQuery
     */
    public QueryCursor<Entry<K, V>> localQuery(Query qry);

    /**
     * Queries separate entry fields locally.
     *
     * @param qry SQL Query.
     * @return Cursor.
     */
    public QueryCursor<List<?>> localQueryFields(SqlFieldsQuery qry);

    public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException;

    /**
     * Attempts to evict all entries associated with keys. Note,
     * that entry will be evicted only if it's not used (not
     * participating in any locks or transactions).
     * <p>
     * If {@link CacheConfiguration#isSwapEnabled()} is set to {@code true} and
     * {@link CacheFlag#SKIP_SWAP} is not enabled, the evicted entry will
     * be swapped to offheap, and then to disk.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link CacheFlag#READ}.
     *
     * @param keys Keys to evict.
     */
    public void localEvict(Collection<? extends K> keys);

    /**
     * Peeks at in-memory cached value using default {@link GridCachePeekMode#SMART}
     * peek mode.
     * <p>
     * This method will not load value from any persistent store or from a remote node.
     * <h2 class="header">Transactions</h2>
     * This method does not participate in any transactions, however, it will
     * peek at transactional value according to the {@link GridCachePeekMode#SMART} mode
     * semantics.
     *
     * @param key Entry key.
     * @return Peeked value, or {@code null} if not found.
     * @throws NullPointerException If key is {@code null}.
     */
    public V localPeek(K key, CachePeekMode... peekModes);

    /**
     * This method unswaps cache entries by given keys, if any, from swap storage
     * into memory.
     * <h2 class="header">Transactions</h2>
     * This method is not transactional.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link CacheFlag#SKIP_SWAP}, {@link CacheFlag#READ}.
     *
     * @param keys Keys to promote entries for.
     * @throws CacheException If promote failed.
     * @throws CacheFlagException If flags validation failed.
     */
    public void localPromote(Set<? extends K> keys) throws CacheException;

    /**
     * Gets the number of all entries cached across all nodes.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return Cache size across all nodes.
     */
    @IgniteAsyncSupported
    public int size(CachePeekMode... peekModes) throws CacheException;

    /**
     * Gets the number of all entries cached on this nodes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return Cache size on this node.
     */
    public int localSize(CachePeekMode... peekModes);

    /**
     * @param map Map containing keys and entry processors to be applied to values.
     * @param args Additional arguments to pass to the {@link EntryProcessor}.
     * @return The map of {@link EntryProcessorResult}s of the processing per key,
     * if any, defined by the {@link EntryProcessor} implementation.  No mappings
     * will be returned for {@link EntryProcessor}s that return a
     * <code>null</code> value for a key.
     */
    @IgniteAsyncSupported
    <T> Map<K, EntryProcessorResult<T>> invokeAll(Map<? extends K, ? extends EntryProcessor<K, V, T>> map, Object... args);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public V get(K key);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public Map<K, V> getAll(Set<? extends K> keys);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean containsKey(K key);

    @IgniteAsyncSupported
    public boolean containsKeys(Set<? extends K> keys);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public void put(K key, V val);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public V getAndPut(K key, V val);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public void putAll(Map<? extends K, ? extends V> map);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean putIfAbsent(K key, V val);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean remove(K key);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean remove(K key, V oldVal);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public V getAndRemove(K key);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean replace(K key, V oldVal, V newVal);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean replace(K key, V val);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public V getAndReplace(K key, V val);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public void removeAll(Set<? extends K> keys);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public void removeAll();

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public void clear();

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments);

    /**
     * Invokes an {@link IgniteEntryProcessor} against the {@link Entry} specified by
     * the provided key. If an {@link Entry} does not exist for the specified key,
     * an attempt is made to load it (if a loader is configured) or a surrogate
     * {@link Entry}, consisting of the key with a null value is used instead.
     * This method different
     * <p>
     *
     * @param key            the key to the entry
     * @param entryProcessor the {@link IgniteEntryProcessor} to invoke
     * @param arguments      additional arguments to pass to the
     *                       {@link IgniteEntryProcessor}
     * @return the result of the processing, if any, defined by the
     *         {@link IgniteEntryProcessor} implementation
     * @throws NullPointerException    if key or {@link IgniteEntryProcessor} is null
     * @throws IllegalStateException   if the cache is {@link #isClosed()}
     * @throws ClassCastException    if the implementation is configured to perform
     *                               runtime-type-checking, and the key or value
     *                               types are incompatible with those that have been
     *                               configured for the {@link Cache}
     * @throws EntryProcessorException if an exception is thrown by the {@link
     *                                 IgniteEntryProcessor}, a Caching Implementation
     *                                 must wrap any {@link Exception} thrown
     *                                 wrapped in an {@link EntryProcessorException}.
     * @see IgniteEntryProcessor
     */
    @IgniteAsyncSupported
    public <T> T invoke(K key, IgniteEntryProcessor<K, V, T> entryProcessor, Object... arguments);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor, Object... args);

    /**
     * Invokes an {@link IgniteEntryProcessor} against the set of {@link Entry}s
     * specified by the set of keys.
     * <p>
     * If an {@link Entry} does not exist for the specified key, an attempt is made
     * to load it (if a loader is configured) or a surrogate {@link Entry},
     * consisting of the key and a value of null is provided.
     * <p>
     * The order that the entries for the keys are processed is undefined.
     * Implementations may choose to process the entries in any order, including
     * concurrently.  Furthermore there is no guarantee implementations will
     * use the same {@link IgniteEntryProcessor} instance to process each entry, as
     * the case may be in a non-local cache topology.
     * <p>
     * The result of executing the {@link IgniteEntryProcessor} is returned as a
     * {@link Map} of {@link EntryProcessorResult}s, one result per key.  Should the
     * {@link IgniteEntryProcessor} or Caching implementation throw an exception, the
     * exception is wrapped and re-thrown when a call to
     * {@link javax.cache.processor.EntryProcessorResult#get()} is made.
     *
     * @param keys           the set of keys for entries to process
     * @param entryProcessor the {@link IgniteEntryProcessor} to invoke
     * @param args      additional arguments to pass to the
     *                       {@link IgniteEntryProcessor}
     * @return the map of {@link EntryProcessorResult}s of the processing per key,
     * if any, defined by the {@link IgniteEntryProcessor} implementation.  No mappings
     * will be returned for {@link IgniteEntryProcessor}s that return a
     * <code>null</code> value for a key.
     * @throws NullPointerException    if keys or {@link IgniteEntryProcessor} are null
     * @throws IllegalStateException   if the cache is {@link #isClosed()}
     * @throws ClassCastException    if the implementation is configured to perform
     *                               runtime-type-checking, and the key or value
     *                               types are incompatible with those that have been
     *                               configured for the {@link Cache}
     * @see IgniteEntryProcessor
     */
    @IgniteAsyncSupported
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        IgniteEntryProcessor<K, V, T> entryProcessor, Object... args);

    /**
     * This cache node to re-balance its partitions. This method is usually used when
     * {@link CacheConfiguration#getRebalanceDelay()} configuration parameter has non-zero value.
     * When many nodes are started or stopped almost concurrently, it is more efficient to delay
     * rebalancing until the node topology is stable to make sure that no redundant re-partitioning
     * happens.
     * <p>
     * In case of{@link CacheMode#PARTITIONED} caches, for better efficiency user should
     * usually make sure that new nodes get placed on the same place of consistent hash ring as
     * the left nodes, and that nodes are restarted before
     * {@link CacheConfiguration#getRebalanceDelay() rebalanceDelay} expires. To place nodes
     * on the same place in consistent hash ring, use
     * {@link CacheRendezvousAffinityFunction#setHashIdResolver(CacheAffinityNodeHashResolver)} to make sure that
     * a node maps to the same hash ID if re-started.
     * <p>
     * See {@link CacheConfiguration#getRebalanceDelay()} for more information on how to configure
     * rebalance re-partition delay.
     * <p>
     * @return Future that will be completed when rebalancing is finished.
     */
    public IgniteFuture<?> rebalance();

    /**
     * Gets snapshot metrics (statistics) for this cache.
     *
     * @return Cache metrics.
     */
    public CacheMetrics metrics();

    /**
     * Gets MxBean for this cache.
     *
     * @return MxBean.
     */
    public CacheMetricsMXBean mxBean();
}
