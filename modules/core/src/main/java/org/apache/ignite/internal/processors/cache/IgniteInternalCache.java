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

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * This interface provides a rich API for working with distributed caches. It includes the following
 * main functionality:
 * <ul>
 * <li>
 *  Various {@code 'get(..)'} methods to synchronously or asynchronously get values from cache.
 *  All {@code 'get(..)'} methods are transactional and will participate in an ongoing transaction
 *  if there is one.
 * </li>
 * <li>
 *  Various {@code 'put(..)'}, {@code 'putIfAbsent(..)'}, and {@code 'replace(..)'} methods to
 *  synchronously or asynchronously put single or multiple entries into cache.
 *  All these methods are transactional and will participate in an ongoing transaction
 *  if there is one.
 * </li>
 * <li>
 *  Various {@code 'remove(..)'} methods to synchronously or asynchronously remove single or multiple keys
 *  from cache. All {@code 'remove(..)'} methods are transactional and will participate in an ongoing transaction
 *  if there is one.
 * </li>
 * <li>
 *  Various {@code 'contains(..)'} method to check if cache contains certain keys or values locally.
 * </li>
 * <li>
 *  Various {@code 'forEach(..)'}, {@code 'forAny(..)'}, and {@code 'reduce(..)'} methods to visit
 *  every local cache entry within this projection.
 * </li>
 * <li>
 *  Various {@code flagsOn(..)'}, {@code 'flagsOff(..)'}, and {@code 'projection(..)'} methods to
 *  set specific flags and filters on a cache projection.
 * </li>
 * <li>
 *  Methods like {@code 'keySet(..)'}, {@code 'values(..)'}, and {@code 'entrySet(..)'} to provide
 *  views on cache keys, values, and entries.
 * </li>
 * <li>
 *  Various {@code 'peek(..)'} methods to peek at values in global or transactional memory, swap
 *  storage, or persistent storage.
 * </li>
 * <li>
 *  Various {@code 'reload(..)'} methods to reload latest values from persistent storage.
 * </li>
 * <li>
 *  Various {@code 'promote(..)'} methods to load specified keys from swap storage into
 *  global cache memory.
 * </li>
 * <li>
 *  Various {@code 'lock(..)'}, {@code 'unlock(..)'}, and {@code 'isLocked(..)'} methods to acquire, release,
 *  and check on distributed locks on a single or multiple keys in cache. All locking methods
 *  are not transactional and will not enlist keys into ongoing transaction, if any.
 * </li>
 * <li>
 *  Various {@code 'clear(..)'} methods to clear elements from cache, and optionally from
 *  swap storage. All {@code 'clear(..)'} methods are not transactional and will not enlist cleared
 *  keys into ongoing transaction, if any.
 * </li>
 * <li>
 *  Various {@code 'evict(..)'} methods to evict elements from cache, and optionally store
 *  them in underlying swap storage for later access. All {@code 'evict(..)'} methods are not
 *  transactional and will not enlist evicted keys into ongoing transaction, if any.
 * </li>
 * <li>
 *  Various {@code 'txStart(..)'} methods to perform various cache
 *  operations within a transaction (see {@link Transaction} for more information).
 * </li>
 * <li>
 *  Various {@code 'gridProjection(..)'} methods which provide {@link org.apache.ignite.cluster.ClusterGroup} only
 *  for nodes on which given keys reside. All {@code 'gridProjection(..)'} methods are not
 *  transactional and will not enlist keys into ongoing transaction.
 * </li>
 * <li>
 * </ul>
 * <h1 class="header">Extended Put And Remove Methods</h1>
 * All methods that end with {@code 'x'} provide the same functionality as their sibling
 * methods that don't end with {@code 'x'}, however instead of returning a previous value they
 * return a {@code boolean} flag indicating whether operation succeeded or not. Returning
 * a previous value may involve a network trip or a persistent store lookup and should be
 * avoided whenever not needed.
 * <h1 class="header">Predicate Filters</h1>
 * All filters passed into methods on this API are checked <b>atomically</b>. In other words the
 * value returned by the methods is guaranteed to be consistent with the filters passed in. Note
 * that filters are optional, and if not passed in, then methods will still work as is without
 * filter validation.
 * <h1 class="header">Transactions</h1>
 * Cache API supports distributed transactions. All {@code 'get(..)'}, {@code 'put(..)'}, {@code 'replace(..)'},
 * and {@code 'remove(..)'} operations are transactional and will participate in an ongoing transaction,
 * if any. Other methods like {@code 'peek(..)'} or various {@code 'contains(..)'} methods may
 * be transaction-aware, i.e. check in-transaction entries first, but will not affect the current
 * state of transaction. See {@link Transaction} documentation for more information
 * about transactions.
 * <h1 class="header">Group Locking</h1>
 * <i>Group Locking</i> is a feature where instead of acquiring individual locks, Ignite will lock
 * multiple keys with one lock to save on locking overhead. There are 2 types of <i>Group Locking</i>:
 * <i>affinity-based</i>, and <i>partitioned-based</i>.
 * <p>
 * With {@code affinity-based-group-locking} the keys are grouped by <i>affinity-key</i>. This means that
 * only keys with identical affinity-key (see {@link AffinityKeyMapped}) can participate in the
 * transaction, and only one lock on the <i>affinity-key</i> will be acquired for the whole transaction.
 * {@code Affinity-group-locked} transactions are started via
 * <code>txStartAffinity(Object, TransactionConcurrency, TransactionIsolation, long, int)</code> method.
 * <p>
 * With {@code partition-based-group-locking} the keys are grouped by partition ID. This means that
 * only keys belonging to identical partition (see {@link Affinity#partition(Object)}) can participate in the
 * transaction, and only one lock on the whole partition will be acquired for the whole transaction.
 * {@code Partition-group-locked} transactions are started via
 * <code>txStartPartition(int, TransactionConcurrency, TransactionIsolation, long, int)</code> method.
 * <p>
 * <i>Group locking</i> should always be used for transactions whenever possible. If your requirements fit either
 * <i>affinity-based</i> or <i>partition-based</i> scenarios outlined above then <i>group-locking</i>
 * can significantly improve performance of your application, often by an order of magnitude.
 * <h1 class="header">Null Keys or Values</h1>
 * Neither {@code null} keys or values are allowed to be stored in cache. If a {@code null} value
 * happens to be in cache (e.g. after invalidation or remove), then cache will treat this case
 * as there is no value at all.
 * <h1 class="header">Peer Class Loading</h1>
 * If peer-class-loading is enabled, all classes passed into cache API will be automatically deployed
 * to any participating grid nodes. However, in case of redeployment, caches will be cleared and
 * all entries will be removed. This behavior is useful during development, but should not be
 * used in production.
 * <h1 class="header">Binary Objects</h1>
 * If an object is defined as binary Ignite cache will automatically store it in binary (i.e. binary)
 * format. User can choose to work either with the binary format or with the deserialized form (assuming
 * that class definitions are present in the classpath). By default, cache works with deserialized form
 * (example shows the case when {@link Integer} is used as a key for a binary object):
 * <pre>
 * IgniteInternalCache<Integer, Value> prj = Ignition.grid().cache(null);
 *
 * // Value will be serialized and stored in cache in binary format.
 * prj.put(1, new Value());
 *
 * // Value will be deserialized since it's stored in binary format.
 * Value val = prj.get(1);
 * </pre>
 * You won't be able to work with deserialized form if class definition for the {@code Value} is not on
 * classpath. Even if you have the class definition, you should always avoid full deserialization if it's not
 * needed for performance reasons. To work with binary format directly you should create special projection
 * using {@link #keepBinary()} method:
 * <pre>
 * IgniteInternalCache<Integer, GridBinaryObject> prj = Ignition.grid().cache(null).keepBinary();
 *
 * // Value is not deserialized and returned in binary format.
 * GridBinaryObject po = prj.get(1);
 * </pre>
 * See {@link #keepBinary()} method JavaDoc for more details.
 */
public interface IgniteInternalCache<K, V> extends Iterable<Cache.Entry<K, V>> {
    /**
     * Gets name of this cache ({@code null} for default cache).
     *
     * @return Cache name.
     */
    public String name();

    /**
     * Gets base cache for this projection.
     *
     * @param <K1> Cache key type.
     * @param <V1> Cache value type.
     * @return Base cache for this projection.
     */
    @SuppressWarnings({"ClassReferencesSubclass"})
    public <K1, V1> IgniteInternalCache<K1, V1> cache();

    /**
     * @return Skip store.
     */
    public boolean skipStore();

    /**
     * @param skipStore Skip store flag.
     * @return New internal cache instance based on this one, but with skip store flag enabled.
     */
    public IgniteInternalCache<K, V> setSkipStore(boolean skipStore);

    /**
     * Creates projection that will operate with binary objects.
     * <p>
     * Projection returned by this method will force cache not to deserialize binary objects,
     * so keys and values will be returned from cache API methods without changes. Therefore,
     * signature of the projection can contain only following types:
     * <ul>
     *     <li><code>org.gridgain.grid.binary.BinaryObject</code> for binary classes</li>
     *     <li>All primitives (byte, int, ...) and there boxed versions (Byte, Integer, ...)</li>
     *     <li>Arrays of primitives (byte[], int[], ...)</li>
     *     <li>{@link String} and array of {@link String}s</li>
     *     <li>{@link UUID} and array of {@link UUID}s</li>
     *     <li>{@link Date} and array of {@link Date}s</li>
     *     <li>{@link Timestamp} and array of {@link Timestamp}s</li>
     *     <li>Enums and array of enums</li>
     *     <li>
     *         Maps, collections and array of objects (but objects inside
     *         them will still be converted if they are binary)
     *     </li>
     * </ul>
     * <p>
     * For example, if you use {@link Integer} as a key and {@code Value} class as a value
     * (which will be stored in binary format), you should acquire following projection
     * to avoid deserialization:
     * <pre>
     * IgniteInternalCache<Integer, GridBinaryObject> prj = cache.keepBinary();
     *
     * // Value is not deserialized and returned in binary format.
     * GridBinaryObject po = prj.get(1);
     * </pre>
     * <p>
     * Note that this method makes sense only if cache is working in binary mode
     * (<code>org.apache.ignite.configuration.CacheConfiguration#isBinaryEnabled()</code> returns {@code true}. If not,
     * this method is no-op and will return current projection.
     *
     * @return New internal cache instance for binary objects.
     */
    public <K1, V1> IgniteInternalCache<K1, V1> keepBinary();

    /**
     * Returns {@code true} if this map contains no key-value mappings.
     *
     * @return {@code true} if this map contains no key-value mappings.
     */
    public boolean isEmpty();

    /**
     * @param key Key.
     * @return {@code True} if cache contains mapping for a given key.
     */
    public boolean containsKey(K key);

    /**
     * @param key Key.
     * @return Future.
     */
    public IgniteInternalFuture<Boolean> containsKeyAsync(K key);

    /**
     * @param keys Keys,
     * @return {@code True} if cache contains all keys.
     */
    public boolean containsKeys(Collection<? extends K> keys);

    /**
     * @param keys Keys to check.
     * @return Future.
     */
    public IgniteInternalFuture<Boolean> containsKeysAsync(Collection<? extends K> keys);

    /**
     * @param key Key.
     * @param peekModes Peek modes.
     * @param plc Expiry policy if TTL should be updated.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public V localPeek(K key, CachePeekMode[] peekModes, @Nullable IgniteCacheExpiryPolicy plc)
        throws IgniteCheckedException;

    /**
     * @param peekModes Peek modes.
     * @return Entries iterable.
     * @throws IgniteCheckedException If failed.
     */
    public Iterable<Cache.Entry<K, V>> localEntries(CachePeekMode[] peekModes) throws IgniteCheckedException;

    /**
     * Retrieves value mapped to the specified key from cache. Value will only be returned if
     * its entry passed the optional filter provided. Filter check is atomic, and therefore the
     * returned value is guaranteed to be consistent with the filter. The return value of {@code null}
     * means entry did not pass the provided filter or cache has no mapping for the
     * key.
     * <p>
     * If the value is not present in cache, then it will be looked up from swap storage. If
     * it's not present in swap, or if swap is disable, and if read-through is allowed, value
     * will be loaded from {@link CacheStore} persistent storage via
     * <code>CacheStore#load(Transaction, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to retrieve the value for.
     * @return Value for the given key.
     * @throws IgniteCheckedException If get operation failed.
     * @throws NullPointerException if the key is {@code null}.
     */
    @Nullable public V get(K key) throws IgniteCheckedException;

    /**
     * Retrieves value mapped to the specified key from cache. Value will only be returned if
     * its entry passed the optional filter provided. Filter check is atomic, and therefore the
     * returned value is guaranteed to be consistent with the filter. The return value of {@code null}
     * means entry did not pass the provided filter or cache has no mapping for the
     * key.
     * <p>
     * If the value is not present in cache, then it will be looked up from swap storage. If
     * it's not present in swap, or if swap is disable, and if read-through is allowed, value
     * will be loaded from {@link CacheStore} persistent storage via
     * <code>CacheStore#load(Transaction, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to retrieve the value for.
     * @return Value for the given key.
     * @throws IgniteCheckedException If get operation failed.
     * @throws NullPointerException if the key is {@code null}.
     */
    @Nullable public CacheEntry<K, V> getEntry(K key) throws IgniteCheckedException;

    /**
     * Asynchronously retrieves value mapped to the specified key from cache. Value will only be returned if
     * its entry passed the optional filter provided. Filter check is atomic, and therefore the
     * returned value is guaranteed to be consistent with the filter. The return value of {@code null}
     * means entry did not pass the provided filter or cache has no mapping for the
     * key.
     * <p>
     * If the value is not present in cache, then it will be looked up from swap storage. If
     * it's not present in swap, or if swap is disabled, and if read-through is allowed, value
     * will be loaded from {@link CacheStore} persistent storage via
     * <code>CacheStore#load(Transaction, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key for the value to get.
     * @return Future for the get operation.
     * @throws NullPointerException if the key is {@code null}.
     */
    public IgniteInternalFuture<V> getAsync(K key);

    /**
     * Asynchronously retrieves value mapped to the specified key from cache. Value will only be returned if
     * its entry passed the optional filter provided. Filter check is atomic, and therefore the
     * returned value is guaranteed to be consistent with the filter. The return value of {@code null}
     * means entry did not pass the provided filter or cache has no mapping for the
     * key.
     * <p>
     * If the value is not present in cache, then it will be looked up from swap storage. If
     * it's not present in swap, or if swap is disabled, and if read-through is allowed, value
     * will be loaded from {@link CacheStore} persistent storage via
     * <code>CacheStore#load(Transaction, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key for the value to get.
     * @return Future for the get operation.
     * @throws NullPointerException if the key is {@code null}.
     */
    public IgniteInternalFuture<CacheEntry<K, V>> getEntryAsync(K key);

    /**
     * Retrieves values mapped to the specified keys from cache. Value will only be returned if
     * its entry passed the optional filter provided. Filter check is atomic, and therefore the
     * returned value is guaranteed to be consistent with the filter. If requested key-value pair
     * is not present in the returned map, then it means that its entry did not pass the provided
     * filter or cache has no mapping for the key.
     * <p>
     * If some value is not present in cache, then it will be looked up from swap storage. If
     * it's not present in swap, or if swap is disabled, and if read-through is allowed, value
     * will be loaded from {@link CacheStore} persistent storage via
     * <code>CacheStore#loadAll(Transaction, Collection, org.apache.ignite.lang.IgniteBiInClosure)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param keys Keys to get.
     * @return Map of key-value pairs.
     * @throws IgniteCheckedException If get operation failed.
     */
    public Map<K, V> getAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException;

    /**
     * Retrieves values mapped to the specified keys from cache. Value will only be returned if
     * its entry passed the optional filter provided. Filter check is atomic, and therefore the
     * returned value is guaranteed to be consistent with the filter. If requested key-value pair
     * is not present in the returned map, then it means that its entry did not pass the provided
     * filter or cache has no mapping for the key.
     * <p>
     * If some value is not present in cache, then it will be looked up from swap storage. If
     * it's not present in swap, or if swap is disabled, and if read-through is allowed, value
     * will be loaded from {@link CacheStore} persistent storage via
     * <code>CacheStore#loadAll(Transaction, Collection, org.apache.ignite.lang.IgniteBiInClosure)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param keys Keys to get.
     * @return Map of key-value pairs.
     * @throws IgniteCheckedException If get operation failed.
     */
    public Collection<CacheEntry<K, V>> getEntries(@Nullable Collection<? extends K> keys) throws IgniteCheckedException;

    /**
     * Asynchronously retrieves values mapped to the specified keys from cache. Value will only be returned if
     * its entry passed the optional filter provided. Filter check is atomic, and therefore the
     * returned value is guaranteed to be consistent with the filter. If requested key-value pair
     * is not present in the returned map, then it means that its entry did not pass the provided
     * filter or cache has no mapping for the key.
     * <p>
     * If some value is not present in cache, then it will be looked up from swap storage. If
     * it's not present in swap, or if swap is disabled, and if read-through is allowed, value
     * will be loaded from {@link CacheStore} persistent storage via
     * <code>CacheStore#loadAll(Transaction, Collection, org.apache.ignite.lang.IgniteBiInClosure)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param keys Key for the value to get.
     * @return Future for the get operation.
     */
    public IgniteInternalFuture<Map<K, V>> getAllAsync(@Nullable Collection<? extends K> keys);

    /**
     * Asynchronously retrieves values mapped to the specified keys from cache. Value will only be returned if
     * its entry passed the optional filter provided. Filter check is atomic, and therefore the
     * returned value is guaranteed to be consistent with the filter. If requested key-value pair
     * is not present in the returned map, then it means that its entry did not pass the provided
     * filter or cache has no mapping for the key.
     * <p>
     * If some value is not present in cache, then it will be looked up from swap storage. If
     * it's not present in swap, or if swap is disabled, and if read-through is allowed, value
     * will be loaded from {@link CacheStore} persistent storage via
     * <code>CacheStore#loadAll(Transaction, Collection, org.apache.ignite.lang.IgniteBiInClosure)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param keys Key for the value to get.
     * @return Future for the get operation.
     */
    public IgniteInternalFuture<Collection<CacheEntry<K, V>>> getEntriesAsync(@Nullable Collection<? extends K> keys);

    /**
     * Stores given key-value pair in cache. If filters are provided, then entries will
     * be stored in cache only if they pass the filter. Note that filter check is atomic,
     * so value stored in cache is guaranteed to be consistent with the filters. If cache
     * previously contained value for the given key, then this value is returned.
     * In case of {@link CacheMode#PARTITIONED} or {@link CacheMode#REPLICATED} caches,
     * the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage,  <code>CacheStore#load(Transaction, Object)</code> method will be used.
     * <p>
     * If the returned value is not needed, method <code>#putx(Object, Object, org.apache.ignite.lang.IgnitePredicate[])</code> should
     * always be used instead of this one to avoid the overhead associated with returning of the previous value.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Previous value associated with specified key, or {@code null}
     *  if entry did not pass the filter, or if there was no mapping for the key in swap
     *  or in persistent storage.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws IgniteCheckedException If put operation failed.
     */
    @Nullable public V getAndPut(K key, V val)
        throws IgniteCheckedException;

    /**
     * Asynchronously stores given key-value pair in cache. If filters are provided, then entries will
     * be stored in cache only if they pass the filter. Note that filter check is atomic,
     * so value stored in cache is guaranteed to be consistent with the filters. If cache
     * previously contained value for the given key, then this value is returned. Otherwise,
     * in case of {@link CacheMode#REPLICATED} caches, the value will be loaded from swap
     * and, if it's not there, and read-through is allowed, from the underlying
     * {@link CacheStore} storage. In case of {@link CacheMode#PARTITIONED} caches,
     * the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap and read-through is allowed,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage,  <code>CacheStore#load(Transaction, Object)</code> method will be used.
     * <p>
     * If the returned value is not needed, method <code>#putx(Object, Object, org.apache.ignite.lang.IgnitePredicate[])</code> should
     * always be used instead of this one to avoid the overhead associated with returning of the previous value.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Future for the put operation.
     * @throws NullPointerException If either key or value are {@code null}.
     */
    public IgniteInternalFuture<V> getAndPutAsync(K key, V val);

    /**
     * Stores given key-value pair in cache. If filters are provided, then entries will
     * be stored in cache only if they pass the filter. Note that filter check is atomic,
     * so value stored in cache is guaranteed to be consistent with the filters.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * Unlike <code>#put(Object, Object, org.apache.ignite.lang.IgnitePredicate[])</code> method, it does not return previous
     * value and, therefore, does not have any overhead associated with returning a value. It
     * should be used whenever return value is not required.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return {@code True} if optional filter passed and value was stored in cache,
     *      {@code false} otherwise. Note that this method will return {@code true} if filter is not
     *      specified.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws IgniteCheckedException If put operation failed.
     */
    public boolean put(K key, V val)
        throws IgniteCheckedException;

    /**
     * Stores given key-value pair in cache. If filters are provided, then entries will
     * be stored in cache only if they pass the filter. Note that filter check is atomic,
     * so value stored in cache is guaranteed to be consistent with the filters.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * Unlike <code>#put(Object, Object, org.apache.ignite.lang.IgnitePredicate[])</code> method, it does not return previous
     * value and, therefore, does not have any overhead associated with returning of a value. It
     * should always be used whenever return value is not required.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Future for the put operation. Future will return {@code true} if optional filter
     *      passed and value was stored in cache, {@code false} otherwise. Note that future will
     *      return {@code true} if filter is not specified.
     * @throws NullPointerException If either key or value are {@code null}.
     */
    public IgniteInternalFuture<Boolean> putAsync(K key, V val);

    /**
     * Stores given key-value pair in cache only if cache had no previous mapping for it. If cache
     * previously contained value for the given key, then this value is returned.
     * In case of {@link CacheMode#PARTITIONED} or {@link CacheMode#REPLICATED} caches,
     * the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, <code>CacheStore#load(Transaction, Object)</code> method will be used.
     * <p>
     * If the returned value is not needed, method {@link #putIfAbsent(Object, Object)} should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Previously contained value regardless of whether put happened or not.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws IgniteCheckedException If put operation failed.
     */
    @Nullable public V getAndPutIfAbsent(K key, V val) throws IgniteCheckedException;

    /**
     * Asynchronously stores given key-value pair in cache only if cache had no previous mapping for it. If cache
     * previously contained value for the given key, then this value is returned. In case of
     * {@link CacheMode#PARTITIONED} or {@link CacheMode#REPLICATED} caches,
     * the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, <code>CacheStore#load(Transaction, Object)</code> method will be used.
     * <p>
     * If the returned value is not needed, method {@link #putIfAbsentAsync(Object, Object)} should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Future of put operation which will provide previously contained value
     *   regardless of whether put happened or not.
     * @throws NullPointerException If either key or value are {@code null}.
     */
    public IgniteInternalFuture<V> getAndPutIfAbsentAsync(K key, V val);

    /**
     * Stores given key-value pair in cache only if cache had no previous mapping for it.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * Unlike {@link #getAndPutIfAbsent(Object, Object)} method, it does not return previous
     * value and, therefore, does not have any overhead associated with returning of a value. It
     * should always be used whenever return value is not required.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return {@code true} if value is stored in cache and {@code false} otherwise.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws IgniteCheckedException If put operation failed.
     */
    public boolean putIfAbsent(K key, V val) throws IgniteCheckedException;

    /**
     * Asynchronously stores given key-value pair in cache only if cache had no previous mapping for it.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * Unlike {@link #getAndPutIfAbsent(Object, Object)} method, it does not return previous
     * value and, therefore, does not have any overhead associated with returning of a value. It
     * should always be used whenever return value is not required.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Future for this put operation.
     * @throws NullPointerException If either key or value are {@code null}.
     */
    public IgniteInternalFuture<Boolean> putIfAbsentAsync(K key, V val);

    /**
     * Stores given key-value pair in cache only if there is a previous mapping for it.
     * In case of {@link CacheMode#PARTITIONED} or {@link CacheMode#REPLICATED} caches,
     * the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, <code>CacheStore#load(Transaction, Object)</code> method will be used.
     * <p>
     * If the returned value is not needed, method {@link #replace(Object, Object)} should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Previously contained value regardless of whether replace happened or not.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws IgniteCheckedException If replace operation failed.
     */
    @Nullable public V getAndReplace(K key, V val) throws IgniteCheckedException;

    /**
     * Asynchronously stores given key-value pair in cache only if there is a previous mapping for it. If cache
     * previously contained value for the given key, then this value is returned.In case of
     * {@link CacheMode#PARTITIONED} caches, the value will be loaded from the primary node,
     * which in its turn may load the value from the swap storage, and consecutively, if it's not in swap,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, <code>CacheStore#load(Transaction, Object)</code> method will be used.
     * <p>
     * If the returned value is not needed, method {@link #replace(Object, Object)} should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Future for replace operation.
     * @throws NullPointerException If either key or value are {@code null}.
     */
    public IgniteInternalFuture<V> getAndReplaceAsync(K key, V val);

    /**
     * Stores given key-value pair in cache only if only if there is a previous mapping for it.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * Unlike {@link #getAndReplace(Object, Object)} method, it does not return previous
     * value and, therefore, does not have any overhead associated with returning of a value. It
     * should always be used whenever return value is not required.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return {@code True} if replace happened, {@code false} otherwise.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws IgniteCheckedException If replace operation failed.
     */
    public boolean replace(K key, V val) throws IgniteCheckedException;

    /**
     * Asynchronously stores given key-value pair in cache only if only if there is a previous mapping for it.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * Unlike {@link #getAndReplaceAsync(Object, Object)} method, it does not return previous
     * value and, therefore, does not have any overhead associated with returning of a value. It
     * should always be used whenever return value is not required.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Future for the replace operation.
     * @throws NullPointerException If either key or value are {@code null}.
     */
    public IgniteInternalFuture<Boolean> replaceAsync(K key, V val);

    /**
     * Stores given key-value pair in cache only if only if the previous value is equal to the
     * {@code 'oldVal'} passed in.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param oldVal Old value to match.
     * @param newVal Value to be associated with the given key.
     * @return {@code True} if replace happened, {@code false} otherwise.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws IgniteCheckedException If replace operation failed.
     */
    public boolean replace(K key, V oldVal, V newVal) throws IgniteCheckedException;

    /**
     * Asynchronously stores given key-value pair in cache only if only if the previous value is equal to the
     * {@code 'oldVal'} passed in.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via <code>CacheStore#put(Transaction, Object, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param oldVal Old value to match.
     * @param newVal Value to be associated with the given key.
     * @return Future for the replace operation.
     * @throws NullPointerException If either key or value are {@code null}.
     */
    public IgniteInternalFuture<Boolean> replaceAsync(K key, V oldVal, V newVal);

    /**
     * Stores given key-value pairs in cache. If filters are provided, then entries will
     * be stored in cache only if they pass the filter. Note that filter check is atomic,
     * so value stored in cache is guaranteed to be consistent with the filters.
     * <p>
     * If write-through is enabled, the stored values will be persisted to {@link CacheStore}
     * via <code>CacheStore#putAll(Transaction, Map)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param m Key-value pairs to store in cache.
     * @throws IgniteCheckedException If put operation failed.
     */
    public void putAll(@Nullable Map<? extends K, ? extends V> m) throws IgniteCheckedException;

    /**
     * Asynchronously stores given key-value pairs in cache. If filters are provided, then entries will
     * be stored in cache only if they pass the filter. Note that filter check is atomic,
     * so value stored in cache is guaranteed to be consistent with the filters.
     * <p>
     * If write-through is enabled, the stored values will be persisted to {@link CacheStore}
     * via <code>CacheStore#putAll(Transaction, Map)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param m Key-value pairs to store in cache.
     * @return Future for putAll operation.
     */
    public IgniteInternalFuture<?> putAllAsync(@Nullable Map<? extends K, ? extends V> m);

    /**
     * Set of keys cached on this node. You can remove elements from this set, but you cannot add elements
     * to this set. All removal operation will be reflected on the cache itself.
     * <p>
     * Iterator over this set will not fail if set was concurrently updated
     * by another thread. This means that iterator may or may not return latest
     * keys depending on whether they were added before or after current
     * iterator position.
     * <p>
     * NOTE: this operation is not distributed and returns only the keys cached on this node.
     *
     * @return Key set for this cache projection.
     */
    public Set<K> keySet();

    /**
     * @return Set of keys including internal keys.
     */
    public Set<K> keySetx();

    /**
     * Set of keys for which this node is primary.
     * This set is dynamic and may change with grid topology changes.
     * Note that this set will contain mappings for all keys, even if their values are
     * {@code null} because they were invalidated. You can remove elements from
     * this set, but you cannot add elements to this set. All removal operation will be
     * reflected on the cache itself.
     * <p>
     * Iterator over this set will not fail if set was concurrently updated
     * by another thread. This means that iterator may or may not return latest
     * keys depending on whether they were added before or after current
     * iterator position.
     * <p>
     * NOTE: this operation is not distributed and returns only the keys cached on this node.
     *
     * @return Primary key set for the current node.
     */
    public Set<K> primaryKeySet();

    /**
     * Collection of values cached on this node. You can remove
     * elements from this collection, but you cannot add elements to this collection.
     * All removal operation will be reflected on the cache itself.
     * <p>
     * Iterator over this collection will not fail if collection was
     * concurrently updated by another thread. This means that iterator may or
     * may not return latest values depending on whether they were added before
     * or after current iterator position.
     * <p>
     * NOTE: this operation is not distributed and returns only the values cached on this node.
     *
     * @return Collection of cached values.
     */
    public Iterable<V> values();

    /**
     * Gets set of all entries cached on this node. You can remove
     * elements from this set, but you cannot add elements to this set.
     * All removal operation will be reflected on the cache itself.
     * <p>
     * NOTE: this operation is not distributed and returns only the entries cached on this node.
     *
     * @return Entries that pass through key filter.
     */
    public Set<Cache.Entry<K, V>> entrySet();

    /**
     * Gets set containing cache entries that belong to provided partition or {@code null}
     * if partition is not found locally.
     * <p>
     * NOTE: this operation is not distributed and returns only the entries cached on this node.
     *
     * @param part Partition.
     * @return Set containing partition's entries or {@code null} if partition is
     *      not found locally.
     */
    @Nullable public Set<Cache.Entry<K, V>> entrySet(int part);

    /**
     * Starts new transaction with the specified concurrency and isolation.
     *
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @return New transaction.
     * @throws IllegalStateException If transaction is already started by this thread.
     * @throws UnsupportedOperationException If cache is {@link CacheAtomicityMode#ATOMIC}.
     */
    public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation);

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @return New transaction.
     */
    public IgniteInternalTx txStartEx(TransactionConcurrency concurrency, TransactionIsolation isolation);

    /**
     * Starts transaction with specified isolation, concurrency, timeout, invalidation flag,
     * and number of participating entries.
     *
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Number of entries participating in transaction (may be approximate).
     * @return New transaction.
     * @throws IllegalStateException If transaction is already started by this thread.
     * @throws UnsupportedOperationException If cache is {@link CacheAtomicityMode#ATOMIC}.
     */
    public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation, long timeout,
        int txSize);

    /**
     * Gets transaction started by this thread or {@code null} if this thread does
     * not have a transaction.
     *
     * @return Transaction started by this thread or {@code null} if this thread
     *      does not have a transaction.
     */
    @Nullable public Transaction tx();

    /**
     * Evicts entry associated with given key from cache. Note, that entry will be evicted
     * only if it's not used (not participating in any locks or transactions).
     * <p>
     * If {@link org.apache.ignite.configuration.CacheConfiguration#isSwapEnabled()} is set to {@code true}, the evicted entry will
     * be swapped to offheap, and then to disk.
     *
     * @param key Key to evict from cache.
     * @return {@code True} if entry could be evicted, {@code false} otherwise.
     */
    public boolean evict(K key);

    /**
     * Attempts to evict all entries associated with keys. Note,
     * that entry will be evicted only if it's not used (not
     * participating in any locks or transactions).
     * <p>
     * If {@link org.apache.ignite.configuration.CacheConfiguration#isSwapEnabled()} is set to {@code true}, the evicted entry will
     * be swapped to offheap, and then to disk.
     *
     * @param keys Keys to evict.
     */
    public void evictAll(@Nullable Collection<? extends K> keys);

    /**
     * Clears all entries from this cache only if the entry is not
     * currently locked or participating in a transaction.
     * <p>
     * If {@link org.apache.ignite.configuration.CacheConfiguration#isSwapEnabled()} is set to {@code true}, the evicted entries will
     * also be cleared from swap.
     * <p>
     * Note that this operation is local as it merely clears
     * entries from local cache. It does not remove entries from
     * remote caches or from underlying persistent storage.
     *
     * @param srv Whether to clear server cache.
     * @param near Whether to clear near cache.
     * @param readers Whether to clear readers.
     */
    public void clearLocally(boolean srv, boolean near, boolean readers);

    /**
     * Clears an entry from this cache and swap storage only if the entry
     * is not currently locked, and is not participating in a transaction.
     * <p>
     * If {@link org.apache.ignite.configuration.CacheConfiguration#isSwapEnabled()} is set to {@code true}, the evicted entries will
     * also be cleared from swap.
     * <p>
     * Note that this operation is local as it merely clears
     * an entry from local cache. It does not remove entries from
     * remote caches or from underlying persistent storage.
     *
     * @param key Key to clearLocally.
     * @return {@code True} if entry was successfully cleared from cache, {@code false}
     *      if entry was in use at the time of this method invocation and could not be
     *      cleared.
     */
    public boolean clearLocally(K key);

    /**
     * Clears entries from this cache and swap storage only if the entry
     * is not currently locked, and is not participating in a transaction.
     * <p>
     * If {@link org.apache.ignite.configuration.CacheConfiguration#isSwapEnabled()} is set to {@code true}, the evicted entries will
     * also be cleared from swap.
     * <p>
     * Note that this operation is local as it merely clears
     * an entry from local cache. It does not remove entries from
     * remote caches or from underlying persistent storage.
     *
     * @param keys Keys to clearLocally.
     * @param srv Whether to clear server cache.
     * @param near Whether to clear near cache.
     * @param readers Whether to clear readers.
     */
    public void clearLocallyAll(Set<? extends K> keys, boolean srv, boolean near, boolean readers);

    /**
     * Clears key on all nodes that store it's data. That is, caches are cleared on remote
     * nodes and local node, as opposed to {@link IgniteInternalCache#clearLocally(Object)} method which only
     * clears local node's cache.
     * <p>
     * Ignite will make the best attempt to clear caches on all nodes. If some caches
     * could not be cleared, then exception will be thrown.
     *
     * @param key Key to clear.
     * @throws IgniteCheckedException In case of cache could not be cleared on any of the nodes.
     */
    public void clear(K key) throws IgniteCheckedException;

    /**
     * Clears keys on all nodes that store it's data. That is, caches are cleared on remote
     * nodes and local node, as opposed to {@link IgniteInternalCache#clearLocallyAll(Set, boolean, boolean, boolean)} method which only
     * clears local node's cache.
     * <p>
     * Ignite will make the best attempt to clear caches on all nodes. If some caches
     * could not be cleared, then exception will be thrown.
     *
     * @param keys Keys to clear.
     * @throws IgniteCheckedException In case of cache could not be cleared on any of the nodes.
     */
    public void clearAll(Set<? extends K> keys) throws IgniteCheckedException;

    /**
     * Clears cache on all nodes that store it's data. That is, caches are cleared on remote
     * nodes and local node, as opposed to {@link IgniteInternalCache#clearLocally(boolean, boolean, boolean)} method which only
     * clears local node's cache.
     * <p>
     * Ignite will make the best attempt to clear caches on all nodes. If some caches
     * could not be cleared, then exception will be thrown.
     * <p>
     *
     * @throws IgniteCheckedException In case of cache could not be cleared on any of the nodes.
     */
    public void clear() throws IgniteCheckedException;

    /**
     * @return Clear future.
     */
    public IgniteInternalFuture<?> clearAsync();

    /**
     * @param key Key to clear.
     * @return Clear future.
     */
    public IgniteInternalFuture<?> clearAsync(K key);

    /**
     * @param keys Keys to clear.
     * @return Clear future.
     */
    public IgniteInternalFuture<?> clearAllAsync(Set<? extends K> keys);

    /**
     * Removes given key mapping from cache. If cache previously contained value for the given key,
     * then this value is returned. In case of {@link CacheMode#PARTITIONED} or {@link CacheMode#REPLICATED}
     * caches, the value will be loaded from the primary node, which in its turn may load the value
     * from the disk-based swap storage, and consecutively, if it's not in swap,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, <code>CacheStore#load(Transaction, Object)</code> method will be used.
     * <p>
     * If the returned value is not needed, method <code>#removex(Object, org.apache.ignite.lang.IgnitePredicate[])</code> should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     * <p>
     * If write-through is enabled, the value will be removed from {@link CacheStore}
     * via <code>CacheStore#remove(Transaction, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key whose mapping is to be removed from cache.
     * @return Previous value associated with specified key, or {@code null}
     *      if there was no value for this key.
     * @throws NullPointerException If key is {@code null}.
     * @throws IgniteCheckedException If remove operation failed.
     */
    @Nullable public V getAndRemove(K key)
        throws IgniteCheckedException;

    /**
     * Asynchronously removes given key mapping from cache. If cache previously contained value for the given key,
     * then this value is returned. In case of {@link CacheMode#PARTITIONED} or {@link CacheMode#REPLICATED}
     * caches, the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, <code>CacheStore#load(Transaction, Object)</code> method will be used.
     * <p>
     * If the returned value is not needed, method <code>#removex(Object, org.apache.ignite.lang.IgnitePredicate[])</code> should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     * <p>
     * If write-through is enabled, the value will be removed from {@link CacheStore}
     * via <code>CacheStore#remove(Transaction, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key whose mapping is to be removed from cache.
     * @return Future for the remove operation.
     * @throws NullPointerException if the key is {@code null}.
     */
    public IgniteInternalFuture<V> getAndRemoveAsync(K key);

    /**
     * Removes given key mapping from cache.
     * <p>
     * This method will return {@code true} if remove did occur, which means that all optionally
     * provided filters have passed and there was something to remove, {@code false} otherwise.
     * <p>
     * If write-through is enabled, the value will be removed from {@link CacheStore}
     * via <code>CacheStore#remove(Transaction, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key whose mapping is to be removed from cache.
     * @return {@code True} if filter passed validation and entry was removed, {@code false} otherwise.
     *      Note that if filter is not specified, this method will return {@code true}.
     * @throws NullPointerException if the key is {@code null}.
     * @throws IgniteCheckedException If remove failed.
     */
    public boolean remove(K key) throws IgniteCheckedException;

    /**
     * Asynchronously removes given key mapping from cache.
     * <p>
     * This method will return {@code true} if remove did occur, which means that all optionally
     * provided filters have passed and there was something to remove, {@code false} otherwise.
     * <p>
     * If write-through is enabled, the value will be removed from {@link CacheStore}
     * via <code>CacheStore#remove(Transaction, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key whose mapping is to be removed from cache.
     * @return Future for the remove operation. The future will return {@code true}
     *      if optional filters passed validation and remove did occur, {@code false} otherwise.
     *      Note that if filter is not specified, this method will return {@code true}.
     * @throws NullPointerException if the key is {@code null}.
     */
    public IgniteInternalFuture<Boolean> removeAsync(K key);

    /**
     * Removes given key mapping from cache if one exists and value is equal to the passed in value.
     * <p>
     * If write-through is enabled, the value will be removed from {@link CacheStore}
     * via <code>CacheStore#remove(Transaction, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key whose mapping is to be removed from cache.
     * @param val Value to match against currently cached value.
     * @return {@code True} if entry was removed and passed in value matched the cached one,
     *      {@code false} otherwise.
     * @throws NullPointerException if the key or value is {@code null}.
     * @throws IgniteCheckedException If remove failed.
     */
    public boolean remove(K key, V val) throws IgniteCheckedException;

    /**
     * Asynchronously removes given key mapping from cache if one exists and value is equal to the passed in value.
     * <p>
     * This method will return {@code true} if remove did occur, which means that all optionally
     * provided filters have passed and there was something to remove, {@code false} otherwise.
     * <p>
     * If write-through is enabled, the value will be removed from {@link CacheStore}
     * via <code>CacheStore#remove(Transaction, Object)</code> method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key whose mapping is to be removed from cache.
     * @param val Value to match against currently cached value.
     * @return Future for the remove operation. The future will return {@code true}
     *      if currently cached value will match the passed in one.
     * @throws NullPointerException if the key or value is {@code null}.
     */
    public IgniteInternalFuture<Boolean> removeAsync(K key, V val);

    /**
     * Removes given key mappings from cache.
     * <p>
     * If write-through is enabled, the values will be removed from {@link CacheStore} via {@link IgniteDataStreamer}.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param keys Keys whose mappings are to be removed from cache.
     * @throws IgniteCheckedException If remove failed.
     */
    public void removeAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException;

    /**
     * Asynchronously removes given key mappings from cache for entries.
     * <p>
     * If write-through is enabled, the values will be removed from {@link CacheStore} via {@link IgniteDataStreamer}.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param keys Keys whose mappings are to be removed from cache.
     * @return Future for the remove operation. The future will complete whenever
     *      remove operation completes.
     */
    public IgniteInternalFuture<?> removeAllAsync(@Nullable Collection<? extends K> keys);

    /**
     * Removes mappings from cache.
     * <p>
     * <b>USE WITH CARE</b> - if your cache has many entries then transaction will quickly become very heavy and slow.
     * <p>
     * If write-through is enabled, the values will be removed from {@link CacheStore} via {@link IgniteDataStreamer}.
     * <h2 class="header">Transactions</h2>
     * This method is not transactional.
     *
     * @throws IgniteCheckedException If remove failed.
     */
    public void removeAll() throws IgniteCheckedException;

    /**
     * @return Remove future.
     */
    public IgniteInternalFuture<?> removeAllAsync();

    /**
     * Synchronously acquires lock on a cached object with given
     * key only if the passed in filter (if any) passes. This method
     * together with filter check will be executed as one atomic operation.
     * <h2 class="header">Transactions</h2>
     * Locks are not transactional and should not be used from within transactions. If you do
     * need explicit locking within transaction, then you should use
     * {@link TransactionConcurrency#PESSIMISTIC} concurrency control for transaction
     * which will acquire explicit locks for relevant cache operations.
     *
     * @param key Key to lock.
     * @param timeout Timeout in milliseconds to wait for lock to be acquired
     *      ({@code '0'} for no expiration), {@code -1} for immediate failure if
     *      lock cannot be acquired immediately).
     * @return {@code True} if all filters passed and lock was acquired,
     *      {@code false} otherwise.
     * @throws IgniteCheckedException If lock acquisition resulted in error.
     */
    public boolean lock(K key, long timeout)
        throws IgniteCheckedException;

    /**
     * Asynchronously acquires lock on a cached object with given
     * key only if the passed in filter (if any) passes. This method
     * together with filter check will be executed as one atomic operation.
     * <h2 class="header">Transactions</h2>
     * Locks are not transactional and should not be used from within transactions. If you do
     * need explicit locking within transaction, then you should use
     * {@link TransactionConcurrency#PESSIMISTIC} concurrency control for transaction
     * which will acquire explicit locks for relevant cache operations.
     *
     * @param key Key to lock.
     * @param timeout Timeout in milliseconds to wait for lock to be acquired
     *      ({@code '0'} for no expiration, {@code -1} for immediate failure if
     *      lock cannot be acquired immediately).
     * @return Future for the lock operation. The future will return {@code true}
     *      whenever all filters pass and locks are acquired before timeout is expired,
     *      {@code false} otherwise.
     */
    public IgniteInternalFuture<Boolean> lockAsync(K key, long timeout);

    /**
     * All or nothing synchronous lock for passed in keys. This method
     * together with filter check will be executed as one atomic operation.
     * If at least one filter validation failed, no locks will be acquired.
     * <h2 class="header">Transactions</h2>
     * Locks are not transactional and should not be used from within transactions. If you do
     * need explicit locking within transaction, then you should use
     * {@link TransactionConcurrency#PESSIMISTIC} concurrency control for transaction
     * which will acquire explicit locks for relevant cache operations.
     *
     * @param keys Keys to lock.
     * @param timeout Timeout in milliseconds to wait for lock to be acquired
     *      ({@code '0'} for no expiration).
     * @return {@code True} if all filters passed and locks were acquired before
     *      timeout has expired, {@code false} otherwise.
     * @throws IgniteCheckedException If lock acquisition resulted in error.
     */
    public boolean lockAll(@Nullable Collection<? extends K> keys, long timeout) throws IgniteCheckedException;

    /**
     * All or nothing synchronous lock for passed in keys. This method
     * together with filter check will be executed as one atomic operation.
     * If at least one filter validation failed, no locks will be acquired.
     * <h2 class="header">Transactions</h2>
     * Locks are not transactional and should not be used from within transactions. If you do
     * need explicit locking within transaction, then you should use
     * {@link TransactionConcurrency#PESSIMISTIC} concurrency control for transaction
     * which will acquire explicit locks for relevant cache operations.
     *
     * @param keys Keys to lock.
     * @param timeout Timeout in milliseconds to wait for lock to be acquired
     *      ({@code '0'} for no expiration).
     * @return Future for the collection of locks. The future will return
     *      {@code true} if all filters passed and locks were acquired before
     *      timeout has expired, {@code false} otherwise.
     */
    public IgniteInternalFuture<Boolean> lockAllAsync(@Nullable Collection<? extends K> keys, long timeout);

    /**
     * Unlocks given key only if current thread owns the lock. If optional filter
     * will not pass, then unlock will not happen. If the key being unlocked was
     * never locked by current thread, then this method will do nothing.
     * <h2 class="header">Transactions</h2>
     * Locks are not transactional and should not be used from within transactions. If you do
     * need explicit locking within transaction, then you should use
     * {@link TransactionConcurrency#PESSIMISTIC} concurrency control for transaction
     * which will acquire explicit locks for relevant cache operations.
     *
     * @param key Key to unlock.
     * @throws IgniteCheckedException If unlock execution resulted in error.
     */
    public void unlock(K key) throws IgniteCheckedException;

    /**
     * Unlocks given keys only if current thread owns the locks. Only the keys
     * that have been locked by calling thread and pass through the filter (if any)
     * will be unlocked. If none of the key locks is owned by current thread, then
     * this method will do nothing.
     * <h2 class="header">Transactions</h2>
     * Locks are not transactional and should not be used from within transactions. If you do
     * need explicit locking within transaction, then you should use
     * {@link TransactionConcurrency#PESSIMISTIC} concurrency control for transaction
     * which will acquire explicit locks for relevant cache operations.
     *
     * @param keys Keys to unlock.
     * @throws IgniteCheckedException If unlock execution resulted in error.
     */
    public void unlockAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException;

    /**
     * Checks if any node owns a lock for this key.
     * <p>
     * This is a local in-VM operation and does not involve any network trips
     * or access to persistent storage in any way.
     *
     * @param key Key to check.
     * @return {@code True} if lock is owned by some node.
     */
    public boolean isLocked(K key);

    /**
     * Checks if current thread owns a lock on this key.
     * <p>
     * This is a local in-VM operation and does not involve any network trips
     * or access to persistent storage in any way.
     *
     * @param key Key to check.
     * @return {@code True} if key is locked by current thread.
     */
    public boolean isLockedByThread(K key);

    /**
     * Gets the number of all entries cached on this node. This method will return the count of
     * all cache entries and has O(1) complexity on base {@link IgniteInternalCache}. It is essentially the
     * size of cache key set and is semantically identical to {{@code Cache.keySet().size()}.
     * <p>
     * NOTE: this operation is not distributed and returns only the number of entries cached on this node.
     *
     * @return Size of cache on this node.
     */
    public int size();

    /**
     * Gets the number of all entries cached on this node as a long value. This method will return the count of
     * all cache entries and has O(1) complexity on base {@link IgniteInternalCache}. It is essentially the
     * size of cache key set and is semantically identical to {{@code Cache.keySet().size()}.
     * <p>
     * NOTE: this operation is not distributed and returns only the number of entries cached on this node.
     *
     * @return Size of cache on this node.
     */
    public long sizeLong();

    /**
     * @param peekModes Peek modes.
     * @return Local cache size.
     * @throws IgniteCheckedException If failed.
     */
    public int localSize(CachePeekMode[] peekModes) throws IgniteCheckedException;

    /**
     * @param peekModes Peek modes.
     * @return Local cache size as a long value.
     * @throws IgniteCheckedException If failed.
     */
    public long localSizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException;

    /**
     * @param partition partition.
     * @param peekModes Peek modes.
     * @return Local cache size as a long value.
     * @throws IgniteCheckedException If failed.
     */
    public long localSizeLong(int partition, CachePeekMode[] peekModes) throws IgniteCheckedException;

    /**
     * @param peekModes Peek modes.
     * @return Global cache size.
     * @throws IgniteCheckedException If failed.
     */
    public int size(CachePeekMode[] peekModes) throws IgniteCheckedException;

    /**
     * @param peekModes Peek modes.
     * @return Global cache size as a long value.
     * @throws IgniteCheckedException If failed.
     */
    public long sizeLong(CachePeekMode[] peekModes) throws IgniteCheckedException;

    /**
     * @param partition partition
     * @param peekModes Peek modes.
     * @return Global cache size as a long value.
     * @throws IgniteCheckedException If failed.
     */
    public long sizeLong(int partition, CachePeekMode[] peekModes) throws IgniteCheckedException;

    /**
     * @param peekModes Peek modes.
     * @return Future.
     */
    public IgniteInternalFuture<Integer> sizeAsync(CachePeekMode[] peekModes);

    /**
     * @param peekModes Peek modes.
     * @return Future.
     */
    public IgniteInternalFuture<Long> sizeLongAsync(CachePeekMode[] peekModes);

    /**
     * @param partition partiton
     * @param peekModes Peek modes.
     * @return Future.
     */
    public IgniteInternalFuture<Long> sizeLongAsync(int partition, CachePeekMode[] peekModes);

    /**
     * Gets size of near cache key set. This method will return count of all entries in near
     * cache and has O(1) complexity on base cache projection.
     * <p>
     * Note that for {@code LOCAL} non-distributed caches this method will always return {@code 0}
     *
     * @return Size of near cache key set or {@code 0} if cache is not {@link CacheMode#PARTITIONED}.
     */
    public int nearSize();

    /**
     * Gets the number of all primary entries cached on this node. For {@link CacheMode#LOCAL} non-distributed
     * cache mode, this method is identical to {@link #size()}.
     * <p>
     * For {@link CacheMode#PARTITIONED} and {@link CacheMode#REPLICATED} modes, this method will
     * return number of primary entries cached on this node (excluding any backups). The complexity of
     * this method is O(P), where P is the total number of partitions.
     * <p>
     * NOTE: this operation is not distributed and returns only the number of primary entries cached on this node.
     *
     * @return Number of primary entries in cache.
     */
    public int primarySize();

    /**
     * Gets the number of all primary entries cached on this node as a long value. For {@link CacheMode#LOCAL}
     * non-distributed cache mode, this method is identical to {@link #size()}.
     * <p>
     * For {@link CacheMode#PARTITIONED} and {@link CacheMode#REPLICATED} modes, this method will
     * return number of primary entries cached on this node (excluding any backups). The complexity of
     * this method is O(P), where P is the total number of partitions.
     * <p>
     * NOTE: this operation is not distributed and returns only the number of primary entries cached on this node.
     *
     * @return Number of primary entries in cache.
     */
    public long primarySizeLong();

    /**
     * This method unswaps cache entries by given keys, if any, from swap storage
     * into memory.
     * <h2 class="header">Transactions</h2>
     * This method is not transactional.
     *
     * @param keys Keys to promote entries for.
     * @throws IgniteCheckedException If promote failed.
     */
    public void promoteAll(@Nullable Collection<? extends K> keys) throws IgniteCheckedException;

    /**
     * Gets configuration bean for this cache.
     *
     * @return Configuration bean for this cache.
     */
    public CacheConfiguration configuration();

    /**
     * Gets affinity service to provide information about data partitioning
     * and distribution.
     *
     * @return Cache data affinity service.
     */
    public Affinity<K> affinity();

    /**
     * Gets whole cluster metrics (statistics) for this cache.
     *
     * @return Cache metrics.
     */
    public CacheMetrics clusterMetrics();

    /**
     * Gets cluster group metrics (statistics) for this cache.
     *
     * @param grp Cluster group.
     * @return Cache metrics.
     */
    public CacheMetrics clusterMetrics(ClusterGroup grp);

    /**
     * Gets local metrics (statistics) for this cache.
     *
     * @return Cache metrics.
     */
    public CacheMetrics localMetrics();

    /**
     * Gets whole cluster metrics (statistics) for this cache.
     *
     * @return Cache metrics.
     */
    public CacheMetricsMXBean clusterMxBean();

    /**
     * Gets local metrics (statistics) for this cache.
     *
     * @return Cache metrics.
     */
    public CacheMetricsMXBean localMxBean();

    /**
     * Gets size (in bytes) of all entries swapped to disk.
     *
     * @return Size (in bytes) of all entries swapped to disk.
     * @throws IgniteCheckedException In case of error.
     */
    public long overflowSize() throws IgniteCheckedException;

    /**
     * Gets number of cache entries stored in off-heap memory.
     *
     * @return Number of cache entries stored in off-heap memory.
     */
    public long offHeapEntriesCount();

    /**
     * Gets memory size allocated in off-heap.
     *
     * @return Allocated memory size.
     */
    public long offHeapAllocatedSize();

    /**
     * Gets size in bytes for swap space.
     *
     * @return Size in bytes.
     * @throws IgniteCheckedException If failed.
     */
    public long swapSize() throws IgniteCheckedException;

    /**
     * Gets number of swap entries (keys).
     *
     * @return Number of entries stored in swap.
     * @throws IgniteCheckedException If failed.
     */
    public long swapKeys() throws IgniteCheckedException;

    /**
     * Forces this cache node to re-balance its partitions. This method is usually used when
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
     * {@link IgniteConfiguration#setConsistentId(Serializable)} to make sure that
     * a node maps to the same hash ID if re-started.
     * <p>
     * See {@link org.apache.ignite.configuration.CacheConfiguration#getRebalanceDelay()} for more information on how to configure
     * rebalance re-partition delay.
     * <p>
     * @return Future that will be completed when rebalancing is finished.
     */
    public IgniteInternalFuture<?> rebalance();

    /**
     * Creates projection for specified subject ID.
     *
     * @param subjId Client ID.
     * @return Internal projection.
     */
    public IgniteInternalCache<K, V> forSubjectId(UUID subjId);

    /**
     * Store DR data.
     *
     * @param drMap DR map.
     * @throws IgniteCheckedException If put operation failed.
     */
    public void putAllConflict(Map<KeyCacheObject, GridCacheDrInfo> drMap) throws IgniteCheckedException;

    /**
     * Store DR data asynchronously.
     *
     * @param drMap DR map.
     * @return Future.
     * @throws IgniteCheckedException If put operation failed.
     */
    public IgniteInternalFuture<?> putAllConflictAsync(Map<KeyCacheObject, GridCacheDrInfo> drMap)
        throws IgniteCheckedException;

    /**
     * Removes DR data.
     *
     * @param drMap DR map.
     * @throws IgniteCheckedException If remove failed.
     */
    public void removeAllConflict(Map<KeyCacheObject, GridCacheVersion> drMap) throws IgniteCheckedException;

    /**
     * Removes DR data asynchronously.
     *
     * @param drMap DR map.
     * @return Future.
     * @throws IgniteCheckedException If remove failed.
     */
    public IgniteInternalFuture<?> removeAllConflictAsync(Map<KeyCacheObject, GridCacheVersion> drMap) throws IgniteCheckedException;

    /**
     * Gets value from cache. Will go to primary node even if this is a backup.
     *
     * @param key Key to get value for.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public V getForcePrimary(K key) throws IgniteCheckedException;

    /**
     * Asynchronously gets value from cache. Will go to primary node even if this is a backup.
     *
     * @param key Key to get value for.
     * @return Future with result.
     */
    public IgniteInternalFuture<V> getForcePrimaryAsync(K key);

    /**
     * Gets values from cache. Will bypass started transaction, if any, i.e. will not enlist entries
     * and will not lock any keys if pessimistic transaction is started by thread.
     *
     * @param keys Keys to get values for.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    public Map<K, V> getAllOutTx(Set<? extends K> keys) throws IgniteCheckedException;

    /**
     * Gets values from cache. Will bypass started transaction, if any, i.e. will not enlist entries
     * and will not lock any keys if pessimistic transaction is started by thread.
     *
     * @param keys Keys to get values for.
     * @return Future for getAllOutTx operation.
     */
    public IgniteInternalFuture<Map<K, V>> getAllOutTxAsync(Set<? extends K> keys);

    /**
     * Checks whether this cache is IGFS data cache.
     *
     * @return {@code True} in case this cache is IGFS data cache.
     */
    public boolean isIgfsDataCache();

    /**
     * Get current amount of used IGFS space in bytes.
     *
     * @return Amount of used IGFS space in bytes.
     */
    public long igfsDataSpaceUsed();

    /**
     * Get maximum space available for IGFS.
     *
     * @return Amount of space available for IGFS in bytes.
     */
    public long igfsDataSpaceMax();

    /**
     * Checks whether this cache is Mongo data cache.
     *
     * @return {@code True} if this cache is mongo data cache.
     */
    public boolean isMongoDataCache();

    /**
     * Checks whether this cache is Mongo meta cache.
     *
     * @return {@code True} if this cache is mongo meta cache.
     */
    public boolean isMongoMetaCache();

    /**
     * Gets entry set containing internal entries.
     *
     * @param filter Filter.
     * @return Entry set.
     */
    public Set<Cache.Entry<K, V>> entrySetx(CacheEntryPredicate... filter);

    /**
     * @return {@link javax.cache.expiry.ExpiryPolicy} associated with this projection.
     */
    @Nullable public ExpiryPolicy expiry();

    /**
     * @param plc {@link ExpiryPolicy} to associate with this projection.
     * @return New projection based on this one, but with the specified expiry policy.
     */
    public IgniteInternalCache<K, V> withExpiryPolicy(ExpiryPolicy plc);

    /**
     * @return Cache with no-retries behavior enabled.
     */
    public IgniteInternalCache<K, V> withNoRetries();

    /**
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param args Arguments.
     * @return Invoke result.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <T> EntryProcessorResult<T> invoke(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException;

    /**
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param args Arguments.
     * @return Future.
     */
    public <T> IgniteInternalFuture<EntryProcessorResult<T>> invokeAsync(K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args);

    /**
     * @param keys Keys.
     * @param entryProcessor Entry processor.
     * @param args Arguments.
     * @return Invoke results.
     * @throws IgniteCheckedException If failed.
     */
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException;

    /**
     * @param keys Keys.
     * @param entryProcessor Entry processor.
     * @param args Arguments.
     * @return Future.
     */
    public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args);

    /**
     * @param map Map containing keys and entry processors to be applied to values.
     * @param args Arguments.
     * @return Invoke results.
     * @throws IgniteCheckedException If failed.
     */
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws IgniteCheckedException;

    /**
     * @param map Map containing keys and entry processors to be applied to values.
     * @param args Arguments.
     * @return Future.
     */
    public <T> IgniteInternalFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args);

    /**
     * @return Context.
     */
    public GridCacheContext<K, V> context();

    /**
     * Delegates to {@link CacheStore#loadCache(org.apache.ignite.lang.IgniteBiInClosure,Object...)} method
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
     *      {@link CacheStore#loadCache(org.apache.ignite.lang.IgniteBiInClosure, Object...)} method.
     * @throws IgniteCheckedException If loading failed.
     */
    public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args)
        throws IgniteCheckedException;

    /**
     * Asynchronously delegates to {@link CacheStore#loadCache(org.apache.ignite.lang.IgniteBiInClosure, Object...)} method
     * to reload state from the underlying persistent storage. The reloaded values
     * will then be given to the optionally passed in predicate, and if the predicate returns
     * {@code true}, will be stored in cache. If predicate is {@code null}, then
     * all reloaded values will be stored in cache.
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
     *      {@link CacheStore#loadCache(org.apache.ignite.lang.IgniteBiInClosure,Object...)} method.
     * @return Future to be completed whenever loading completes.
     */
    public IgniteInternalFuture<?> localLoadCacheAsync(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args);

    /**
     * Gets value without waiting for toplogy changes.
     *
     * @param key Key.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    public V getTopologySafe(K key) throws IgniteCheckedException;

    /**
     * Tries to get and put value in cache. Will fail with {@link GridCacheTryPutFailedException}
     * if topology exchange is in progress.
     *
     * @param key Key.
     * @param val value.
     * @return Old value.
     * @throws IgniteCheckedException In case of error.
     */
    @Nullable public V tryGetAndPut(K key, V val) throws IgniteCheckedException;

    /**
     * @param topVer Locked topology version.
     * @param key Key.
     * @param entryProcessor Entry processor.
     * @param args Arguments.
     * @return Invoke result.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <T> EntryProcessorResult<T> invoke(
        @Nullable AffinityTopologyVersion topVer,
        K key,
        EntryProcessor<K, V, T> entryProcessor,
        Object... args) throws IgniteCheckedException;
}
