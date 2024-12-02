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

package org.apache.ignite.client;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;

/**
 * Thin client cache.
 */
public interface ClientCache<K, V> {
    /**
     * Gets an entry from the cache.
     *
     * @param key the key whose associated value is to be returned
     * @return the element, or null, if it does not exist.
     * @throws NullPointerException if the key is null.
     */
    public V get(K key) throws ClientException;

    /**
     * Gets an entry from the cache asynchronously.
     *
     * @param key Key.
     * @return a Future representing pending completion of the operation.
     * Future result is the cache entry value or null if it does not exist.
     */
    public IgniteClientFuture<V> getAsync(K key);

    /**
     * Associates the specified value with the specified key in the cache.
     * <p>
     * If the {@link ClientCache} previously contained a mapping for the key, the old
     * value is replaced by the specified value.
     *
     * @param key key with which the specified value is to be associated
     * @param val value to be associated with the specified key.
     * @throws NullPointerException if key is null or if value is null.
     */
    public void put(K key, V val) throws ClientException;

    /**
     * Associates the specified value with the specified key in the cache asynchronously.
     * <p>
     * If the {@link ClientCache} previously contained a mapping for the key, the old
     * value is replaced by the specified value.
     *
     * @param key key with which the specified value is to be associated
     * @param val value to be associated with the specified key.
     * @return a Future representing pending completion of the operation.
     * @throws NullPointerException if key is null or if value is null.
     */
    public IgniteClientFuture<Void> putAsync(K key, V val) throws ClientException;

    /**
     * Determines if the {@link ClientCache} contains an entry for the specified key.
     * <p>
     * More formally, returns <tt>true</tt> if and only if this cache contains a
     * mapping for a key <tt>k</tt> such that <tt>key.equals(k)</tt>.
     * (There can be at most one such mapping)
     *
     * @param key key whose presence in this cache is to be tested.
     * @return <tt>true</tt> if this map contains a mapping for the specified key.
     */
    public boolean containsKey(K key) throws ClientException;

    /**
     * Determines if the {@link ClientCache} contains an entry for the specified key asynchronously.
     * <p>
     * More formally, returns <tt>true</tt> if and only if this cache contains a
     * mapping for a key <tt>k</tt> such that <tt>key.equals(k)</tt>.
     * (There can be at most one such mapping)
     *
     * @param key key whose presence in this cache is to be tested.
     * @return a Future representing pending completion of the operation.
     * Future result is <tt>true</tt> if this map contains a mapping for the specified key.
     */
    public IgniteClientFuture<Boolean> containsKeyAsync(K key) throws ClientException;

    /**
     * Determines if the {@link ClientCache} contains entries for the specified keys.
     *
     * @param keys Keys whose presence in this cache is to be tested.
     * @return {@code True} if this cache contains a mapping for the specified keys.
     */
    public boolean containsKeys(Set<? extends K> keys) throws ClientException;

    /**
     * Determines if the {@link ClientCache} contains entries for the specified keys asynchronously.
     *
     * @param keys Keys whose presence in this cache is to be tested.
     * @return Future representing pending completion of the operation.
     * Future result is {@code true} if this map contains a mapping for the specified keys.
     */
    public IgniteClientFuture<Boolean> containsKeysAsync(Set<? extends K> keys) throws ClientException;

    /**
     * @return The name of the cache.
     */
    public String getName();

    /**
     * @return The cache configuration.
     */
    public ClientCacheConfiguration getConfiguration() throws ClientException;

    /**
     * Gets the cache configuration asynchronously.
     * @return a Future representing pending completion of the operation, which wraps the cache configuration.
     */
    public IgniteClientFuture<ClientCacheConfiguration> getConfigurationAsync() throws ClientException;

    /**
     * Gets the number of all entries cached across all nodes. By default, if {@code peekModes} value isn't provided,
     * only size of primary copies across all nodes will be returned. This behavior is identical to calling
     * this method with {@link CachePeekMode#PRIMARY} peek mode.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return The number of all entries cached across all nodes.
     */
    public int size(CachePeekMode... peekModes) throws ClientException;

    /**
     * Gets the number of all entries cached across all nodes. By default, if {@code peekModes} value isn't provided,
     * only size of primary copies across all nodes will be returned. This behavior is identical to calling
     * this method with {@link CachePeekMode#PRIMARY} peek mode.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return a Future representing pending completion of the operation, which wraps the cache size.
     */
    public IgniteClientFuture<Integer> sizeAsync(CachePeekMode... peekModes) throws ClientException;

    /**
     * Gets a collection of entries from the {@link ClientCache}, returning them as
     * {@link Map} of the values associated with the set of keys requested.
     *
     * @param keys The keys whose associated values are to be returned.
     * @return A map of entries that were found for the given keys. Keys not found
     * in the cache are not in the returned map.
     */
    public Map<K, V> getAll(Set<? extends K> keys) throws ClientException;

    /**
     * Gets a collection of entries from the {@link ClientCache}, returning them as
     * {@link Map} of the values associated with the set of keys requested.
     *
     * @param keys The keys whose associated values are to be returned.
     * @return a Future representing pending completion of the operation, which wraps a map of entries that
     * were found for the given keys. Keys not found in the cache are not in the returned map.
     */
    public IgniteClientFuture<Map<K, V>> getAllAsync(Set<? extends K> keys) throws ClientException;

    /**
     * Copies all of the entries from the specified map to the {@link ClientCache}.
     * <p>
     * The effect of this call is equivalent to that of calling
     * {@link #put(Object, Object) put(k, v)} on this cache once for each mapping
     * from key <tt>k</tt> to value <tt>v</tt> in the specified map.
     * <p>
     * The order in which the individual puts occur is undefined.
     * <p>
     * The behavior of this operation is undefined if entries in the cache
     * corresponding to entries in the map are modified or removed while this
     * operation is in progress, or if map is modified while the operation is in progress.
     * <p>
     *
     * @param map Mappings to be stored in this cache.
     */
    public void putAll(Map<? extends K, ? extends V> map) throws ClientException;

    /**
     * Copies all of the entries from the specified map to the {@link ClientCache}.
     * <p>
     * The effect of this call is equivalent to that of calling
     * {@link #put(Object, Object) put(k, v)} on this cache once for each mapping
     * from key <tt>k</tt> to value <tt>v</tt> in the specified map.
     * <p>
     * The order in which the individual puts occur is undefined.
     * <p>
     * The behavior of this operation is undefined if entries in the cache
     * corresponding to entries in the map are modified or removed while this
     * operation is in progress, or if map is modified while the operation is in progress.
     * <p>
     *
     * @param map Mappings to be stored in this cache.
     * @return a Future representing pending completion of the operation.
     */
    public IgniteClientFuture<Void> putAllAsync(Map<? extends K, ? extends V> map) throws ClientException;

    /**
     * Atomically replaces the entry for a key only if currently mapped to a given value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue)) {
     *  cache.put(key, newValue);
     * return true;
     * } else {
     *  return false;
     * }
     * </code></pre>
     *
     * @param key Key with which the specified value is associated.
     * @param oldVal Value expected to be associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return <tt>true</tt> if the value was replaced
     */
    public boolean replace(K key, V oldVal, V newVal) throws ClientException;

    /**
     * Atomically replaces the entry for a key only if currently mapped to a given value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue)) {
     *  cache.put(key, newValue);
     * return true;
     * } else {
     *  return false;
     * }
     * </code></pre>
     *
     * @param key Key with which the specified value is associated.
     * @param oldVal Value expected to be associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return a Future representing pending completion of the operation, which wraps a value indicating whether the
     * cache value was replaced.
     */
    public IgniteClientFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) throws ClientException;

    /**
     * Atomically replaces the entry for a key only if currently mapped to some
     * value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     *
     * @param key The key with which the specified value is associated.
     * @param val The value to be associated with the specified key.
     * @return <tt>true</tt> if the value was replaced.
     */
    public boolean replace(K key, V val) throws ClientException;

    /**
     * Atomically replaces the entry for a key only if currently mapped to some
     * value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     *
     * @param key The key with which the specified value is associated.
     * @param val The value to be associated with the specified key.
     * @return a Future representing pending completion of the operation, which wraps a value indicating whether the
     * cache value was replaced.
     */
    public IgniteClientFuture<Boolean> replaceAsync(K key, V val) throws ClientException;

    /**
     * Removes the mapping for a key from this cache if it is present.
     * <p>
     * More formally, if this cache contains a mapping from key <tt>k</tt> to value <tt>v</tt> such that
     * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping is removed.
     * (The cache can contain at most one such mapping.)
     *
     * <p>Returns <tt>true</tt> if this cache previously associated the key, or <tt>false</tt> if the cache
     * contained no mapping for the key.
     * <p>
     * The cache will not contain a mapping for the specified key once the
     * call returns.
     *
     * @param key Key whose mapping is to be removed from the cache.
     * @return <tt>false</tt> if there was no matching key.
     */
    public boolean remove(K key) throws ClientException;

    /**
     * Removes the mapping for a key from this cache if it is present.
     * <p>
     * More formally, if this cache contains a mapping from key <tt>k</tt> to value <tt>v</tt> such that
     * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping is removed.
     * (The cache can contain at most one such mapping.)
     *
     * <p>Returns <tt>true</tt> if this cache previously associated the key, or <tt>false</tt> if the cache
     * contained no mapping for the key.
     * <p>
     * The cache will not contain a mapping for the specified key once the call returns.
     *
     * @param key Key whose mapping is to be removed from the cache.
     * @return a Future representing pending completion of the operation, which wraps a value indicating whether the
     * cache value was removed.
     */
    public IgniteClientFuture<Boolean> removeAsync(K key) throws ClientException;

    /**
     * Atomically removes the mapping for a key only if currently mapped to the given value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue) {
     *   cache.remove(key);
     *   return true;
     * } else {
     *   return false;
     * }
     * </code></pre>
     *
     * @param key Key whose mapping is to be removed from the cache.
     * @param oldVal Value expected to be associated with the specified key.
     * @return <tt>false</tt> if there was no matching key.
     */
    public boolean remove(K key, V oldVal) throws ClientException;

    /**
     * Atomically removes the mapping for a key only if currently mapped to the given value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue) {
     *   cache.remove(key);
     *   return true;
     * } else {
     *   return false;
     * }
     * </code></pre>
     *
     * @param key Key whose mapping is to be removed from the cache.
     * @param oldVal Value expected to be associated with the specified key.
     * @return a Future representing pending completion of the operation, which wraps a value indicating whether the
     * cache value was removed.
     */
    public IgniteClientFuture<Boolean> removeAsync(K key, V oldVal) throws ClientException;

    /**
     * Removes entries for the specified keys.
     * <p>
     * The order in which the individual entries are removed is undefined.
     *
     * @param keys The keys to remove.
     */
    public void removeAll(Set<? extends K> keys) throws ClientException;

    /**
     * Removes entries for the specified keys.
     * <p>
     * The order in which the individual entries are removed is undefined.
     *
     * @param keys The keys to remove.
     * @return a Future representing pending completion of the operation.
     */
    public IgniteClientFuture<Void> removeAllAsync(Set<? extends K> keys) throws ClientException;

    /**
     * Removes all of the mappings from this cache.
     * <p>
     * The order that the individual entries are removed is undefined.
     * <p>
     * This operation is not transactional. It calls broadcast closure that
     * deletes all primary keys from remote nodes.
     * <p>
     * This is potentially an expensive operation as listeners are invoked.
     * Use {@link #clear()} to avoid this.
     */
    public void removeAll() throws ClientException;

    /**
     * Removes all of the mappings from this cache.
     * <p>
     * The order that the individual entries are removed is undefined.
     * <p>
     * This operation is not transactional. It calls broadcast closure that
     * deletes all primary keys from remote nodes.
     * <p>
     * This is potentially an expensive operation as listeners are invoked.
     * Use {@link #clear()} to avoid this.
     * @return a Future representing pending completion of the operation.
     */
    public IgniteClientFuture<Void> removeAllAsync() throws ClientException;

    /**
     * Associates the specified value with the specified key in this cache, returning an existing value if one existed.
     * <p>
     * If the cache previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A cache
     * <tt>c</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(Object) c.containsKey(k)} would return
     * <tt>true</tt>.)
     * <p>
     * The previous value is returned, or null if there was no value associated
     * with the key previously.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return The value associated with the key at the start of the operation or
     * null if none was associated.
     */
    public V getAndPut(K key, V val) throws ClientException;

    /**
     * Associates the specified value with the specified key in this cache, returning an existing value if one existed.
     * <p>
     * If the cache previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A cache
     * <tt>c</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(Object) c.containsKey(k)} would return
     * <tt>true</tt>.)
     * <p>
     * The previous value is returned, or null if there was no value associated
     * with the key previously.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return a Future representing pending completion of the operation, which wraps the value associated with the
     * key at the start of the operation or null if none was associated.
     */
    public IgniteClientFuture<V> getAndPutAsync(K key, V val) throws ClientException;

    /**
     * Atomically removes the entry for a key only if currently mapped to some value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   V oldValue = cache.get(key);
     *   cache.remove(key);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * </code></pre>
     *
     * @param key Key with which the specified value is associated.
     * @return The value if one existed or null if no mapping existed for this key.
     */
    public V getAndRemove(K key) throws ClientException;

    /**
     * Atomically removes the entry for a key only if currently mapped to some value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   V oldValue = cache.get(key);
     *   cache.remove(key);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * </code></pre>
     *
     * @param key Key with which the specified value is associated.
     * @return a Future representing pending completion of the operation, which wraps the value if one existed or null
     * if no mapping existed for this key.
     */
    public IgniteClientFuture<V> getAndRemoveAsync(K key) throws ClientException;

    /**
     * Atomically replaces the value for a given key if and only if there is a value currently mapped by the key.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   V oldValue = cache.get(key);
     *   cache.put(key, value);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * </code></pre>
     *
     * @param key Key with which the specified value is associated.
     * @param val Value to be associated with the specified key.
     * @return The previous value associated with the specified key, or
     * <tt>null</tt> if there was no mapping for the key.
     */
    public V getAndReplace(K key, V val) throws ClientException;

    /**
     * Atomically replaces the value for a given key if and only if there is a value currently mapped by the key.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   V oldValue = cache.get(key);
     *   cache.put(key, value);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * </code></pre>
     *
     * @param key Key with which the specified value is associated.
     * @param val Value to be associated with the specified key.
     * @return a Future representing pending completion of the operation, which wraps the previous value associated
     * with the specified key, or <tt>null</tt> if there was no mapping for the key.
     */
    public IgniteClientFuture<V> getAndReplaceAsync(K key, V val) throws ClientException;

    /**
     * Atomically associates the specified key with the given value if it is not already associated with a value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (!cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return true;
     * } else {
     *   return false;
     * }
     * </code></pre>
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return <tt>true</tt> if a value was set.
     */
    public boolean putIfAbsent(K key, V val) throws ClientException;

    /**
     * Atomically associates the specified key with the given value if it is not already associated with a value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (!cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return true;
     * } else {
     *   return false;
     * }
     * </code></pre>
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return a Future representing pending completion of the operation, which wraps the value indicating whether
     * a value was set.
     */
    public IgniteClientFuture<Boolean> putIfAbsentAsync(K key, V val) throws ClientException;

    /**
     * Atomically associates the specified key with the given value if it is not already associated with a value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (!cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return null;
     * } else {
     *   return cache.get(key);
     * }
     * </code></pre>
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return Value that is already associated with the specified key, or {@code null} if no value was associated
     * with the specified key and a value was set.
     */
    public V getAndPutIfAbsent(K key, V val) throws ClientException;

    /**
     * Atomically associates the specified key with the given value if it is not already associated with a value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (!cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return null;
     * } else {
     *   return cache.get(key);
     * }
     * </code></pre>
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation, which wraps the value that is already
     * associated with the specified key, or {@code null} if no value was associated with the specified key and a
     * value was set.
     */
    public IgniteClientFuture<V> getAndPutIfAbsentAsync(K key, V val) throws ClientException;

    /**
     * Clears the contents of the cache.
     * In contrast to {@link #removeAll()}, this method does not notify event listeners and cache writers.
     */
    public void clear() throws ClientException;

    /**
     * Clears the contents of the cache asynchronously.
     * In contrast to {@link #removeAll()}, this method does not notify event listeners and cache writers.
     * @return a Future representing pending completion of the operation.
     */
    public IgniteClientFuture<Void> clearAsync() throws ClientException;

    /**
     * Clears entry with specified key from the cache.
     * In contrast to {@link #remove(Object)}, this method does not notify event listeners and cache writers.
     *
     * @param key Cache entry key to clear.
     */
    public void clear(K key) throws ClientException;

    /**
     * Clears entry with specified key from the cache asynchronously.
     * In contrast to {@link #removeAsync(Object)}, this method does not notify event listeners and cache writers.
     *
     * @param key Cache entry key to clear.
     * @return Future representing pending completion of the operation.
     */
    public IgniteClientFuture<Void> clearAsync(K key) throws ClientException;

    /**
     * Clears entries with specified keys from the cache.
     * In contrast to {@link #removeAll(Set)}, this method does not notify event listeners and cache writers.
     *
     * @param keys Cache entry keys to clear.
     */
    public void clearAll(Set<? extends K> keys) throws ClientException;

    /**
     * Clears entries with specified keys from the cache asynchronously.
     * In contrast to {@link #removeAllAsync(Set)}, this method does not notify event listeners and cache writers.
     *
     * @param keys Cache entry keys to clear.
     * @return Future representing pending completion of the operation.
     */
    public IgniteClientFuture<Void> clearAllAsync(Set<? extends K> keys) throws ClientException;

    /**
     * Invokes an {@link EntryProcessor} against the {@link javax.cache.Cache.Entry} specified by
     * the provided key. If an {@link javax.cache.Cache.Entry} does not exist for the specified key,
     * an attempt is made to load it (if a loader is configured) or a surrogate
     * {@link javax.cache.Cache.Entry}, consisting of the key with a null value is used instead.
     * <p>
     * An instance of entry processor must be stateless as it may be invoked multiple times on primary and
     * backup nodes in the cache. It is guaranteed that the value passed to the entry processor will be always
     * the same.
     * <p>
     *
     * @param key The key to the entry.
     * @param entryProc The {@link EntryProcessor} to invoke.
     * @param arguments Additional arguments to pass to the {@link EntryProcessor}.
     * @param <T> Type of the cache entry processing result.
     * @return The result of the processing, if any, defined by the {@link EntryProcessor} implementation.
     * @throws NullPointerException If key or {@link EntryProcessor} is null.
     * @throws EntryProcessorException If an exception is thrown by the {@link
     *                                 EntryProcessor}, a Caching Implementation
     *                                 must wrap any {@link Exception} thrown
     *                                 wrapped in an {@link EntryProcessorException}.
     * @throws ClientException If operation is failed.
     */
    public <T> T invoke(
        K key,
        EntryProcessor<K, V, T> entryProc,
        Object... arguments
    ) throws EntryProcessorException, ClientException;

    /**
     * Asynchronously invokes an {@link EntryProcessor} against the {@link javax.cache.Cache.Entry} specified by
     * the provided key. If an {@link javax.cache.Cache.Entry} does not exist for the specified key,
     * an attempt is made to load it (if a loader is configured) or a surrogate
     * {@link javax.cache.Cache.Entry}, consisting of the key with a null value is used instead.
     * <p>
     * An instance of entry processor must be stateless as it may be invoked multiple times on primary and
     * backup nodes in the cache. It is guaranteed that the value passed to the entry processor will be always
     * the same.
     * <p>
     *
     * @param key The key to the entry.
     * @param entryProc The {@link EntryProcessor} to invoke.
     * @param arguments Additional arguments to pass to the {@link EntryProcessor}.
     * @param <T> Type of the cache entry processing result.
     * @return Future representing pending completion of the operation.
     * @throws NullPointerException If key or {@link EntryProcessor} is null.
     * @throws ClientException If operation is failed.
     */
    public <T> IgniteClientFuture<T> invokeAsync(
        K key,
        EntryProcessor<K, V, T> entryProc,
        Object... arguments
    ) throws ClientException;

    /**
     * Invokes each {@link EntryProcessor} against the set of {@link javax.cache.Cache.Entry}s specified by
     * the set of keys.
     * <p>
     * If an {@link javax.cache.Cache.Entry} does not exist for the specified key, an attempt is made
     * to load it (if a loader is configured) or a surrogate {@link javax.cache.Cache.Entry},
     * consisting of the key and a value of null is provided.
     * <p>
     * The order that the entries for the keys are processed is undefined.
     * Implementations may choose to process the entries in any order, including
     * concurrently. Furthermore there is no guarantee implementations will
     * use the same {@link EntryProcessor} instance to process each entry, as
     * the case may be in a non-local cache topology.
     * <p>
     * The result of executing the {@link EntryProcessor} is returned as a
     * {@link Map} of {@link EntryProcessorResult}s, one result per key. Should the
     * {@link EntryProcessor} or Caching implementation throw an exception, the
     * exception is wrapped and re-thrown when a call to
     * {@link javax.cache.processor.EntryProcessorResult#get()} is made.
     * <p>
     * Keys are locked in the order in which they appear in key set. It is caller's responsibility to
     * make sure keys always follow same order, such as by using {@link java.util.TreeSet}. Using unordered map,
     * such as {@link java.util.HashSet}, while calling this method in parallel <b>will lead to deadlock</b>.
     *
     * @param keys The set of keys for entries to proces.
     * @param entryProc The EntryProcessor to invoke.
     * @param args Additional arguments to pass to the {@link EntryProcessor}.
     * @param <T> Type of the cache entry processing result.
     * @return The map of {@link EntryProcessorResult}s of the processing per key,
     *      if any, defined by the {@link EntryProcessor} implementation.  No mappings
     *      will be returned for {@link EntryProcessor}s that return a
     *      <code>null</code> value for a key.
     * @throws NullPointerException If keys or {@link EntryProcessor} is null.
     * @throws ClientException If operation is failed.
     */
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProc,
        Object... args
    ) throws ClientException;

    /**
     * Asynchronously invokes each {@link EntryProcessor} against the set of {@link javax.cache.Cache.Entry}s
     * specified by the set of keys.
     * <p>
     * If an {@link javax.cache.Cache.Entry} does not exist for the specified key, an attempt is made
     * to load it (if a loader is configured) or a surrogate {@link javax.cache.Cache.Entry},
     * consisting of the key and a value of null is provided.
     * <p>
     * The order that the entries for the keys are processed is undefined.
     * Implementations may choose to process the entries in any order, including
     * concurrently. Furthermore there is no guarantee implementations will
     * use the same {@link EntryProcessor} instance to process each entry, as
     * the case may be in a non-local cache topology.
     * <p>
     * The result of executing the {@link EntryProcessor} is returned in the future as a
     * {@link Map} of {@link EntryProcessorResult}s, one result per key. Should the
     * {@link EntryProcessor} or Caching implementation throw an exception, the
     * exception is wrapped and re-thrown when a call to
     * {@link javax.cache.processor.EntryProcessorResult#get()} is made.
     * <p>
     * Keys are locked in the order in which they appear in key set. It is caller's responsibility to
     * make sure keys always follow same order, such as by using {@link java.util.TreeSet}. Using unordered map,
     * such as {@link java.util.HashSet}, while calling this method in parallel <b>will lead to deadlock</b>.
     *
     * @param keys The set of keys for entries to proces.
     * @param entryProc The EntryProcessor to invoke.
     * @param args Additional arguments to pass to the {@link EntryProcessor}.
     * @param <T> Type of the cache entry processing result.
     * @return Future representing pending completion of the operation.
     * @throws NullPointerException If keys or {@link EntryProcessor} is null.
     * @throws ClientException If operation is failed.
     */
    public <T> IgniteClientFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProc,
        Object... args
    ) throws ClientException;

    /**
     * Returns cache that will operate with binary objects.
     * <p>
     * Cache returned by this method will not be forced to deserialize binary objects,
     * so keys and values will be returned from cache API methods without changes. Therefore,
     * signature of the cache can contain only following types:
     * <ul>
     *     <li><code>org.apache.ignite.binary.BinaryObject</code> for binary classes</li>
     *     <li>All primitives (byte, int, ...) and there boxed versions (Byte, Integer, ...)</li>
     *     <li>Arrays of primitives (byte[], int[], ...)</li>
     *     <li>{@link String} and array of {@link String}s</li>
     *     <li>{@link UUID} and array of {@link UUID}s</li>
     *     <li>{@link Date} and array of {@link Date}s</li>
     *     <li>{@link java.sql.Timestamp} and array of {@link java.sql.Timestamp}s</li>
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
     * CacheClient&lt;Integer, BinaryObject&gt; prj = cache.withKeepBinary();
     *
     * // Value is not deserialized and returned in binary format.
     * BinaryObject po = prj.get(1);
     * </pre>
     * <p>
     * Note that this method makes sense only if cache is working in binary mode
     * if default marshaller is used.
     * If not, this method is no-op and will return current cache.
     *
     * @param <K1> Client cache key type.
     * @param <V1> Client cache value type.
     * @return New cache instance for binary objects.
     */
    public <K1, V1> ClientCache<K1, V1> withKeepBinary();

    /**
     * Returns cache with the specified expired policy set. This policy will be used for each operation invoked on
     * the returned cache.
     *
     * @param expirePlc Expiration policy.
     * @param <K1> Client cache key type.
     * @param <V1> Client cache value type.
     * @return Cache instance with the specified expiry policy set.
     */
    public <K1, V1> ClientCache<K1, V1> withExpirePolicy(ExpiryPolicy expirePlc);

    /**
     * Queries cache. Supports {@link ScanQuery}, {@link SqlFieldsQuery} and {@link ContinuousQuery}.
     * <p>
     * NOTE: For continuous query listeners there is no failover in case of client channel failure, this event should
     * be handled on the user's side. Use {@link #query(ContinuousQuery, ClientDisconnectListener)} method to get
     * notified about client disconnected event via {@link ClientDisconnectListener} interface if you need it.
     *
     * @param qry Query.
     * @param <R> Query result type.
     * @return Cursor.
     */
    public <R> QueryCursor<R> query(Query<R> qry);

    /**
     * Start {@link ContinuousQuery} on the cache.
     * <p>
     * NOTE: There is no failover in case of client channel failure, this event should be handled on the user's side.
     * Use {@code disconnectListener} to handle this.
     *
     * @param qry Query.
     * @param disconnectListener Listener of client disconnected event.
     * @param <R> Query result type.
     * @return Cursor.
     */
    public <R> QueryCursor<R> query(ContinuousQuery<K, V> qry, ClientDisconnectListener disconnectListener);

    /**
     * Convenience method to execute {@link SqlFieldsQuery}.
     *
     * @param qry Query.
     * @return Cursor.
     */
    public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry);

    /**
     * Registers a {@link CacheEntryListener}. The supplied {@link CacheEntryListenerConfiguration} is used to
     * instantiate a listener and apply it to those events specified in the configuration.
     * <p>
     * NOTE: There is no failover in case of client channel failure, this event should be handled on the user's side.
     * Use {@link #registerCacheEntryListener(CacheEntryListenerConfiguration, ClientDisconnectListener)} method to get
     * notified about client disconnected event via {@link ClientDisconnectListener} interface if you need it.
     *
     * @param cacheEntryListenerConfiguration a factory and related configuration for creating the listener.
     * @throws IllegalArgumentException is the same CacheEntryListenerConfiguration is used more than once or
     *          if some unsupported by thin client properties are set.
     * @see CacheEntryListener
     */
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);

    /**
     * Registers a {@link CacheEntryListener}. The supplied {@link CacheEntryListenerConfiguration} is used to
     * instantiate a listener and apply it to those events specified in the configuration.
     * <p>
     * NOTE: There is no failover in case of client channel failure, this event should be handled on the user's side.
     * Use {@code disconnectListener} to handle this.
     *
     * @param cacheEntryListenerConfiguration a factory and related configuration for creating the listener.
     * @param disconnectListener Listener of client disconnected event.
     * @throws IllegalArgumentException is the same CacheEntryListenerConfiguration is used more than once or
     *          if some unsupported by thin client properties are set.
     * @see CacheEntryListener
     */
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration,
        ClientDisconnectListener disconnectListener);

    /**
     * Deregisters a listener, using the {@link CacheEntryListenerConfiguration} that was used to register it.
     *
     * @param cacheEntryListenerConfiguration the factory and related configuration that was used to create the
     *         listener.
     */
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration);
}
