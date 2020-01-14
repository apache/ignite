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
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.cache.CachePeekMode;
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
     * @return The name of the cache.
     */
    public String getName();

    /**
     * @return The cache configuration.
     */
    public ClientCacheConfiguration getConfiguration() throws ClientException;

    /**
     * Gets the number of all entries cached across all nodes. By default, if {@code peekModes} value isn't provided,
     * only size of primary copies across all nodes will be returned. This behavior is identical to calling
     * this method with {@link CachePeekMode#PRIMARY} peek mode.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     */
    public int size(CachePeekMode... peekModes) throws ClientException;

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
     * operation is in progress. or if map is modified while the operation is in
     * progress.
     * <p>
     *
     * @param map Mappings to be stored in this cache.
     */
    public void putAll(Map<? extends K, ? extends V> map) throws ClientException;

    /**
     * Atomically replaces the entry for a key only if currently mapped to a given value.
     * <p>
     * This is equivalent to:
     * <pre><code>
     * if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue)) {
     *  cache.put(key, newValue);
     * return true;
     * } else {
     *  return false;
     * }
     * </code></pre>
     * except that the action is performed atomically.
     *
     * @param key Key with which the specified value is associated.
     * @param oldVal Value expected to be associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return <tt>true</tt> if the value was replaced
     */
    public boolean replace(K key, V oldVal, V newVal) throws ClientException;

    /**
     * Atomically replaces the entry for a key only if currently mapped to some
     * value.
     * <p>
     * This is equivalent to
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except that the action is performed atomically.
     *
     * @param key The key with which the specified value is associated.
     * @param val The value to be associated with the specified key.
     * @return <tt>true</tt> if the value was replaced.
     */
    public boolean replace(K key, V val) throws ClientException;

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
     * Atomically removes the mapping for a key only if currently mapped to the given value.
     * <p>
     * This is equivalent to:
     * <pre><code>
     * if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue) {
     *   cache.remove(key);
     *   return true;
     * } else {
     *   return false;
     * }
     * </code></pre>
     * except that the action is performed atomically.
     *
     * @param key Key whose mapping is to be removed from the cache.
     * @param oldVal Value expected to be associated with the specified key.
     * @return <tt>false</tt> if there was no matching key.
     */
    public boolean remove(K key, V oldVal) throws ClientException;

    /**
     * Removes entries for the specified keys.
     * <p>
     * The order in which the individual entries are removed is undefined.
     *
     * @param keys The keys to remove.
     */
    public void removeAll(Set<? extends K> keys) throws ClientException;

    /**
     * Removes all of the mappings from this cache.
     * <p>
     * The order that the individual entries are removed is undefined.
     */
    public void removeAll() throws ClientException;

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
     * Atomically removes the entry for a key only if currently mapped to some value.
     * <p>
     * This is equivalent to:
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   V oldValue = cache.get(key);
     *   cache.remove(key);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * </code></pre>
     * except that the action is performed atomically.
     *
     * @param key Key with which the specified value is associated.
     * @return The value if one existed or null if no mapping existed for this key.
     */
    public V getAndRemove(K key) throws ClientException;

    /**
     * Atomically replaces the value for a given key if and only if there is a value currently mapped by the key.
     * <p>
     * This is equivalent to
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   V oldValue = cache.get(key);
     *   cache.put(key, value);
     *   return oldValue;
     * } else {
     *   return null;
     * }
     * </code></pre>
     * except that the action is performed atomically.
     *
     * @param key Key with which the specified value is associated.
     * @param val Value to be associated with the specified key.
     * @return The previous value associated with the specified key, or
     * <tt>null</tt> if there was no mapping for the key.
     */
    public V getAndReplace(K key, V val) throws ClientException;

    /**
     * Atomically associates the specified key with the given value if it is not already associated with a value.
     * <p>
     * This is equivalent to:
     * <pre><code>
     * if (!cache.containsKey(key)) {}
     *   cache.put(key, value);
     *   return true;
     * } else {
     *   return false;
     * }
     * </code></pre>
     * except that the action is performed atomically.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return <tt>true</tt> if a value was set.
     */
    public boolean putIfAbsent(K key, V val) throws ClientException;

    /**
     * Clears the contents of the cache.
     */
    public void clear() throws ClientException;

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
     * CacheClient<Integer, BinaryObject> prj = cache.withKeepBinary();
     *
     * // Value is not deserialized and returned in binary format.
     * BinaryObject po = prj.get(1);
     * </pre>
     * <p>
     * Note that this method makes sense only if cache is working in binary mode
     * if default marshaller is used.
     * If not, this method is no-op and will return current cache.
     *
     * @return New cache instance for binary objects.
     */
    public <K1, V1> ClientCache<K1, V1> withKeepBinary();

    /**
     * Returns cache with the specified expired policy set. This policy will be used for each operation invoked on
     * the returned cache.
     *
     * @return Cache instance with the specified expiry policy set.
     */
    public <K1, V1> ClientCache<K1, V1> withExpirePolicy(ExpiryPolicy expirePlc);

    /**
     * Queries cache. Supports {@link ScanQuery} and {@link SqlFieldsQuery}.
     *
     * @param qry Query.
     * @return Cursor.
     *
     */
    public <R> QueryCursor<R> query(Query<R> qry);

    /**
     * Convenience method to execute {@link SqlFieldsQuery}.
     *
     * @param qry Query.
     * @return Cursor.
     */
    public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry);
}
