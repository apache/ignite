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

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;

public interface IgniteMultimap<K, V> extends Closeable {

    /**
     * Returns collection of values to which the specified key is mapped,
     * or empty collection if this multimap contains no mapping for the key
     *
     * @param key the key whose associated values are to be returned
     * @return the list of values to which the specified key is mapped,
     * or empty collection if this multimap contains no mapping for the key
     */
    public List<V> get(K key);

    /**
     * Returns the value at a certain index to which the specified key is mapped,
     * or {@code null} if this multimap contains no mapping for the key
     *
     * @param key the key whose associated values are to be returned
     * @param index index to lookup.
     * @return the value at a certain index to which the specified key is mapped,
     * or {@code null} if this multimap contains no mapping for the key
     * @throws IndexOutOfBoundsException if the index is out of range (index < 0 || index >= list.size())
     */
    public V get(K key, int index);

    /**
     * Returns the values for specified range of indexes, between min and max
     * to which the specified key is mapped, or empty collection if this multimap contains
     * no mapping for the key
     *
     * @param key the key whose associated values are to be returned
     * @param min min index to lookup
     * @param max max index to lookup
     * @return the values for specified range of indexes, between min and max
     * to which the specified key is mapped, or empty collection if this multimap contains
     * no mapping for the key
     * @throws IndexOutOfBoundsException if either of indexes min or max is out of
     * range (index < 0 || index >= list.size())
     */
    public List<V> get(K key, int min, int max);

    /**
     * Returns the list of values for specified indexes to which the specified key
     * is mapped, or empty collection if this multimap contains no mapping for the key
     *
     * @param key the key whose associated values are to be returned
     * @param indexes the indexes to lookup
     * @return the list of values for specified indexes to which the specified key
     * is mapped, or empty collection if this multimap contains no mapping for the key
     * @throws IndexOutOfBoundsException if either of indexes is out of
     * range (index < 0 || index >= list.size())
     */
    public List<V> get(K key, Iterable<Integer> indexes);

    /**
     * Returns the list of values for a collection of keys to which the keys are mapped
     * Empty collection will be added to resulting list if this multimap contains no mapping for the key
     *
     * @param keys collection of keys
     * @return the list of values for a collection of keys to which the keys are mapped.
     * Empty collection will be added to resulting list if this multimap contains no mapping for the key
     */
    public Map<K, List<V>> getAll(Collection<K> keys);

    /**
     * Returns a value at a certain index for a collection of keys to which the keys are mapped,
     * or {@code null} if this multimap contains no mapping for the key
     *
     * @param keys collection of keys
     * @param index the index to lookup
     * @return a value at a certain index for a collection of keys to which the keys are mapped,
     * or {@code null} if this multimap contains no mapping for the key
     * @throws IndexOutOfBoundsException if the indexes is out of range (index < 0 || index >= list.size())
     */
    public Map<K, V> getAll(Collection<K> keys, int index);

    /**
     * Returns the list of values for the range of indexes, between min and max for a collection
     * of keys to which the keys are mapped. Empty collection will be added to resulting list if this
     * multimap contains no mapping for the key
     *
     * @param keys collection of keys
     * @param min min index to lookup
     * @param max max index to lookup
     * @return the list of values for the range of indexes, between min and max for a collection
     * of keys to which the keys are mapped. Empty collection will be added to resulting list if this
     * multimap contains no mapping for the key
     * @throws IndexOutOfBoundsException if either of indexes min or max is out of
     * range (index < 0 || index >= list.size())
     */
    public Map<K, List<V>> getAll(Collection<K> keys, int min, int max);

    /**
     * Returns the list of values for specified indexes or a collection of keys to which the keys are
     * mapped. Empty collection will be added to resulting list if this multimap contains no mapping for the key
     *
     * @param keys collection of keys
     * @param indexes the indexes to lookup
     * @return the list of values for specified indexes or a collection of keys to which the keys are
     * mapped. Empty collection will be added to resulting list if this multimap contains no mapping for the key
     * @throws IndexOutOfBoundsException if either of indexes is out of range (index < 0 || index >= list.size())
     */
    public Map<K, List<V>> getAll(Collection<K> keys, Iterable<Integer> indexes);

    /**
     * Clears the multimap. Removes all key-value pairs
     */
    public void clear();

    /**
     * Returns {@code true} if this multimap contains a mapping for the specified key
     *
     * @param key key whose presence in this multimap is to be tested
     * @return {@code true} if this multimap contains a mapping for the specified key
     */
    public boolean containsKey(K key);

    /**
     * Returns {@code true} if this multimap contains at least one key-value pair
     * with the value {@code value}
     *
     * @param value value whose presence in this multimap is to be tested
     * @return {@code true} if this multimap contains at least one key-value pair
     * with the value {@code value}
     */
    public boolean containsValue(V value);

    /**
     * Returns whether the multimap contains the given key-value pair
     *
     * @param key key whose presence in this multimap is to be tested
     * @param value value whose presence in this multimap is to be tested
     * @return {@code true} if the multimap contains the key-value pair, {@code false} otherwise
     */
    public boolean containsEntry(K key, V value);

    /**
     * Returns a {@link Iterator} view of the mappings contained in this multimap
     *
     * @return a {@link Iterator} view of the mappings contained in this multimap
     */
    public Iterator<Map.Entry<K, V>> entries();

    /**
     * Returns the locally owned set of keys
     *
     * @return the locally owned set of keys
     */
    public Set<K> localKeySet();

    /**
     * Returns a {@link Set} view of the keys contained in this multimap
     *
     * @return a {@link Set} view of the keys contained in this multimap
     */
    public Set<K> keySet();

    /**
     * Associates the specified value with the specified key in this multimap
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return {@code true} if the method increased the size of the multimap, or
     * {@code false} if the multimap already contained the key-value pair and
     * doesn't allow duplicates
     */
    public boolean put(K key, V value);

    /**
     * Stores a key-value pair in this multimap for each of {@code values}, all
     * using the same key, {@code key}. Equivalent to (but expected to be more
     * efficient than): <pre>   {@code
     *
     *   for (V value : values) {
     *     put(key, value);
     *   }}</pre>
     *
     * <p>In particular, this is a no-op if {@code values} is empty
     *
     * @param key key with which the specified value is to be associated
     * @param values values to be associated with the specified key
     * @return {@code true} if the multimap changed
     */
    public boolean putAll(K key, Iterable<? extends V> values);

    /**
     * Stores all key-value pairs of {@code multimap} in this multimap, in the
     * order returned by {@code multimap.entries()}
     *
     * @param multimap a multimap to store
     * @return {@code true} if the multimap changed
     */
    public boolean putAll(IgniteMultimap<? extends K, ? extends V> multimap);

    /**
     * Removes the key and all values associated with the {@code key}
     *
     * @param key key to remove
     * @return the {@code Collection} of values that were removed, or empty {@code Collection}
     * if this multimap contains no mapping for the key
     */
    public List<V> remove(K key);

    /**
     * Removes a single key-value pair with the {@code key} and the {@code value} from this multimap,
     * if such exists. If multiple key-value pairs in the multimap fit this description,
     * which one is removed is unspecified
     *
     * @param key key to remove
     * @param value value to remove
     * @return {@code true} if the multimap changed
     */
    public boolean remove(K key, V value);

    /**
     * Stores a collection of values with the same key, replacing any existing
     * values for that key
     *
     * @param key key to store
     * @param values values to store
     * @return the {@code List } of replaced values, or empty {@code List}
     * if this multimap contains no mapping for the key
     */
    public List<V> replaceValues(K key, Iterable<? extends V> values);

    /**
     * Returns {@code true} if this multimap contains no key-value mappings
     *
     * @return {@code true} if this multimap contains no key-value mappings
     */

    public boolean isEmpty();

    /**
     * Iterate through all elements with a certain index
     *
     * @param index the index to lookup
     * @return Iterator through all elements with a certain index
     * @throws IndexOutOfBoundsException index is out of range for any list of values
     * (index < 0 || index >= list.size())
     */
    public Iterator<Map.Entry<K, V>> iterate(int index);

    /**
     * Returns the number of keys in this multimap
     *
     * @return the number of keys in this multimap
     */
    public long size();

    /**
     * Returns an iterator containing the {@code value} from each key-value
     * pair contained in this multimap, without collapsing duplicates
     *
     * @return an iterator containing the {@code value} from each key-value
     * pair contained in this multimap, without collapsing duplicates
     */
    public Iterator<V> values();

    /**
     * Returns the number of values that match the given key in the multimap
     *
     * @param key the key whose values count is to be returned
     * @return the number of values that match the given key in the multimap
     */
    public int valueCount(K key);

    /**
     * Gets multimap name
     *
     * @return Multimap name
     */
    public String name();

    /**
     * Removes this multimap
     *
     * @throws IgniteException If operation failed
     */
    @Override public void close();

    /**
     * Returns {@code true} if this multimap can be kept on the one node only
     * Returns {@code false} if this multimap can be kept on the many nodes
     *
     * @return {@code true} if this multimap is in {@code collocated} mode {@code false} otherwise
     */
    public boolean collocated();

    /**
     * Executes given job on collocated multimap on the node where the multimap is located
     * (a.k.a. affinity co-location). This is not supported for non-collocated multimaps
     *
     * @param job Job which will be co-located with the multimap
     * @throws IgniteException If job failed
     */
    public void affinityRun(IgniteRunnable job);

    /**
     * Executes given job on collocated multimap on the node where the multimap is located
     * (a.k.a. affinity co-location). This is not supported for non-collocated multimaps
     *
     * @param job Job which will be co-located with the multimap
     * @throws IgniteException If job failed
     */
    public <R> R affinityCall(IgniteCallable<R> job);

    /**
     * Gets status of multimap
     *
     * @return {@code true} if multimap was removed from cache {@code false} otherwise
     */
    public boolean removed();
}
