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

package org.apache.ignite.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.table.mapper.Mappers;
import org.jetbrains.annotations.NotNull;

/**
 * Key-Value view of table provides methods to access table records.
 *
 * @param <K> Mapped key type.
 * @param <V> Mapped value type.
 * @apiNote 'Key/value class field' &gt;-&lt; 'table column' mapping laid down in implementation.
 * @see Mappers
 */
public interface KeyValueView<K, V> {
    /**
     * Gets a value associated with the given key.
     *
     * @param key The key whose associated value is to be returned.
     * @return Value or {@code null}, if it does not exist.
     */
    V get(K key);

    /**
     * Asynchronously gets a value associated with the given key.
     *
     * @param key The key whose associated value is to be returned.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAsync(K key);

    /**
     * Get values associated with given keys.
     *
     * @param keys Keys whose associated values are to be returned.
     * @return Values associated with given keys.
     */
    Map<K, V> getAll(Collection<K> keys);

    /**
     * Get values associated with given keys.
     *
     * @param keys Keys whose associated values are to be returned.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Map<K, V>> getAllAsync(Collection<K> keys);

    /**
     * Determines if the table contains an entry for the specified key.
     *
     * @param key The key whose presence is to be tested.
     * @return {@code True} if a value exists for the specified key, {@code false} otherwise.
     */
    boolean contains(K key);

    /**
     * Puts value associated with given key into the table.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     */
    void put(K key, V val);

    /**
     * Asynchronously puts value associated with given key into the table.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> putAsync(K key, V val);

    /**
     * Put associated key-value pairs.
     *
     * @param pairs Key-value pairs.
     */
    void putAll(Map<K, V> pairs);

    /**
     * Asynchronously put associated key-value pairs.
     *
     * @param pairs Key-value pairs.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> putAllAsync(Map<K, V> pairs);

    /**
     * Puts new or replaces existed value associated with given key into the table.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return Replaced value or {@code null}, if not existed.
     */
    V getAndPut(K key, V val);

    /**
     * Asynchronously puts new or replaces existed value associated with given key into the table.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndPutAsync(K key, V val);

    /**
     * Puts value associated with given key into the table if not exists.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return {@code True} if successful, {@code false} otherwise.
     */
    boolean putIfAbsent(K key, @NotNull V val);

    /**
     * Asynchronously puts value associated with given key into the table if not exists.
     *
     * @param key Key with which the specified value is to be associated.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> putIfAbsentAsync(K key, V val);

    /**
     * Removes value associated with given key from the table.
     *
     * @param key Key whose mapping is to be removed from the table.
     * @return {@code True} if a value associated with the specified key was successfully removed, {@code false} otherwise.
     */
    boolean remove(K key);

    /**
     * Asynchronously removes value associated with given key from the table.
     *
     * @param key Key whose mapping is to be removed from the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> removeAsync(K key);

    /**
     * Removes expected value associated with given key from the table.
     *
     * @param key Key whose associated value is to be removed from the table.
     * @param val Expected value.
     * @return {@code True} if the expected value for the specified key was successfully removed, {@code false} otherwise.
     */
    boolean remove(K key, @NotNull V val);

    /**
     * Asynchronously removes expected value associated with given key from the table.
     *
     * @param key Key whose associated value is to be removed from the table.
     * @param val Expected value.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> removeAsync(K key, V val);

    /**
     * Remove values associated with given keys from the table.
     *
     * @param keys Keys whose mapping is to be removed from the table.
     * @return Keys whose values were not existed.
     */
    Collection<K> removeAll(Collection<K> keys);

    /**
     * Asynchronously remove values associated with given keys from the table.
     *
     * @param keys Keys whose mapping is to be removed from the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<K> removeAllAsync(Collection<K> keys);

    /**
     * Gets then removes value associated with given key from the table.
     *
     * @param key Key whose associated value is to be removed from the table.
     * @return Removed value or {@code null}, if not existed.
     */
    V getAndRemove(K key);

    /**
     * Asynchronously gets then removes value associated with given key from the table.
     *
     * @param key Key whose mapping is to be removed from the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndRemoveAsync(K key);

    /**
     * Replaces the value for a key only if exists. This is equivalent to
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except that the action is performed atomically.
     *
     * @param key Key with which the specified value is associated.
     * @param val Value to be associated with the specified key.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     */
    boolean replace(K key, V val);

    /**
     * Asynchronously replaces the value for a key only if exists.
     * See {@link #replace(Object, Object)}.
     *
     * @param key Key with which the specified value is associated.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(K key, V val);

    /**
     * Replaces the expected value for a key. This is equivalent to
     * <pre><code>
     * if (cache.get(key) == oldVal) {
     *   cache.put(key, newVal);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except that the action is performed atomically.
     *
     * @param key Key with which the specified value is associated.
     * @param oldVal Expected value associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     */
    boolean replace(K key, V oldVal, V newVal);

    /**
     * Asynchronously replaces the expected value for a key.
     * See {@link #replace(Object, Object, Object)}
     *
     * @param key Key with which the specified value is associated.
     * @param oldVal Expected value associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(K key, V oldVal, V newVal);

    /**
     * Replaces the value for a given key only if exists. This is equivalent to
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
     * @return Replaced value, or {@code null} if not existed.
     */
    V getAndReplace(K key, V val);

    /**
     * Asynchronously replaces the value for a given key only if exists.
     * See {@link #getAndReplace(Object, Object)}
     *
     * @param key Key with which the specified value is associated.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndReplaceAsync(K key, V val);

    /**
     * Executes invoke processor code against the value associated with the provided key.
     *
     * @param key Key associated with the value that invoke processor will be applied to.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @param <R> Invoke processor result type.
     * @return Result of the processing.
     * @see InvokeProcessor
     */
    <R extends Serializable> R invoke(K key, InvokeProcessor<K, V, R> proc, Serializable... args);

    /**
     * Asynchronously executes invoke processor code against the value associated with the provided key.
     *
     * @param key Key associated with the value that invoke processor will be applied to.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @param <R> Invoke processor result type.
     * @return Future representing pending completion of the operation.
     * @see InvokeProcessor
     */
    @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(K key, InvokeProcessor<K, V, R> proc,
        Serializable... args);

    /**
     * Executes invoke processor code against values associated with the provided keys.
     *
     * @param <R> Invoke processor result type.
     * @param keys Ordered collection of keys which values associated with should be processed.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @return Results of the processing.
     * @see InvokeProcessor
     */
    <R extends Serializable> Map<K, R> invokeAll(
        Collection<K> keys,
        InvokeProcessor<K, V, R> proc,
        Serializable... args);

    /**
     * Asynchronously executes invoke processor code against values associated with the provided keys.
     *
     * @param <R> Invoke processor result type.
     * @param keys Ordered collection of keys which values associated with should be processed.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @return Future representing pending completion of the operation.
     * @see InvokeProcessor
     */
    @NotNull <R extends Serializable> CompletableFuture<Map<K, R>> invokeAllAsync(
        Collection<K> keys,
        InvokeProcessor<K, V, R> proc,
        Serializable... args);
}
