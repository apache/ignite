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
     * @param key A key which associated the value is to be returned.
     * The key cannot be {@code null}.
     * @return Value or {@code null}, if it does not exist.
     */
    V get(@NotNull K key);

    /**
     * Asynchronously gets a value associated with the given key.
     *
     * @param key A key which associated the value is to be returned.
     * The key cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAsync(@NotNull K key);

    /**
     * Get values associated with given keys.
     *
     * @param keys Keys which associated values are to be returned.
     * The keys cannot be {@code null}.
     * @return Values associated with given keys.
     */
    Map<K, V> getAll(@NotNull Collection<K> keys);

    /**
     * Get values associated with given keys.
     *
     * @param keys Keys whose associated values are to be returned.
     * The keys cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Map<K, V>> getAllAsync(@NotNull Collection<K> keys);

    /**
     * Determines if the table contains an entry for the specified key.
     *
     * @param key A key which presence is to be tested.
     * The keys cannot be {@code null}.
     * @return {@code True} if a value exists for the specified key, {@code false} otherwise.
     */
    boolean contains(@NotNull K key);

    /**
     * Puts value associated with given key into the table.
     *
     * @param key A key with which the specified value is to be associated.
     * The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     */
    void put(@NotNull K key, V val);

    /**
     * Asynchronously puts value associated with given key into the table.
     *
     * @param key A key with which the specified value is to be associated.
     * The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> putAsync(@NotNull K key, V val);

    /**
     * Put associated key-value pairs.
     *
     * @param pairs Key-value pairs.
     * The pairs cannot be {@code null}.
     */
    void putAll(@NotNull Map<K, V> pairs);

    /**
     * Asynchronously put associated key-value pairs.
     *
     * @param pairs Key-value pairs.
     * The pairs cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> putAllAsync(@NotNull Map<K, V> pairs);

    /**
     * Puts new or replaces existed value associated with given key into the table.
     *
     * @param key A key with which the specified value is to be associated.
     * The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Replaced value or {@code null}, if not existed.
     */
    V getAndPut(@NotNull K key, V val);

    /**
     * Asynchronously puts new or replaces existed value associated with given key into the table.
     *
     * @param key A key with which the specified value is to be associated.
     * The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndPutAsync(@NotNull K key, V val);

    /**
     * Puts value associated with given key into the table if not exists.
     *
     * @param key A key with which the specified value is to be associated.
     * The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return {@code True} if successful, {@code false} otherwise.
     */
    boolean putIfAbsent(@NotNull K key, @NotNull V val);

    /**
     * Asynchronously puts value associated with given key into the table if not exists.
     *
     * @param key Key with which the specified value is to be associated.
     * The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@NotNull K key, V val);

    /**
     * Removes value associated with given key from the table.
     *
     * @param key A key which mapping is to be removed from the table.
     * The key cannot be {@code null}.
     * @return {@code True} if a value associated with the specified key was successfully removed, {@code false} otherwise.
     */
    boolean remove(@NotNull K key);

    /**
     * Asynchronously removes value associated with given key from the table.
     *
     * @param key A key which mapping is to be removed from the table.
     * The key cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key);

    /**
     * Removes an expected value associated with the given key from the table.
     *
     * @param key A key which associated value is to be removed from the table.
     * The key cannot be {@code null}.
     * @param val Expected value. The value cannot be {@code null}.
     * @return {@code True} if the expected value for the specified key was successfully removed, {@code false} otherwise.
     */
    boolean remove(@NotNull K key, @NotNull V val);

    /**
     * Asynchronously removes expected value associated with given key from the table.
     *
     * @param key A key which associated the value is to be removed from the table.
     * The key cannot be {@code null}.
     * @param val Expected value. The value cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key, @NotNull V val);

    /**
     * Remove values associated with given keys from the table.
     *
     * @param keys Keys which mapping is to be removed from the table.
     * The keys cannot be {@code null}.
     * @return Keys whose values were not existed.
     */
    Collection<K> removeAll(@NotNull Collection<K> keys);

    /**
     * Asynchronously remove values associated with given keys from the table.
     *
     * @param keys Keys which mapping is to be removed from the table.
     * The keys cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<K>> removeAllAsync(@NotNull Collection<K> keys);

    /**
     * Gets then removes value associated with given key from the table.
     *
     * @param key A key which associated value is to be removed from the table.
     * The key cannot be {@code null}.
     * @return Removed value or {@code null}, if not existed.
     */
    V getAndRemove(@NotNull K key);

    /**
     * Asynchronously gets then removes value associated with given key from the table.
     *
     * @param key A Key which mapping is to be removed from the table.
     * The key cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndRemoveAsync(@NotNull K key);

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
     * @param key A key with which the specified value is associated.
     * The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     */
    boolean replace(@NotNull K key, V val);

    /**
     * Asynchronously replaces the value for a key only if exists.
     * See {@link #replace(Object, Object)}.
     *
     * @param key A key with which the specified value is associated.
     * The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V val);

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
     * @param key A key with which the specified value is associated.
     * The key cannot be {@code null}.
     * @param oldVal Expected value associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return {@code True} if an old value was replaced, {@code false} otherwise.
     */
    boolean replace(@NotNull K key, V oldVal, V newVal);

    /**
     * Asynchronously replaces the expected value for a key.
     * See {@link #replace(Object, Object, Object)}
     *
     * @param key A key with which the specified value is associated.
     * The key cannot be {@code null}.
     * @param oldVal Expected value associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V oldVal, V newVal);

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
     * @param key A key with which the specified value is associated.
     * The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Replaced value, or {@code null} if not existed.
     */
    V getAndReplace(@NotNull K key, V val);

    /**
     * Asynchronously replaces the value for a given key only if exists.
     * See {@link #getAndReplace(Object, Object)}
     *
     * @param key A key with which the specified value is associated.
     * The key cannot be {@code null}.
     * @param val Value to be associated with the specified key.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<V> getAndReplaceAsync(@NotNull K key, V val);

    /**
     * Executes invoke processor code against the value associated with the provided key.
     *
     * @param key A key associated with the value that invoke processor will be applied to.
     * The key cannot be {@code null}.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @param <R> Invoke processor result type.
     * @return Result of the processing.
     * @see InvokeProcessor
     */
    <R extends Serializable> R invoke(@NotNull K key, InvokeProcessor<K, V, R> proc, Serializable... args);

    /**
     * Asynchronously executes invoke processor code against the value associated with the provided key.
     *
     * @param key A key associated with the value that invoke processor will be applied to.
     * The key cannot be {@code null}.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @param <R> Invoke processor result type.
     * @return Future representing pending completion of the operation.
     * @see InvokeProcessor
     */
    @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
        @NotNull K key,
        InvokeProcessor<K, V, R> proc,
        Serializable... args);

    /**
     * Executes invoke processor code against values associated with the provided keys.
     *
     * @param <R> Invoke processor result type.
     * @param keys Ordered collection of keys which values associated with should be processed.
     * The keys cannot be {@code null}.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @return Results of the processing.
     * @see InvokeProcessor
     */
    <R extends Serializable> Map<K, R> invokeAll(
        @NotNull Collection<K> keys,
        InvokeProcessor<K, V, R> proc,
        Serializable... args);

    /**
     * Asynchronously executes invoke processor code against values associated with the provided keys.
     *
     * @param <R> Invoke processor result type.
     * @param keys Ordered collection of keys which values associated with should be processed.
     * The keys cannot be {@code null}.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @return Future representing pending completion of the operation.
     * @see InvokeProcessor
     */
    @NotNull <R extends Serializable> CompletableFuture<Map<K, R>> invokeAllAsync(
        @NotNull Collection<K> keys,
        InvokeProcessor<K, V, R> proc,
        Serializable... args);
}
