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

package org.apache.ignite.streamer.index;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * User view on streamer index. Streamer indexes are used for fast look ups into streamer windows.
 * <p>
 * Streamer index can be accessed from {@link org.apache.ignite.streamer.StreamerWindow} via any of the following methods:
 * <ul>
 * <li>{@link org.apache.ignite.streamer.StreamerWindow#index()}</li>
 * <li>{@link org.apache.ignite.streamer.StreamerWindow#index(String)}</li>
 * <li>{@link org.apache.ignite.streamer.StreamerWindow#indexes()}</li>
 * </ul>
 * <p>
 * Indexes are created and provided for streamer windows by {@link StreamerIndexProvider} which is
 * specified in streamer configuration.
 * <h1 class="header">Example of how to use indexes</h1>
 * <p>
 * Stock price events are streamed into the system, the stock price event is an object containing stock symbol and price.
 * We need to get minimum price for GOOG instrument.
 * <p>
 * Here is {@link StreamerIndexUpdater} that maintains index values up to date:
 * <pre name="code" class="java">
 * class StockPriceIndexUpdater implements GridStreamerIndexUpdater&lt;StockPriceEvent, String, Double&gt; {
 *     &#64;Nullable &#64;Override public String indexKey(StockPriceEvent evt) {
 *         return evt.getSymbol(); // Symbol is an index key.
 *     }
 *
 *     &#64;Nullable &#64;Override public Double initialValue(StockPriceEvent evt, String key) {
 *         return evt.getPrice(); // Set first event's price as an initial value.
 *     }
 *
 *     &#64;Nullable &#64;Override public Double onAdded(GridStreamerIndexEntry&lt;StockPriceEvent, String, Double&gt; entry,
 *         StockPriceEvent evt) throws IgniteCheckedException {
 *         return Math.min(entry.value(), evt.getPrice()); // Update the minimum on new event.
 *     }
 *
 *     &#64;Nullable &#64;Override
 *     public Double onRemoved(GridStreamerIndexEntry&lt;StockPriceEvent, String, Double&gt; entry, StockPriceEvent evt) {
 *         return entry.value(); // Leave minimum unchanged when event is evicted.
 *     }
 * }
 * </pre>
 * <p>
 * Here is the code that queries minimum price for GOOG instrument using index:
 * <pre name="code" class="java">
 * double minGooglePrice = streamer.context().reduce(
 *     // This closure will execute on remote nodes.
 *     new GridClosure&lt;GridStreamerContext, Double&gt;() {
 *         &#64;Nullable &#64;Override public Double apply(GridStreamerContext ctx) {
 *             GridStreamerIndex&lt;StockPriceEvent, String, Double&gt; minIdx = ctx.&lt;StockPriceEvent&gt;window().index("min-price");
 *
 *             return minIdx.entry("GOOG").value();
 *         }
 *     },
 *     new GridReducer&lt;Double, Double&gt;() {
 *         private double minPrice = Integer.MAX_VALUE;
 *
 *         &#64;Override public boolean collect(Double price) {
 *             minPrice = Math.min(minPrice, price); // Take minimum price from all nodes.
 *
 *             return true;
 *         }
 *
 *         &#64;Override public Double reduce() {
 *             return minPrice;
 *         }
 *     }
 * );
 * </pre>
 */
public interface StreamerIndex<E, K, V> extends Iterable<StreamerIndexEntry<E, K, V>> {
    /**
     * Index name.
     *
     * @return Index name.
     */
    @Nullable public String name();

    /**
     * Gets index unique flag. If index is unique then exception
     * will be thrown if key is already present in the index.
     *
     * @return Index unique flag.
     */
    public boolean unique();

    /**
     * Returns {@code true} if index supports sorting and therefore can perform range operations.
     * <p>
     * Note that sorting happens by value and not by key.
     *
     * @return Index sorted flag.
     */
    public boolean sorted();

    /**
     * Gets index policy.
     *
     * @return Index policy.
     */
    public StreamerIndexPolicy policy();

    /**
     * @return Number entries in the index.
     */
    public int size();

    /**
     * Gets index entry for given key.
     *
     * @param key Key for which to retrieve entry.
     * @return Entry for given key, or {@code null} if one could not be found.
     */
    @Nullable public StreamerIndexEntry<E, K, V> entry(K key);

    /**
     * Gets read-only collection of entries in the index.
     * <p>
     * Returned collection is ordered for sorted indexes.
     *
     * @param cnt If 0 then all entries are returned,
     *      if positive, then returned collection contains up to {@code cnt} elements
     *      (in ascending order for sorted indexes),
     *      if negative, then returned collection contains up to {@code |cnt|} elements
     *      (in descending order for sorted indexes and not supported for unsorted indexes).
     * @return Collection of entries in the index.
     */
    public Collection<StreamerIndexEntry<E, K, V>> entries(int cnt);

    /**
     * Gets read-only set of index keys.
     * <p>
     * Returned collection is ordered for sorted indexes.
     *
     * @param cnt If 0 then all keys are returned,
     *      if positive, then returned collection contains up to {@code cnt} elements
     *      (in ascending order for sorted indexes),
     *      if negative, then returned collection contains up to {@code |cnt|} elements
     *      (in descending order for sorted indexes and not supported for unsorted indexes).
     * @return Read-only set of index keys within given position range.
     */
    public Set<K> keySet(int cnt);

    /**
     * Gets read-only collection of index values.
     * <p>
     * Returned collection is ordered for sorted indexes.
     *
     * @param cnt If 0 then all values are returned,
     *      if positive, then returned collection contains up to {@code cnt} elements
     *      (in ascending order for sorted indexes),
     *      if negative, then returned collection contains up to {@code |cnt|} elements
     *      (in descending order for sorted indexes and not supported for unsorted indexes).
     * @return Read-only collections of index values.
     */
    public Collection<V> values(int cnt);

    /**
     * Gets read-only collection of index events.
     * <p>
     * For sorted indexes events are guaranteed to be grouped by corresponding values, however
     * the order of the events corresponding to the same value is not defined.
     *
     * @param cnt If 0 then all values are returned,
     *      if positive, then returned collection contains up to {@code cnt} elements
     *      (in ascending order of values for sorted indexes),
     *      if negative, then returned collection contains up to {@code |cnt|} elements
     *      (in descending order of values for sorted indexes and not supported for unsorted indexes).
     * @return Read-only collections of index events.
     * @throws IllegalStateException If index is not configured to track events.
     * @see StreamerIndexPolicy
     */
    public Collection<E> events(int cnt);

    /**
     * Gets read-only set of index entries with given value.
     * <p>
     * This operation is only available for sorted indexes.
     *
     * @param val Value.
     * @return Read-only set of index entries with given value.
     */
    public Set<StreamerIndexEntry<E, K, V>> entrySet(V val);

    /**
     * Gets read-only set of index entries within given value range.
     * <p>
     * This operation is only available for sorted indexes.
     *
     * @param asc {@code True} for ascending set.
     * @param fromVal From value, if {@code null}, then start from beginning.
     * @param toVal To value, if {@code null} then include all entries until the end.
     * @param fromIncl Whether or not left value is inclusive (ignored if {@code minVal} is {@code null}).
     * @param toIncl Whether or not right value is inclusive (ignored if {@code maxVal} is {@code null}).
     * @return Read-only set of index entries within given value range.
     */
    public Set<StreamerIndexEntry<E, K, V>> entrySet(boolean asc, @Nullable V fromVal, boolean fromIncl,
        @Nullable V toVal, boolean toIncl);

    /**
     * Gets read-only set of index keys with given value. Iteration order over
     * this set has the same order as within index.
     * <p>
     * This operation is only available for sorted indexes.
     *
     * @param val Value.
     * @return Read-only set of index entries with given value.
     */
    public Set<K> keySet(V val);

    /**
     * Gets read-only set of index keys within given value range. Iteration order over
     * this set has the same order as within index.
     * <p>
     * This operation is only available for sorted indexes.
     *
     * @param asc {@code True} for ascending set.
     * @param fromVal From value, if {@code null}, then start from beginning.
     * @param toVal To value, if {@code null} then include all entries until the end.
     * @param fromIncl Whether or not left value is inclusive (ignored if {@code minVal} is {@code null}).
     * @param toIncl Whether or not right value is inclusive (ignored if {@code maxVal} is {@code null}).
     * @return Read-only set of index entries within given value range.
     */
    public Set<K> keySet(boolean asc, @Nullable V fromVal, boolean fromIncl, @Nullable V toVal, boolean toIncl);

    /**
     * Gets read-only collection of index values within given value range. Iteration order over
     * this collection has the same order as within index.
     * <p>
     * This operation is only available for sorted indexes.
     *
     * @param asc {@code True} for ascending set.
     * @param fromVal From value, if {@code null}, then start from beginning.
     * @param toVal To value, if {@code null} then include all entries until the end.
     * @param fromIncl Whether or not left value is inclusive (ignored if {@code minVal} is {@code null}).
     * @param toIncl Whether or not right value is inclusive (ignored if {@code maxVal} is {@code null}).
     * @return Read-only set of index entries within given value range.
     */
    public Collection<V> values(boolean asc, @Nullable V fromVal, boolean fromIncl, @Nullable V toVal, boolean toIncl);

    /**
     * Gets read-only collection of index events.
     * <p>
     * This operation is only available for sorted indexes.
     *
     * @param val From value, if {@code null}, then start from beginning.
     * @return Read-only set of index entries with given value.
     */
    public Collection<E> events(@Nullable V val);

    /**
     * Gets read-only collection of index events.
     * <p>
     * Events are guaranteed to be sorted by corresponding values, however
     * the order of the events corresponding to the same value is not defined.
     * <p>
     * This operation is only available for sorted indexes.
     *
     * @param asc {@code True} for ascending set.
     * @param fromVal From value, if {@code null}, then start from beginning.
     * @param toVal To value, if {@code null} then include all entries until the end.
     * @param fromIncl Whether or not left value is inclusive (ignored if {@code minVal} is {@code null}).
     * @param toIncl Whether or not right value is inclusive (ignored if {@code maxVal} is {@code null}).
     * @return Read-only set of index entries within given value range.
     */
    public Collection<E> events(boolean asc, @Nullable V fromVal, boolean fromIncl, @Nullable V toVal, boolean toIncl);

    /**
     * Gets first entry in the index.
     * <p>
     * This operation is only available for sorted indexes.
     *
     * @return First entry in the index, or {@code null} if index is empty.
     */
    @Nullable public StreamerIndexEntry<E, K, V> firstEntry();

    /**
     * Gets last entry in the index.
     * <p>
     * This operation is only available for sorted indexes.
     *
     * @return Last entry in the index, or {@code null} if index is empty.
     */
    @Nullable public StreamerIndexEntry<E, K, V> lastEntry();
}
