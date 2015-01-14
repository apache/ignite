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

package org.apache.ignite.streamer;

import org.apache.ignite.*;
import org.apache.ignite.streamer.index.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Streamer rolling window. Rolling windows allow new event to come in, as well as automatically
 * evicting obsolete events on the other side. Windows allow to query into specific time-frame
 * or specific sample size of the events. With windows, you can answer questions like "What
 * are my top 10 best selling products over last 24 hours?", or "Who are my top 10 users out of
 * last 1,000,000 users who logged in?"
 * <p>
 * GridGain comes with following rolling windows implementations out of the box:
 * <ul>
 * <li>{@link org.apache.ignite.streamer.window.StreamerUnboundedWindow}</li>
 * <li>{@link org.apache.ignite.streamer.window.StreamerBoundedSizeWindow}</li>
 * <li>{@link org.apache.ignite.streamer.window.StreamerBoundedSizeBatchWindow}</li>
 * <li>{@link org.apache.ignite.streamer.window.StreamerBoundedSizeSortedWindow}</li>
 * <li>{@link org.apache.ignite.streamer.window.StreamerBoundedTimeWindow}</li>
 * <li>{@link org.apache.ignite.streamer.window.StreamerBoundedTimeBatchWindow}</li>
 * </ul>
 * <p>
 * Streamer window is configured via {@link StreamerConfiguration#getWindows()} method.
 */
public interface StreamerWindow<E> extends Iterable<E> {
    /**
     * Gets window name.
     *
     * @return Window name.
     */
    public String name();

    /**
     * Gets default index, if default index is not configured then
     * {@link IllegalArgumentException} will be thrown.
     *
     * @param <K> Type of the index key.
     * @param <V> Type of the index value.
     * @return Index with default name.
     */
     public <K, V> StreamerIndex<E, K, V> index();

    /**
     * Gets index by name, if not index with such name was configured then
     * {@link IllegalArgumentException} will be thrown.
     *
     * @param name Name of the index, if {@code null} then analogous to {@link #index()}.
     * @param <K> Type of the index key.
     * @param <V> Type of the index value.
     * @return Index with a given name.
     */
    public <K, V> StreamerIndex<E, K, V> index(@Nullable String name);

    /**
     * Gets all indexes configured for this window.
     *
     * @return All indexes configured for this window or empty collection, if no
     *         indexes were configured.
     */
    public Collection<StreamerIndex<E, ?, ?>> indexes();

    /**
     * Resets window. Usually will clear all events from window.
     */
    public void reset();

    /**
     * Gets number of events currently stored in window.
     *
     * @return Current size of the window.
     */
    public int size();

    /**
     * Gets number of entries available for eviction.
     *
     * @return Number of entries available for eviction.
     */
    public int evictionQueueSize();

    /**
     * Adds single event to window.
     *
     * @param evt Event to add.
     * @return {@code True} if event was added.
     * @throws IgniteCheckedException If index update failed.
     */
    public boolean enqueue(E evt) throws IgniteCheckedException;

    /**
     * Adds events to window.
     *
     * @param evts Events to add.
     * @return {@code}
     * @throws IgniteCheckedException If index update failed.
     */
    public boolean enqueue(E... evts) throws IgniteCheckedException;

    /**
     * Adds all events to window.
     *
     * @param evts Collection of events to add.
     * @return {@code True} if all events were added, {@code false} if at
     *      least 1 event was skipped.
     * @throws IgniteCheckedException If index update failed.
     */
    public boolean enqueueAll(Collection<E> evts) throws IgniteCheckedException;

    /**
     * Dequeues last element from windows. Will return {@code null} if window is empty.
     *
     * @return Dequeued element.
     * @throws IgniteCheckedException If index update failed.
     */
    @Nullable public E dequeue() throws IgniteCheckedException;

    /**
     * Dequeues up to {@code cnt} elements from window. If current window size is less than {@code cnt},
     * will dequeue all elements from window.
     *
     * @param cnt Count to dequeue.
     * @return Collection of dequeued elements.
     * @throws IgniteCheckedException If index update failed.
     */
    public Collection<E> dequeue(int cnt) throws IgniteCheckedException;

    /**
     * Dequeues all elements from window.
     *
     * @return Collection of dequeued elements.
     * @throws IgniteCheckedException If index update failed.
     */
    public Collection<E> dequeueAll() throws IgniteCheckedException;

    /**
     * If window supports eviction, this method will return next evicted element.
     *
     * @return Polls and returns next evicted event or {@code null} if eviction queue is empty or if
     *      window does not support eviction.
     * @throws IgniteCheckedException If index update failed.
     */
    @Nullable public E pollEvicted() throws IgniteCheckedException;

    /**
     * If window supports eviction, this method will return up to {@code cnt} evicted elements.
     *
     * @param cnt Number of elements to evict.
     * @return Collection of evicted elements.
     * @throws IgniteCheckedException If index update failed.
     */
    public Collection<E> pollEvicted(int cnt) throws IgniteCheckedException;

    /**
     * If window supports batch eviction, this method will poll next evicted batch from window.
     * If windows does not support batch eviction but supports eviction, will return collection of single
     * last evicted element.
     * If window does not support eviction, will return empty collection.
     *
     * @return Next evicted batch.
     * @throws IgniteCheckedException If index update failed.
     */
    public Collection<E> pollEvictedBatch() throws IgniteCheckedException;

    /**
     * If window supports eviction, this method will return all available evicted elements.
     *
     * @return Collection of evicted elements.
     * @throws IgniteCheckedException If index update failed.
     */
    public Collection<E> pollEvictedAll() throws IgniteCheckedException;

    /**
     * Clears all evicted entries.
     *
     * @throws IgniteCheckedException If index update failed.
     */
    public void clearEvicted() throws IgniteCheckedException;

    /**
     * Create window snapshot. Evicted entries are not included.
     *
     * @param includeIvicted Whether to include evicted entries or not.
     * @return Window snapshot.
     */
    public Collection<E> snapshot(boolean includeIvicted);
}
