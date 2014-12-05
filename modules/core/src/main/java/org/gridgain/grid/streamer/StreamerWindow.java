/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer;

import org.gridgain.grid.*;
import org.gridgain.grid.streamer.index.*;
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
 * <li>{@link org.gridgain.grid.streamer.window.StreamerUnboundedWindow}</li>
 * <li>{@link org.gridgain.grid.streamer.window.StreamerBoundedSizeWindow}</li>
 * <li>{@link org.gridgain.grid.streamer.window.StreamerBoundedSizeBatchWindow}</li>
 * <li>{@link org.gridgain.grid.streamer.window.StreamerBoundedSizeSortedWindow}</li>
 * <li>{@link org.gridgain.grid.streamer.window.StreamerBoundedTimeWindow}</li>
 * <li>{@link org.gridgain.grid.streamer.window.StreamerBoundedTimeBatchWindow}</li>
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
     public <K, V> GridStreamerIndex<E, K, V> index();

    /**
     * Gets index by name, if not index with such name was configured then
     * {@link IllegalArgumentException} will be thrown.
     *
     * @param name Name of the index, if {@code null} then analogous to {@link #index()}.
     * @param <K> Type of the index key.
     * @param <V> Type of the index value.
     * @return Index with a given name.
     */
    public <K, V> GridStreamerIndex<E, K, V> index(@Nullable String name);

    /**
     * Gets all indexes configured for this window.
     *
     * @return All indexes configured for this window or empty collection, if no
     *         indexes were configured.
     */
    public Collection<GridStreamerIndex<E, ?, ?>> indexes();

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
     * @throws GridException If index update failed.
     */
    public boolean enqueue(E evt) throws GridException;

    /**
     * Adds events to window.
     *
     * @param evts Events to add.
     * @return {@code}
     * @throws GridException If index update failed.
     */
    public boolean enqueue(E... evts) throws GridException;

    /**
     * Adds all events to window.
     *
     * @param evts Collection of events to add.
     * @return {@code True} if all events were added, {@code false} if at
     *      least 1 event was skipped.
     * @throws GridException If index update failed.
     */
    public boolean enqueueAll(Collection<E> evts) throws GridException;

    /**
     * Dequeues last element from windows. Will return {@code null} if window is empty.
     *
     * @return Dequeued element.
     * @throws GridException If index update failed.
     */
    @Nullable public E dequeue() throws GridException;

    /**
     * Dequeues up to {@code cnt} elements from window. If current window size is less than {@code cnt},
     * will dequeue all elements from window.
     *
     * @param cnt Count to dequeue.
     * @return Collection of dequeued elements.
     * @throws GridException If index update failed.
     */
    public Collection<E> dequeue(int cnt) throws GridException;

    /**
     * Dequeues all elements from window.
     *
     * @return Collection of dequeued elements.
     * @throws GridException If index update failed.
     */
    public Collection<E> dequeueAll() throws GridException;

    /**
     * If window supports eviction, this method will return next evicted element.
     *
     * @return Polls and returns next evicted event or {@code null} if eviction queue is empty or if
     *      window does not support eviction.
     * @throws GridException If index update failed.
     */
    @Nullable public E pollEvicted() throws GridException;

    /**
     * If window supports eviction, this method will return up to {@code cnt} evicted elements.
     *
     * @param cnt Number of elements to evict.
     * @return Collection of evicted elements.
     * @throws GridException If index update failed.
     */
    public Collection<E> pollEvicted(int cnt) throws GridException;

    /**
     * If window supports batch eviction, this method will poll next evicted batch from window.
     * If windows does not support batch eviction but supports eviction, will return collection of single
     * last evicted element.
     * If window does not support eviction, will return empty collection.
     *
     * @return Next evicted batch.
     * @throws GridException If index update failed.
     */
    public Collection<E> pollEvictedBatch() throws GridException;

    /**
     * If window supports eviction, this method will return all available evicted elements.
     *
     * @return Collection of evicted elements.
     * @throws GridException If index update failed.
     */
    public Collection<E> pollEvictedAll() throws GridException;

    /**
     * Clears all evicted entries.
     *
     * @throws GridException If index update failed.
     */
    public void clearEvicted() throws GridException;

    /**
     * Create window snapshot. Evicted entries are not included.
     *
     * @param includeIvicted Whether to include evicted entries or not.
     * @return Window snapshot.
     */
    public Collection<E> snapshot(boolean includeIvicted);
}
