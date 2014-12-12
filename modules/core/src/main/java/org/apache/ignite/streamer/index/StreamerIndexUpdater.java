/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.streamer.index;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;

/**
 * Index updater. The main responsibility of index updater is to maintain index values
 * up to date whenever events are added or removed from window.
 * <p>
 * Updater is provided to index provider in configuration usually via
 * {@link StreamerIndexProviderAdapter#setUpdater(StreamerIndexUpdater)} method.
 */
public interface StreamerIndexUpdater<E, K, V> {
    /**
     * Given an event, extract index key. For example, if you have a 'Person' object
     * with field 'age' and need to index based on this field, then this method
     * should return the value of age field.
     * <p>
     * If {@code null} is returned then event will be ignored by the index.
     *
     * @param evt Event being added or removed from the window.
     * @return Index key for this event.
     */
    @Nullable public K indexKey(E evt);

    /**
     * Gets initial value for the index or {@code null} if event should be ignored.
     * This method is called every time when an entry is added to the window in
     * order to get initial value for given key.
     *
     * @param evt Event being added to or removed from window.
     * @param key Index key return by {@link #indexKey(Object)} method.
     * @return Initial value for given key, if {@code null} then event will be
     *      ignored and index entry will not be created.
     */
    @Nullable public V initialValue(E evt, K key);

    /**
     * Callback invoked whenever an event is being added to the window. Given a key and
     * a current index value for this key, the implementation should return the new
     * value for this key. If returned value is {@code null}, then current entry will
     * be removed from the index.
     * <p>
     * If index is sorted, then sorting happens based on the returned value.
     *
     * @param entry Current index entry.
     * @param evt New event.
     * @return New index value for given key, if {@code null}, then current
     *      index entry will be removed the index.
     * @throws IgniteCheckedException If entry should not be added to index (e.g. if uniqueness is violated).
     */
    @Nullable public V onAdded(StreamerIndexEntry<E, K, V> entry, E evt) throws IgniteCheckedException;

    /**
     * Callback invoked whenever an event is being removed from the window and has
     * index entry for given key. If there was no entry for given key, then
     * {@code onRemoved()} will not be called.
     * <p>
     * Given a key and a current index value for this key, the implementation should return the new
     * value for this key. If returned value is {@code null}, then current entry will
     * be removed from the index.
     * <p>
     * If index is sorted, then sorting happens based on the returned value.
     *
     * @param entry Current index entry.
     * @param evt Event being removed from the window.
     * @return New index value for given key, if {@code null}, then current
     *      index entry will be removed the index.
     */
    @Nullable public V onRemoved(StreamerIndexEntry<E, K, V> entry, E evt);
}
