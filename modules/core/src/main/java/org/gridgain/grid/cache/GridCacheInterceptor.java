/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

/**
 * Cache interceptor. Cache interceptor can be used for getting callbacks before
 * and after cache {@code get(...)}, {@code put(...)}, and {@code remove(...)}
 * operations. The {@code onBefore} callbacks can also be used to change the values
 * stored in cache or preventing entries from being removed from cache.
 * <p>
 * Cache interceptor is configured via {@link GridCacheConfiguration#getInterceptor()}
 * configuration property.
 * <p>
 * Any grid resource from {@code org.gridgain.grid.resources} package can be injected
 * into implementation of this interface.
 */
public interface GridCacheInterceptor<K, V> {
    /**
     * This method is called within {@link GridCacheProjection#get(Object)}
     * and similar operations to provide control over returned value.
     * <p>
     * If this method returns {@code null}, then {@code get()} operation
     * results in {@code null} as well.
     * <p>
     * This method should not throw any exception.
     *
     * @param key Key.
     * @param val Value mapped to {@code key} at the moment of {@code get()} operation.
     * @return The new value to be returned as result of {@code get()} operation.
     * @see GridCacheProjection#get(Object)
     */
    @Nullable public V onGet(K key, @Nullable V val);

    /**
     * This method is called within {@link GridCacheProjection#put(Object, Object, GridPredicate[])}
     * and similar operations before new value is stored in cache.
     * <p>
     * Implementations should not execute any complex logic,
     * including locking, networking or cache operations,
     * as it may lead to deadlock, since this method is called
     * from sensitive synchronization blocks.
     * <p>
     * This method should not throw any exception.
     *
     * @param key Key.
     * @param oldVal Old value.
     * @param newVal New value.
     * @return Value to be put to cache. Returning {@code null} cancels the update.
     * @see GridCacheProjection#put(Object, Object, GridPredicate[])
     */
    @Nullable public V onBeforePut(K key, @Nullable V oldVal, V newVal);

    /**
     * This method is called after new value has been stored.
     * <p>
     * Implementations should not execute any complex logic,
     * including locking, networking or cache operations,
     * as it may lead to deadlock, since this method is called
     * from sensitive synchronization blocks.
     * <p>
     * This method should not throw any exception.
     *
     * @param key Key.
     * @param val Current value.
     */
    public void onAfterPut(K key, V val);

    /**
     * This method is called within {@link GridCacheProjection#remove(Object, GridPredicate[])}
     * and similar operations to provide control over returned value.
     * <p>
     * Implementations should not execute any complex logic,
     * including locking, networking or cache operations,
     * as it may lead to deadlock, since this method is called
     * from sensitive synchronization blocks.
     * <p>
     * This method should not throw any exception.
     *
     * @param key Key.
     * @param val Old value.
     * @return Tuple. The first value is the flag whether remove should be cancelled or not.
     *      The second is the value to be returned as result of {@code remove()} operation,
     *      may be {@code null}.
     * @see GridCacheProjection#remove(Object, GridPredicate[])
     */
    @Nullable public GridBiTuple<Boolean, V> onBeforeRemove(K key, @Nullable V val);

    /**
     * This method is called after value has been removed.
     * <p>
     * Implementations should not execute any complex logic,
     * including locking, networking or cache operations,
     * as it may lead to deadlock, since this method is called
     * from sensitive synchronization blocks.
     * <p>
     * This method should not throw any exception.
     *
     * @param key Key.
     * @param val Removed value.
     */
    public void onAfterRemove(K key, V val);
}
