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

package org.apache.ignite.cache;

import java.io.Serializable;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Cache interceptor. Cache interceptor can be used for getting callbacks before
 * and after cache {@code get(...)}, {@code put(...)}, and {@code remove(...)}
 * operations. The {@code onBefore} callbacks can also be used to change the values
 * stored in cache or preventing entries from being removed from cache.
 * <p>
 * Cache interceptor is configured via {@link CacheConfiguration#getInterceptor()}
 * configuration property.
 * <p>
 * Any grid resource from {@code org.apache.ignite.resources} package can be injected
 * into implementation of this interface.
 */
public interface CacheInterceptor<K, V> extends Serializable {
    /**
     * This method is called within {@link IgniteCache#get(Object)}
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
     * @see Cache#get(Object)
     */
    @Nullable public V onGet(K key, @Nullable V val);

    /**
     * This method is called within {@link IgniteCache#put(Object, Object)}
     * and similar operations before new value is stored in cache.
     * <p>
     * Implementations should not execute any complex logic,
     * including locking, networking or cache operations,
     * as it may lead to deadlock, since this method is called
     * from sensitive synchronization blocks.
     * <p>
     * This method should not throw any exception.
     *
     * @param entry Old entry. If {@link CacheConfiguration#isCopyOnRead()} is {@code true}, then is copy.
     * @param newVal New value.
     * @return Value to be put to cache. Returning {@code null} cancels the update.
     * @see IgniteCache#put(Object, Object)
     */
    @Nullable public V onBeforePut(Cache.Entry<K, V> entry, V newVal);

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
     * @param entry Current entry. If {@link CacheConfiguration#isCopyOnRead()} is {@code true} then
     *      entry is a copy.
     */
    public void onAfterPut(Cache.Entry<K, V> entry);

    /**
     * This method is called within {@link IgniteCache#remove(Object)}
     * and similar operations to provide control over returned value.
     * <p>
     * Implementations should not execute any complex logic,
     * including locking, networking or cache operations,
     * as it may lead to deadlock, since this method is called
     * from sensitive synchronization blocks.
     * <p>
     * This method should not throw any exception.
     *
     * @param entry Old entry. If {@link CacheConfiguration#isCopyOnRead()} is {@code true} then
     *      entry is a copy.
     * @return Tuple. The first value is the flag whether remove should be cancelled or not.
     *      The second is the value to be returned as result of {@code remove()} operation,
     *      may be {@code null}.
     * @see IgniteCache#remove(Object)
     */
    @Nullable public IgniteBiTuple<Boolean, V> onBeforeRemove(Cache.Entry<K, V> entry);

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
     * @param entry Removed entry. If {@link CacheConfiguration#isCopyOnRead()} is {@code true} then
     *      entry is a copy.
     */
    public void onAfterRemove(Cache.Entry<K, V> entry);
}