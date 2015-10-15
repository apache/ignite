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

package org.apache.ignite.internal;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.hadoop.Hadoop;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Extended Grid interface which provides some additional methods required for kernal and Visor.
 */
public interface IgniteEx extends Ignite {
    /**
     * Gets utility cache.
     *
     * @return Utility cache.
     */
    public <K extends GridCacheUtilityKey, V> IgniteInternalCache<K, V> utilityCache();

    /**
     * Gets the cache instance for the given name if one is configured or
     * <tt>null</tt> otherwise returning even non-public caches.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @param name Cache name.
     * @return Cache instance for given name or <tt>null</tt> if one does not exist.
     */
    @Nullable public <K, V> IgniteInternalCache<K, V> cachex(@Nullable String name);

    /**
     * Gets default cache instance if one is configured or <tt>null</tt> otherwise returning even non-public caches.
     * The {@link IgniteInternalCache#name()} method on default instance returns <tt>null</tt>.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Default cache instance.
     */
    @Nullable public <K, V> IgniteInternalCache<K, V> cachex();

    /**
     * Gets configured cache instance that satisfy all provided predicates including non-public caches. If no
     * predicates provided - all configured caches will be returned.
     *
     * @param p Predicates. If none provided - all configured caches will be returned.
     * @return Configured cache instances that satisfy all provided predicates.
     */
    public Collection<IgniteInternalCache<?, ?>> cachesx(@Nullable IgnitePredicate<? super IgniteInternalCache<?, ?>>... p);

    /**
     * Checks if the event type is user-recordable.
     *
     * @param type Event type to check.
     * @return {@code true} if passed event should be recorded, {@code false} - otherwise.
     */
    public boolean eventUserRecordable(int type);

    /**
     * Checks whether all provided events are user-recordable.
     * <p>
     * Note that this method supports only predefined Ignite events.
     *
     * @param types Event types.
     * @return Whether all events are recordable.
     * @throws IllegalArgumentException If {@code types} contains user event type.
     */
    public boolean allEventsUserRecordable(int[] types);

    /**
     * Whether or not remote JMX management is enabled for this node.
     *
     * @return {@code True} if remote JMX management is enabled - {@code false} otherwise.
     */
    public boolean isJmxRemoteEnabled();

    /**
     * Whether or not node restart is enabled.
     *
     * @return {@code True} if restart mode is enabled, {@code false} otherwise.
     */
    public boolean isRestartEnabled();

    /**
     * Get IGFS instance returning null if it doesn't exist.
     *
     * @param name IGFS name.
     * @return IGFS.
     */
    @Nullable public IgniteFileSystem igfsx(@Nullable String name);

    /**
     * Get Hadoop facade.
     *
     * @return Hadoop.
     */
    public Hadoop hadoop();

    /** {@inheritDoc} */
    @Override IgniteClusterEx cluster();

    /**
     * Get latest version in string form.
     *
     * @return Latest version.
     */
    @Nullable public String latestVersion();

    /**
     * Gets local grid node.
     *
     * @return Local grid node.
     */
    public ClusterNode localNode();

    /**
     * Internal context.
     *
     * @return Kernal context.
     */
    public GridKernalContext context();
}