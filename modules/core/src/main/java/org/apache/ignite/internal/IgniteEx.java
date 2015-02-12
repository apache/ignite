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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.hadoop.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Extended Grid interface which provides some additional methods required for kernal and Visor.
 */
public interface IgniteEx extends Ignite, ClusterGroupEx, IgniteCluster {
    /**
     * Gets utility cache.
     *
     * @param keyCls Key class.
     * @param valCls Value class.
     * @return Utility cache.
     */
    public <K extends GridCacheUtilityKey, V> GridCacheProjectionEx<K, V> utilityCache(Class<K> keyCls, Class<V> valCls);

    /**
     * Gets the cache instance for the given name if one is configured or
     * <tt>null</tt> otherwise returning even non-public caches.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @param name Cache name.
     * @return Cache instance for given name or <tt>null</tt> if one does not exist.
     */
    @Nullable public <K, V> GridCache<K, V> cachex(@Nullable String name);

    /**
     * Gets default cache instance if one is configured or <tt>null</tt> otherwise returning even non-public caches.
     * The {@link org.apache.ignite.cache.GridCache#name()} method on default instance returns <tt>null</tt>.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Default cache instance.
     */
    @Nullable public <K, V> GridCache<K, V> cachex();

    /**
     * Gets configured cache instance that satisfy all provided predicates including non-public caches. If no
     * predicates provided - all configured caches will be returned.
     *
     * @param p Predicates. If none provided - all configured caches will be returned.
     * @return Configured cache instances that satisfy all provided predicates.
     */
    public Collection<GridCache<?, ?>> cachesx(@Nullable IgnitePredicate<? super GridCache<?, ?>>... p);

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
     * Gets list of compatible versions.
     *
     * @return Compatible versions.
     */
    public Collection<String> compatibleVersions();

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
     * Whether or not SMTP is configured.
     *
     * @return {@code True} if SMTP is configured - {@code false} otherwise.
     */
    public boolean isSmtpEnabled();

    /**
     * Schedule sending of given email to all configured admin emails.
     */
    IgniteInternalFuture<Boolean> sendAdminEmailAsync(String subj, String body, boolean html);

    /**
     * Get GGFS instance returning null if it doesn't exist.
     *
     * @param name GGFS name.
     * @return GGFS.
     */
    @Nullable public IgniteFs ggfsx(@Nullable String name);

    /**
     * Get Hadoop facade.
     *
     * @return Hadoop.
     */
    public GridHadoop hadoop();

    /**
     * Get latest version in string form.
     *
     * @return Latest version.
     */
    @Nullable public String latestVersion();
}
