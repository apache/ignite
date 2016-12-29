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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.platform.cache.store.PlatformCacheStore;
import org.jetbrains.annotations.Nullable;

/**
 * Platform processor.
 */
@SuppressWarnings({"UnusedDeclaration", "UnnecessaryInterfaceModifier"})
public interface PlatformProcessor extends GridProcessor {
    /**
     * Gets owning Ignite instance.
     *
     * @return Ignite instance.
     */
    public Ignite ignite();

    /**
     * Get environment pointer associated with this processor.
     *
     * @return Environment pointer.
     */
    public long environmentPointer();

    /**
     * Gets platform context.
     *
     * @return Platform context.
     */
    public PlatformContext context();

    /**
     * Notify processor that it is safe to use.
     */
    public void releaseStart();

    /**
     * Await until platform processor is safe to use (i.e. {@link #releaseStart() has been called}.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void awaitStart() throws IgniteCheckedException;

    /**
     * Get cache.
     *
     * @param name Cache name.
     * @return Cache.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTargetProxy cache(@Nullable String name) throws IgniteCheckedException;

    /**
     * Create cache.
     *
     * @param name Cache name.
     * @return Cache.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTargetProxy createCache(@Nullable String name) throws IgniteCheckedException;

    /**
     * Get or create cache.
     *
     * @param name Cache name.
     * @return Cache.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTargetProxy getOrCreateCache(@Nullable String name) throws IgniteCheckedException;

    /**
     * Create cache.
     *
     * @param memPtr Stream with cache config.
     * @return Cache.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTargetProxy createCacheFromConfig(long memPtr) throws IgniteCheckedException;

    /**
     * Get or create cache.
     *
     * @param memPtr Stream with cache config.
     * @return Cache.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTargetProxy getOrCreateCacheFromConfig(long memPtr) throws IgniteCheckedException;

    /**
     * Destroy dynamically created cache.
     *
     * @param name Cache name.
     * @throws IgniteCheckedException If failed.
     */
    public void destroyCache(@Nullable String name) throws IgniteCheckedException;

    /**
     * Get affinity.
     *
     * @param name Cache name.
     * @return Affinity.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTargetProxy affinity(@Nullable String name) throws IgniteCheckedException;

    /**
     * Get data streamer.
     *
     * @param cacheName Cache name.
     * @param keepBinary Binary flag.
     * @return Data streamer.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTargetProxy dataStreamer(@Nullable String cacheName, boolean keepBinary) throws IgniteCheckedException;

    /**
     * Get transactions.
     *
     * @return Transactions.
     */
    public PlatformTargetProxy transactions();

    /**
     * Get projection.
     *
     * @return Projection.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTargetProxy projection() throws IgniteCheckedException;

    /**
     * Create interop compute.
     *
     * @param grp Cluster group.
     * @return Compute instance.
     */
    public PlatformTargetProxy compute(PlatformTargetProxy grp);

    /**
     * Create interop messaging.
     *
     * @param grp Cluster group.
     * @return Messaging instance.
     */
    public PlatformTargetProxy message(PlatformTargetProxy grp);

    /**
     * Create interop events.
     *
     * @param grp Cluster group.
     * @return Events instance.
     */
    public PlatformTargetProxy events(PlatformTargetProxy grp);

    /**
     * Create interop services.
     *
     * @param grp Cluster group.
     * @return Services instance.
     */
    public PlatformTargetProxy services(PlatformTargetProxy grp);

    /**
     * Get platform extensions. Override this method to provide any additional targets and operations you need.
     *
     * @return Platform extensions.
     */
    public PlatformTargetProxy extensions();

    /**
     * Register cache store.
     *
     * @param store Store.
     * @param convertBinary Convert binary flag.
     * @throws IgniteCheckedException If failed.
     */
    public void registerStore(PlatformCacheStore store, boolean convertBinary) throws IgniteCheckedException;

    /**
     * Get or create AtomicLong.
     * @param name Name.
     * @param initVal Initial value.
     * @param create Create flag.
     * @return Platform atomic long.
     */
    public PlatformTargetProxy atomicLong(String name, long initVal, boolean create);

    /**
     * Get or create AtomicSequence.
     * @param name Name.
     * @param initVal Initial value.
     * @param create Create flag.
     * @return Platform atomic long.
     */
    public PlatformTargetProxy atomicSequence(String name, long initVal, boolean create);

    /**
     * Get or create AtomicReference.
     * @param name Name.
     * @param memPtr Pointer to a stream with initial value. 0 for null initial value.
     * @param create Create flag.
     * @return Platform atomic long.
     */
    public PlatformTargetProxy atomicReference(String name, long memPtr, boolean create);

    /**
     * Gets the configuration of the current Ignite instance.
     *
     * @param memPtr Stream to write data to.
     */
    public void getIgniteConfiguration(long memPtr);

    /**
     * Gets the cache names.
     *
     * @param memPtr Stream to write data to.
     */
    public void getCacheNames(long memPtr);

    /**
     * Starts a near cache on local node if cache was previously started.
     *
     * @param cacheName Cache name.
     * @param memPtr Pointer to a stream with near cache config. 0 for default config.
     * @return Cache.
     */
    public PlatformTargetProxy createNearCache(@Nullable String cacheName, long memPtr);

    /**
     * Gets existing near cache with the given name or creates a new one.
     *
     * @param cacheName Cache name.
     * @param memPtr Pointer to a stream with near cache config. 0 for default config.
     * @return Cache.
     */
    public PlatformTargetProxy getOrCreateNearCache(@Nullable String cacheName, long memPtr);

    /**
     * Gets a value indicating whether Ignite logger has specified level enabled.
     *
     * @param level Log level.
     */
    public boolean loggerIsLevelEnabled(int level);

    /**
     * Logs to the Ignite logger.
     *
     * @param level Log level.
     * @param message Message.
     * @param category Category.
     * @param errorInfo Error info.
     */
    public void loggerLog(int level, String message, String category, String errorInfo);

    /**
     * Gets the binary processor.
     *
     * @return Binary processor.
     */
    public PlatformTargetProxy binaryProcessor();
}
