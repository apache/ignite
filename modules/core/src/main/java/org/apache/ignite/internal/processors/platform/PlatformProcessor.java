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
@SuppressWarnings("UnusedDeclaration")
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
    public PlatformTarget cache(@Nullable String name) throws IgniteCheckedException;

    /**
     * Create cache.
     *
     * @param name Cache name.
     * @return Cache.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTarget createCache(@Nullable String name) throws IgniteCheckedException;

    /**
     * Get or create cache.
     *
     * @param name Cache name.
     * @return Cache.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTarget getOrCreateCache(@Nullable String name) throws IgniteCheckedException;

    /**
     * Get affinity.
     *
     * @param name Cache name.
     * @return Affinity.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTarget affinity(@Nullable String name) throws IgniteCheckedException;

    /**
     * Get data streamer.
     *
     * @param cacheName Cache name.
     * @param keepPortable Portable flag.
     * @return Data streamer.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTarget dataStreamer(@Nullable String cacheName, boolean keepPortable) throws IgniteCheckedException;

    /**
     * Get transactions.
     *
     * @return Transactions.
     */
    public PlatformTarget transactions();

    /**
     * Get projection.
     *
     * @return Projection.
     * @throws IgniteCheckedException If failed.
     */
    public PlatformTarget projection() throws IgniteCheckedException;

    /**
     * Create interop compute.
     *
     * @param grp Cluster group.
     * @return Compute instance.
     */
    public PlatformTarget compute(PlatformTarget grp);

    /**
     * Create interop messaging.
     *
     * @param grp Cluster group.
     * @return Messaging instance.
     */
    public PlatformTarget message(PlatformTarget grp);

    /**
     * Create interop events.
     *
     * @param grp Cluster group.
     * @return Events instance.
     */
    public PlatformTarget events(PlatformTarget grp);

    /**
     * Create interop services.
     *
     * @param grp Cluster group.
     * @return Services instance.
     */
    public PlatformTarget services(PlatformTarget grp);

    /**
     * Get platform extensions. Override this method to provide any additional targets and operations you need.
     *
     * @return Platform extensions.
     */
    public PlatformTarget extensions();

    /**
     * Register cache store.
     *
     * @param store Store.
     * @param convertPortable Convert portable flag.
     * @throws IgniteCheckedException If failed.
     */
    public void registerStore(PlatformCacheStore store, boolean convertPortable) throws IgniteCheckedException;
}
