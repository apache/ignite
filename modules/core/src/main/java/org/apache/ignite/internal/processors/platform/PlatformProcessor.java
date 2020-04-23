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
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheManager;
import org.apache.ignite.internal.processors.platform.cache.store.PlatformCacheStore;

/**
 * Platform processor.
 */
@SuppressWarnings({"UnnecessaryInterfaceModifier"})
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
     * Returns a value indicating whether {@link #context()} is available.
     *
     * @return value indicating whether {@link #context()} is available.
     */
    public boolean hasContext();

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
     * Register cache store.
     *
     * @param store Store.
     * @param convertBinary Convert binary flag.
     * @throws IgniteCheckedException If failed.
     */
    public void registerStore(PlatformCacheStore store, boolean convertBinary) throws IgniteCheckedException;

    /**
     * Gets the cache manager.
     *
     * @return Cache manager.
     */
    PlatformCacheManager cacheManager();

    /**
     * Sets thread local value for platform.
     *
     * @param value Value.
     */
    public void setThreadLocal(Object value);
}
