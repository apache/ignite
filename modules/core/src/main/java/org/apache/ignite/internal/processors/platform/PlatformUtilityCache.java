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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 * Platform utility cache.
 */
public class PlatformUtilityCache {
    /** The cache. */
    private GridCacheAdapter<Object, Object> cache;

    /**
     * Called when continuous processor has started.
     *
     * @param ctx Kernal context.
     * @throws IgniteCheckedException If failed.
     */
    public void onContinuousProcessorStarted(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        // No-op.
    }

    /**
     * Called when cache has started.
     *
     * @param ctx Kernal context.
     * @throws IgniteCheckedException In case of error.
     */
    public void onCacheStarted(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        cache = ctx.cache().internalCache(CU.UTILITY_CACHE_NAME_PLATFORM);
    }

    /**
     * Release marshaller context.
     */
    public void onKernalStop() {
        // No-op.
    }
}
