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

/**
 * Platform utility cache.
 */
public class PlatformUtilityCache {
    /** */
    private final PlatformMarshallerContext marshCtx;

    /**
     * Ctor.
     *
     * @param marshCacheKeyPrefix Marshaller cache key prefix.
     * @throws IgniteCheckedException On error.
     */
    public PlatformUtilityCache(Byte marshCacheKeyPrefix) throws IgniteCheckedException {
        marshCtx = marshCacheKeyPrefix != null ? new PlatformMarshallerContext(marshCacheKeyPrefix) : null;
    }

    /**
     * Gets the platform marshaller context.
     */
    public PlatformMarshallerContext getMarshallerContext() {
        return marshCtx;
    }

    /**
     * Called when continuous processor has started.
     *
     * @param ctx Kernal context.
     * @throws IgniteCheckedException If failed.
     */
    public void onContinuousProcessorStarted(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        if (marshCtx != null)
            marshCtx.onContinuousProcessorStarted(ctx);
    }

    /**
     * Called when marshaller cache has started.
     *
     * @param ctx Kernal context.
     * @throws IgniteCheckedException In case of error.
     */
    public void onMarshallerCacheStarted(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        if (marshCtx != null)
            marshCtx.onMarshallerCacheStarted(ctx);
    }

    /**
     * Release marshaller context.
     */
    public void onKernalStop() {
        if (marshCtx != null)
            marshCtx.onKernalStop();
    }
}
