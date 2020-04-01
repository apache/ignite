/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheManager;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Platform cache manager - delegates functionality to native platforms (.NET, C++, ...).
 */
@SuppressWarnings("rawtypes")
public class PlatformCacheManager implements GridCacheManager {
    /** */
    private final PlatformCallbackGateway gate;

    /** */
    private volatile GridCacheContext cctx;

    /**
     * Constructor.
     *
     * @param gate Platform gateway..
     */
    public PlatformCacheManager(PlatformCallbackGateway gate) {
        assert gate != null;

        this.gate = gate;
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheContext cctx) throws IgniteCheckedException {
        assert cctx != null;
        assert this.cctx == null;

        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel, boolean destroy) {
        GridCacheContext ctx = cctx;

        if (ctx != null) {
            gate.onCacheStopped(cctx.cacheId());
            cctx = null;
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        // No-op.
        // Disconnect is handled from PlatformProcessor.onDisconnected.
    }
}
