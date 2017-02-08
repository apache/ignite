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

package org.apache.ignite.internal.processors.platform.plugin.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;

/**
 * Platform cache plugin provider.
 */
class PlatformCachePluginProvider implements CachePluginProvider {
    /** Context. */
    private final CachePluginContext ctx;

    /** Native config. */
    private final Object nativeCfg;

    /** Pointer to native plugin. */
    protected long ptr;

    /**
     * Ctor.
     *
     * @param ctx Context.
     */
    PlatformCachePluginProvider(CachePluginContext ctx, Object nativeCfg) {
        assert ctx != null;
        assert nativeCfg != null;

        this.ctx = ctx;
        this.nativeCfg = nativeCfg;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        PlatformContext platformCtx = PlatformUtils.platformContext(ctx.grid());

        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = platformCtx.writer(out);

            writer.writeObjectDetached(nativeCfg);

            PlatformConfigurationUtils.writeIgniteConfiguration(writer, ctx.igniteConfiguration());
            PlatformConfigurationUtils.writeCacheConfiguration(writer, ctx.igniteCacheConfiguration());

            out.synchronize();

            ptr = platformCtx.gateway().cachePluginCreate(mem.pointer());
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        PlatformContext platformCtx = PlatformUtils.platformContext(ctx.grid());

        platformCtx.gateway().cachePluginDestroy(ptr, cancel);
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() throws IgniteCheckedException {
        PlatformContext platformCtx = PlatformUtils.platformContext(ctx.grid());

        platformCtx.gateway().cachePluginIgniteStart(ptr);
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        PlatformContext platformCtx = PlatformUtils.platformContext(ctx.grid());

        platformCtx.gateway().cachePluginIgniteStop(ptr, cancel);
    }

    /** {@inheritDoc} */
    @Override public void validate() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void validateRemote(CacheConfiguration locCfg, CachePluginConfiguration locPluginCcfg,
        CacheConfiguration rmtCfg, ClusterNode rmtNode) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object unwrapCacheEntry(Cache.Entry entry, Class cls) {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object createComponent(Class cls) {
        return null;
    }
}
