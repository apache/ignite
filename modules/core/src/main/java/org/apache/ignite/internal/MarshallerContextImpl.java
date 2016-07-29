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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheTryPutFailedException;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.PluginProvider;

import java.io.File;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Marshaller context implementation.
 */
public class MarshallerContextImpl extends MarshallerContextAdapter {
    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** */
    private final File workDir;

    /** */
    private static final String cacheName = CU.MARSH_CACHE_NAME;

    /** */
    private IgniteLogger log;

    /** */
    private volatile GridCacheAdapter<Object, String> cache;

    /** Non-volatile on purpose. */
    private int failedCnt;

    /** */
    private MarshallerCacheListener lsnr = new MarshallerCacheListener();

    /**
     * @param plugins Plugins.
     * @throws IgniteCheckedException In case of error.
     */
    public MarshallerContextImpl(List<PluginProvider> plugins)
        throws IgniteCheckedException {
        super(plugins);

        workDir = U.resolveWorkDirectory("marshaller", false);
    }
    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void onContinuousProcessorStarted(GridKernalContext ctx) throws IgniteCheckedException {
        lsnr.onContinuousProcessorStarted(ctx, cacheName, workDir);
    }

    /**
     * @param ctx Kernal context.
     * @throws IgniteCheckedException In case of error.
     */
    public void onMarshallerCacheStarted(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        log = ctx.log(MarshallerContextImpl.class);

        cache = ctx.cache().internalCache(cacheName);

        lsnr.onMarshallerCacheStarted(ctx, cache, log, workDir);

        latch.countDown();
    }

    /**
     * Release marshaller context.
     */
    public void onKernalStop() {
        latch.countDown();
    }

    /** {@inheritDoc} */
    @Override public boolean registerClassName(int id, String clsName) throws IgniteCheckedException {
        GridCacheAdapter<Object, String> cache0 = cache;

        if (cache0 == null)
            return false;

        try {
            String old = cache0.tryGetAndPut(id, clsName);

            if (old != null && !old.equals(clsName))
                throw new IgniteCheckedException("Type ID collision detected [id=" + id + ", clsName1=" + clsName +
                    ", clsName2=" + old + ']');

            failedCnt = 0;

            return true;
        }
        catch (CachePartialUpdateCheckedException | GridCacheTryPutFailedException ignored) {
            if (++failedCnt > 10) {
                if (log.isQuiet())
                    U.quiet(false, "Failed to register marshalled class for more than 10 times in a row " +
                        "(may affect performance).");

                failedCnt = 0;
            }

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public String className(int id) throws IgniteCheckedException {
        GridCacheAdapter<Object, String> cache0 = cache;

        if (cache0 == null) {
            U.awaitQuiet(latch);

            cache0 = cache;

            if (cache0 == null)
                throw new IllegalStateException("Failed to initialize marshaller context (grid is stopping).");
        }

        String clsName = cache0.getTopologySafe(id);

        if (clsName == null) {
            clsName = MarshallerWorkDirectory.getTypeNameFromFile(id, workDir);

            // Must explicitly put entry to cache to invoke other continuous queries.
            registerClassName(id, clsName);
        }

        return clsName;
    }
}
