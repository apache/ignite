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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.MarshallerCacheListener;
import org.apache.ignite.internal.MarshallerWorkDirectory;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheTryPutFailedException;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.File;
import java.util.concurrent.CountDownLatch;

/**
 * Platform marshaller context.
 */
public class PlatformMarshallerContext {
    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** */
    private final File workDir;

    /** */
    private static final String cacheName = CU.UTILITY_CACHE_NAME_PLATFORM;

    /** */
    private final byte keyPrefix;

    /** */
    private IgniteLogger log;

    /** */
    private volatile GridCacheAdapter<Object, String> cache;

    /** Non-volatile on purpose. */
    private int failedCnt;

    /** */
    private MarshallerCacheListener lsnr = new MarshallerCacheListener();

    /**
     * @param keyPrefix Composite key prefix.
     * @throws IgniteCheckedException In case of error.
     */
    public PlatformMarshallerContext(byte keyPrefix)
        throws IgniteCheckedException {

        workDir = U.resolveWorkDirectory("marshaller", false);

        this.keyPrefix = keyPrefix;
    }

    /**
     * Called when continuous processor has started.
     *
     * @param ctx Kernal context.
     * @throws IgniteCheckedException If failed.
     */
    public void onContinuousProcessorStarted(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        lsnr.onContinuousProcessorStarted(ctx, cacheName, workDir);
    }

    /**
     * Called when marshaller cache has started.
     *
     * @param ctx Kernal context.
     * @throws IgniteCheckedException In case of error.
     */
    public void onMarshallerCacheStarted(GridKernalContext ctx) throws IgniteCheckedException {
        assert ctx != null;

        log = ctx.log(PlatformMarshallerContext.class);

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

    /**
     * Registers the type name.
     *
     * @param id Type id.
     * @param typName Type name.
     * @return True on success.
     * @throws IgniteCheckedException On collision.
     */
    public boolean registerTypeName(int id, String typName) throws IgniteCheckedException {
        assert typName != null;

        GridCacheAdapter<Object, String> cache0 = cache;

        if (cache0 == null)
            return false;

        Object key = getKey(id);

        try {
            String old = cache0.tryGetAndPut(key, typName);

            if (old != null && !old.equals(typName))
                throw new IgniteCheckedException("Type ID collision detected [id=" + key + ", clsName1=" + typName +
                    ", clsName2=" + old + ']');

            failedCnt = 0;

            return true;
        }
        catch (CachePartialUpdateCheckedException | GridCacheTryPutFailedException ignored) {
            if (++failedCnt > 10) {
                if (log.isQuiet())
                    U.quiet(false, "Failed to register platform marshalled type for more than 10 times in a row " +
                        "(may affect performance).");

                failedCnt = 0;
            }

            return false;
        }
    }

    /**
     * Gets the type name.
     *
     * @param id Type id.
     * @return Type name.
     * @throws IgniteCheckedException
     */
    public String getTypeName(int id) throws IgniteCheckedException {
        GridCacheAdapter<Object, String> cache0 = cache;

        if (cache0 == null) {
            U.awaitQuiet(latch);

            cache0 = cache;

            if (cache0 == null)
                throw new IllegalStateException("Failed to initialize platform marshaller context (grid is stopping).");
        }

        Object key = getKey(id);

        String typName = cache0.getTopologySafe(key);

        if (typName == null) {
            typName = MarshallerWorkDirectory.getTypeNameFromFile(key, workDir);

            // Must explicitly put entry to cache to invoke other continuous queries.
            registerTypeName(id, typName);
        }

        return typName;
    }

    /**
     * Gets the cache key for a type id.
     *
     * @param id Id.
     * @return Cache key depending on keyPrefix.
     */
    private Object getKey(int id) {
        return new PlatformUtilityCacheKey(keyPrefix, id);
    }
}
