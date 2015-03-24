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
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.concurrent.*;

/**
 * Marshaller context implementation.
 */
public class MarshallerContextImpl extends MarshallerContextAdapter {
    /** Class names cache update retries count. */
    private static final int CACHE_UPDATE_RETRIES_CNT = 5;

    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** */
    private IgniteLogger log;

    /** */
    private volatile GridCacheAdapter<Integer, String> cache;

    /**
     * @param ctx Kernal context.
     */
    public void onMarshallerCacheReady(GridKernalContext ctx) {
        assert ctx != null;

        log = ctx.log(MarshallerContextImpl.class);

        cache = ctx.cache().marshallerCache();

        latch.countDown();
    }

    /** {@inheritDoc} */
    @Override protected boolean registerClassName(int id, String clsName) {
        try {
            GridCacheAdapter<Integer, String> cache0 = cache;

            if (cache0 == null)
                return false;

            for (int i = 0; i < CACHE_UPDATE_RETRIES_CNT; i++) {
                try {
                    String old;

                    try {
                        old = cache0.tryPutIfAbsent(id, clsName);
                    }
                    catch (GridCacheTryPutFailedException ignored) {
                        return false;
                    }

                    if (old != null && !old.equals(clsName))
                        throw new IgniteException("Type ID collision occurred in OptimizedMarshaller. Use " +
                            "OptimizedMarshallerIdMapper to resolve it [id=" + id + ", clsName1=" + clsName +
                            "clsName2=" + old + ']');

                    break;
                }
                catch (IgniteCheckedException e) {
                    if (i == CACHE_UPDATE_RETRIES_CNT - 1)
                        throw e;
                    else
                        U.error(log, "Failed to update marshaller cache, will retry: " + e);
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected String className(int id) {
        try {
            if (cache == null)
                U.awaitQuiet(latch);

            return cache.get(id);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }
}
