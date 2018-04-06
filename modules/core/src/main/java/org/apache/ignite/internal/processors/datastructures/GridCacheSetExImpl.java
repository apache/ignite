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

package org.apache.ignite.internal.processors.datastructures;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.processors.cache.CacheIteratorConverter;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Cache set implementation that is using exclusive cache for each instance.
 */
public class GridCacheSetExImpl<T> extends GridCacheSetImpl<T> {
    /**
     * @param ctx Cache context.
     * @param name Set name.
     * @param hdr Set header.
     */
    public GridCacheSetExImpl(GridCacheContext ctx, String name, GridCacheSetHeader hdr) {
        super(ctx, name, hdr);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return new IgniteFutureImpl<>(cache.sizeAsync(new CachePeekMode[] {CachePeekMode.PRIMARY})).get();
    }

    /**
     * Get iterator for non-collocated igniteSet.
     *
     * @return iterator for non-collocated igniteSet.
     */
    @SuppressWarnings("unchecked")
    @Override protected GridCloseableIterator<T> closeableIterator() {
        GridCacheContext ctx0 = ctx.isNear() ? ctx.near().dht().context() : ctx;

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        try {
            GridCloseableIterator<Map.Entry<T, Object>> iter = ctx0.queries()
                .createScanQuery(null, null, true)
                .keepAll(false)
                .executeScanQuery();

            return ctx.itHolder().iterator(iter, new CacheIteratorConverter<T, Map.Entry<T, Object>>() {
                @Override protected T convert(Map.Entry<T, Object> e) {
                    // Actually Scan Query returns Iterator<CacheQueryEntry> by default,
                    // CacheQueryEntry implements both Map.Entry and Cache.Entry interfaces.
                    return (T)((SetItemKey)e.getKey()).item();
                }

                @Override protected void remove(T item) {
                    CacheOperationContext prev = ctx.gate().enter(opCtx);

                    try {
                        cache.remove(itemKey(item));
                    }
                    catch (IgniteCheckedException e) {
                        throw U.convertException(e);
                    }
                    finally {
                        ctx.gate().leave(prev);
                    }
                }
            });
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected SetItemKey itemKey(Object item) {
        return new GridCacheSetItemKey(null, item);
    }
}
