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

package org.apache.ignite.internal.processors.cache.query;

import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.plugin.extensions.communication.MessageMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Verifies that a query result row key arrives resolved from {@code GridCacheQueryResponse} unmarshalling: a
 * {@code KeyCacheObject} travels bytes-only and forbids lazy resolution (see {@code @MarshalledObjects}).
 */
public class GridCacheQueryResponseUnmarshalTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testRowKeyResolved() throws Exception {
        startGrid(0);

        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        GridCacheContext<?, ?> cctx = grid(0).cachex(DEFAULT_CACHE_NAME).context();
        GridKernalContext kctx = grid(0).context();

        KeyCacheObject key = cctx.toCacheKeyObject(42);

        key.marshal(cctx.cacheObjectContext());

        GridCacheQueryResponse res = new GridCacheQueryResponse(cctx.cacheId(), 0, true, false);

        res.data(List.of(new T2<>(key, "row")));

        MessageMarshaller.marshal(kctx.messageFactory(), res, kctx, null);

        GridCacheQueryResponse rcvd = new GridCacheQueryResponse(cctx.cacheId(), 0, true, false);

        rcvd.dataBytes = res.dataBytes;

        MessageMarshaller.unmarshal(kctx.messageFactory(), rcvd, kctx, null, getClass().getClassLoader());

        Map.Entry<?, ?> row = (Map.Entry<?, ?>)rcvd.data().iterator().next();

        // Throws CacheObjectNotResolvedException on an unresolved key.
        assertEquals(key.hashCode(), row.getKey().hashCode());
    }
}
