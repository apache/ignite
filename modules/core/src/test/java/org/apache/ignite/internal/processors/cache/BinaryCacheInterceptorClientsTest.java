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

package org.apache.ignite.internal.processors.cache;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BinaryCacheInterceptorClientsTest extends CacheInterceptorClientsAbstractTest {
    /** {@inheritDoc} */
    @Override protected boolean binary() {
        return true;
    }

    @Test
    public void testGet() throws Exception {
        for (String cacheName : CACHE_NAMES) {
            assertNotNull(cacheName, thickCache(cacheName).get(0));
            assertNotNull(cacheName, thinCache(cacheName).get(0));
        }
    }

    @Test
    public void testPut() throws Exception {
        for (String cacheName : CACHE_NAMES) {
            Value v0 = new Value(cacheName + 0);
            Value v1 = new Value(cacheName + 1);

            thickCache(cacheName).put(createBinaryKey(0, cacheName), createBinary(v0));
            thinCache(cacheName).put(createBinaryKey(1, cacheName), createBinary(v1));

            assertEquals(cacheName, v0, fromBinary((BinaryObject)thickCache(cacheName).get(0)));
            assertEquals(cacheName, v1, fromBinary((BinaryObject)thinCache(cacheName).get(1)));
        }
    }

    @Test
    public void testGetAll() throws Exception {
        for (String cacheName : CACHE_NAMES) {
            Set<KeyCacheObject> keys = new HashSet<>();

            keys.add(createBinaryKey(0, cacheName));
            keys.add(createBinaryKey(1, cacheName));
            keys.add(createBinaryKey(2, cacheName));

            assertEquals(cacheName, keys.size(), thickCache(cacheName).getAll(keys).values().size());
            assertEquals(cacheName, keys.size(), thinCache(cacheName).getAll(keys).values().size());
        }
    }

//    @Test
//    public void testPutAll() {
//        Set<Integer> keys = Stream.of(-1, -2, -3).collect(Collectors.toSet());
//
//        for (String cacheName : CACHE_NAMES) {
//            Map<Integer, BinaryObject> vals = keys.stream().collect(Collectors.toMap(k->k, k->new Value(cacheName+k)));
//            assertEquals(cacheName, keys.size(), thickCache(cacheName).getAll(keys).values().size());
//            assertEquals(cacheName, keys.size(), thinCache(cacheName).getAll(keys).values().size());
//        }
//    }

    private BinaryObject createBinary(Value v) {
        return grid(SERVER_NODE_NAME).context().cacheObjects().binary().toBinary(v);
    }

    private KeyCacheObject createBinaryKey(Integer i, String cacheName) throws IgniteCheckedException {
        GridKernalContext ctx = grid(SERVER_NODE_NAME).context();

        CacheConfiguration cfg = ctx.cache().cacheConfiguration(cacheName);

        return ctx.cacheObjects().toCacheKeyObject(ctx.cacheObjects().contextForCache(cfg), null, i, true);
    }

    private Value fromBinary(BinaryObject o) {
        return o.deserialize();
    }

    private Integer fromBinaryKey(KeyCacheObjectImpl k, String cacheName) throws IgniteCheckedException {
        CacheConfiguration cfg = grid(SERVER_NODE_NAME).context().cache().cacheConfiguration(cacheName);

        return k.value(grid(SERVER_NODE_NAME).context().cacheObjects().contextForCache(cfg), false);
    }
}
