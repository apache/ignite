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

package org.apache.ignite.internal.processors.cache.objects;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.objects.AbstractCacheObjectsCompressionTest.CompressionTransformer.CompressionType;

/**
 * Checks compression algorithm change.
 */
public class CacheObjectsCompressionEvolutionTest extends AbstractCacheObjectsCompressionTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        Ignite ignite = prepareCluster();

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);

        int cnt = 1000;

        for (int i = 0; i < cnt; i++) {
            for (CompressionType type : CompressionType.values()) {
                CompressionTransformer.type = type;

                cache.put(++key, HUGE_STRING + key);
            }
        }

        CompressionTransformer.type = CompressionTransformer.CompressionType.defaultType();

        assertEquals(0, CompressionTransformer.zstdCnt.get());
        assertEquals(0, CompressionTransformer.lz4Cnt.get());
        assertEquals(0, CompressionTransformer.snapCnt.get());

        while (key > 0)
            assertEquals(HUGE_STRING + key, cache.get(key--));

        assertEquals(cnt, CompressionTransformer.zstdCnt.get());
        assertEquals(cnt, CompressionTransformer.lz4Cnt.get());
        assertEquals(cnt, CompressionTransformer.snapCnt.get());
    }
}
