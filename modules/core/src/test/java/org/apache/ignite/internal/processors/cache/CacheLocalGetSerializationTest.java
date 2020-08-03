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

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CacheLocalGetSerializationTest extends GridCommonAbstractTest {
    /**
     * Check correct Map.Entry serialization key on local get/put operation.
     * https://issues.apache.org/jira/browse/IGNITE-10290
     *
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        Ignite ig = startGrid();

        IgniteCache<Map.Entry<Integer, Integer>, Long> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        // Try use Map.Entry as key in destributed map.
        Map.Entry<Integer, Integer> key = new Map.Entry<Integer, Integer>() {
            @Override public Integer getKey() {
                return 123;
            }

            @Override public Integer getValue() {
                return 123;
            }

            @Override public Integer setValue(Integer value) {
                throw new UnsupportedOperationException();
            }
        };

        cache.put(key, Long.MAX_VALUE);

        Long val = cache.get(key);

        Assert.assertNotNull(val);
        Assert.assertEquals(Long.MAX_VALUE, (long)val);
    }
}
