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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public class PartitionsEvictionTaskFailureHandlerTest extends PartitionsEvictManagerAbstractTest {
    /**
     *
     */
    @Test
    public void testEvictionTaskShouldCallFailureHandler() throws Exception {
        T2<IgniteEx, CountDownLatch> nodeAndEvictLatch = makeNodeWithEvictLatch(true);

        IgniteCache<Object, Object> cache = nodeAndEvictLatch.get1().createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setGroupName("test-grp"));

        for (int i = 0; i < 100_000; i++)
            cache.put(i, i);

        doActionDuringEviction(nodeAndEvictLatch, () -> {});

        assertTrue(GridTestUtils.waitForCondition(() -> failure.get(), 10_000));
    }
}
