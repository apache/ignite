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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.T2;
import org.junit.Test;

/**
 * Checks that destroy cache operations wait for an eviction finish.
 */
public class DropCacheContextDuringEvictionTest extends PartitionsEvictManagerAbstractTest {
    /**
     *
     */
    @Test
    public void testDeactivation() throws Exception {
        T2<IgniteEx, CountDownLatch> nodeAndEvictLatch = makeNodeWithEvictLatch();

        IgniteCache<Object, Object> cache = nodeAndEvictLatch.get1().createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setGroupName("test-grp"));

        for (int i = 0; i < 100_000; i++)
            cache.put(i, i);

        doActionDuringEviction(nodeAndEvictLatch, () -> nodeAndEvictLatch.get1().cluster().active(false));

        assertFalse(failure.get());
    }

    /**
     *
     */
    @Test
    public void testDestroyCacheGroup() throws Exception {
        T2<IgniteEx, CountDownLatch> nodeAndEvictLatch = makeNodeWithEvictLatch();

        List<String> caches = new ArrayList<>();

        for (int idx = 0; idx < 10; idx++) {
            IgniteCache<Object, Object> cache = nodeAndEvictLatch.get1().createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME + idx)
                .setGroupName("test-grp"));

            caches.add(cache.getName());

            try (IgniteDataStreamer streamer = nodeAndEvictLatch.get1().dataStreamer(cache.getName())) {
                streamer.allowOverwrite(true);

                for (int i = 0; i < 200_000; i++)
                    streamer.addData(i, i);
            }
        }

        doActionDuringEviction(nodeAndEvictLatch, () -> nodeAndEvictLatch.get1().destroyCaches(caches));

        assertFalse(failure.get());
    }

    /**
     *
     */
    @Test
    public void testDestroyCache() throws Exception {
        T2<IgniteEx, CountDownLatch> nodeAndEvictLatch = makeNodeWithEvictLatch();

        IgniteCache<Object, Object> cache = nodeAndEvictLatch.get1().createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 500))
        );

        try (IgniteDataStreamer streamer = nodeAndEvictLatch.get1().dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 2_000_000; i++)
                streamer.addData(i, i);
        }

        doActionDuringEviction(nodeAndEvictLatch, cache::destroy);

        assertFalse(failure.get());
    }
}
