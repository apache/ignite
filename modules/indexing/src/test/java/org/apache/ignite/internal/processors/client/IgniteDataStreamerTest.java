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

package org.apache.ignite.internal.processors.client;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CachePeekMode.ALL;

/**
 */
public class IgniteDataStreamerTest extends GridCommonAbstractTest {
    public static final String CACHE_NAME = "UUID_CACHE";

    public static final int DATA_SIZE = 3;

    public static final long WAIT_TIMEOUT = 30_000L;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
        startClientGrid("client");
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        grid("client").destroyCache(CACHE_NAME);
    }

    /**
     * @return Cache configuration
     */
    private <K, V> CacheConfiguration<K, V> cacheConfiguration(Class<K> key, Class<V> value) {
        CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setIndexedTypes(key, value);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStreamerIgniteUuid() throws Exception {
        Ignite client = grid("client");

        IgniteCache<IgniteUuid, Integer> cache =
            client.createCache(cacheConfiguration(IgniteUuid.class, Integer.class));

        try (IgniteDataStreamer<IgniteUuid, Integer> streamer = client.dataStreamer(CACHE_NAME)) {
            assertTrue("Expecting " + DataStreamerImpl.class.getName(), streamer instanceof DataStreamerImpl);

            ((DataStreamerImpl<IgniteUuid, Integer>)streamer).maxRemapCount(0);

            List<IgniteFuture> futs = new ArrayList<>();

            for (int i = 0; i < DATA_SIZE; i++) {
                IgniteFuture<?> fut = streamer.addData(IgniteUuid.randomUuid(), i);

                futs.add(fut);
            }

            streamer.flush();

            for (IgniteFuture fut : futs) {
                //This should not throw any exception.
                Object res = fut.get(WAIT_TIMEOUT);

                if (log.isDebugEnabled()) {
                    //Printing future result to log to prevent jvm optimization
                    log.debug(String.valueOf(res));
                }
            }

            assertTrue(cache.size(ALL) == DATA_SIZE);
        }
    }
}
