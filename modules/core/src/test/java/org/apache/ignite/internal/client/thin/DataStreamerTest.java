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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientDataStreamer;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Checks data streamer for thin client.
 */
public class DataStreamerTest extends AbstractThinClientTest {
    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);

        grid(0).createCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();
    }

    /**
     * Test data streamer.
     */
    @Test
    public void testDataStreamer() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);

            cache.clear();

            try (ClientDataStreamer<Object, Object> streamer = client.dataStreamer(DEFAULT_CACHE_NAME)) {
                for (int i = 0; i < 10_000; i++)
                    streamer.addData(i, i);

                streamer.flush();
            }

            for (int i = 9_500; i < 10_000; i++)
                assertEquals(i, client.cache(DEFAULT_CACHE_NAME).get(i));
        }
    }

    /**
     * Test data streamer auto flushing by timeout.
     */
    @Test
    public void testAutoFlushFrequency() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);

            cache.clear();

            try (ClientDataStreamer<Integer, Integer> streamer = client.dataStreamer(DEFAULT_CACHE_NAME)) {
                streamer.autoFlushFrequency(100);

                assertFalse(cache.containsKey(0));

                streamer.addData(0, 0);

                assertFalse(cache.containsKey(0));

                assertTrue(GridTestUtils.waitForCondition(() -> F.eq(cache.get(0), 0), 1_000L));
            }
        }
    }
}
