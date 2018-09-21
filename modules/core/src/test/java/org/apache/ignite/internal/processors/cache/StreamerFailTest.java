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

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class StreamerFailTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 3;

    /**
     *
     */
    public void testDataStreamer() throws Exception {
        startGridsMultiThreaded(SRVS);

        grid(0).getOrCreateCache(new CacheConfiguration<>().setName(DEFAULT_CACHE_NAME).setCacheMode(REPLICATED));

        doSleep(1_000L);

        grid(0).context().cache().cache(DEFAULT_CACHE_NAME).context().group().readOnly(true);
        grid(1).context().cache().cache(DEFAULT_CACHE_NAME).context().group().readOnly(true);
        grid(2).context().cache().cache(DEFAULT_CACHE_NAME).context().group().readOnly(true);

        try (IgniteDataStreamer<Integer, Integer> streamer = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.addData(0, 0);
        }
    }
}
