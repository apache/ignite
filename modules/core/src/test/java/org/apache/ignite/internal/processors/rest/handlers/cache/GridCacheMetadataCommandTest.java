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

package org.apache.ignite.internal.processors.rest.handlers.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Cache metadata command tests.
 */
public class GridCacheMetadataCommandTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
        cfg.setManagementThreadPoolSize(2);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(false);
    }

    /**
     * <p>Test for requesting the cache's metadata from multiple threads
     * in order to detect starvation or deadlock in the mngmt pool caused by calling other internal tasks within the
     * metadata task.</p>
     *
     * <p>Steps to reproduce:</p>
     * <ul>
     *  <li>Start a few server nodes with the small size of the mngmt pool.</li>
     *  <li>Call the metadata task by requesting REST API from multiple threads.</li>
     *  <li>Check all requests have finished successfully.</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void cacheMetadataWithMultupleThreads() throws Exception {
        int servers = 2;
        int iterations = 1000;
        int threads = 10;

        startGrids(servers);

        ExecutorService ex = Executors.newFixedThreadPool(threads);

        try {
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < threads; i++) {
                futures.add(ex.submit(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        GridRestCommandHandler hnd = new GridCacheCommandHandler((grid(0)).context());

                        GridRestCacheRequest req = new GridRestCacheRequest();

                        req.command(GridRestCommand.CACHE_METADATA);
                        req.cacheName(DEFAULT_CACHE_NAME);

                        for (int i = 0; i < iterations; i++) {
                            GridRestResponse resp = hnd.handleAsync(req).get();

                            assertEquals(GridRestResponse.STATUS_SUCCESS, resp.getSuccessStatus());
                        }

                        return null;
                    }
                }));
            }

            for (Future<?> f : futures)
                f.get(1, TimeUnit.MINUTES);
        }
        finally {
            ex.shutdownNow();
        }
    }
}
