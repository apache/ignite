/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.datastreamer;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class DataStreamerMultinodeCreateCacheTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setSocketTimeout(50);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setAckTimeout(50);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreateCacheAndStream() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-1603");

        final int THREADS = 5;

        startGrids(THREADS);

        final AtomicInteger idx = new AtomicInteger();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int threadIdx = idx.getAndIncrement();

                long stopTime = System.currentTimeMillis() + 60_000;

                Ignite ignite = grid(threadIdx);

                int iter = 0;

                while (System.currentTimeMillis() < stopTime) {
                    String cacheName = "cache-" + threadIdx + "-" + (iter % 10);

                    IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheName);

                    try (IgniteDataStreamer<Object, Object> stmr = ignite.dataStreamer(cacheName)) {
                        ((DataStreamerImpl<Object, Object>)stmr).maxRemapCount(0);

                        for (int i = 0; i < 1000; i++)
                            stmr.addData(i, i);
                    }

                    cache.destroy();

                    iter++;
                }

                return null;
            }
        }, THREADS, "create-cache");

        fut.get(2 * 60_000);
    }
}
