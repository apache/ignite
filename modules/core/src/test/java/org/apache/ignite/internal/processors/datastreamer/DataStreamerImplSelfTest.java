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

package org.apache.ignite.internal.processors.datastreamer;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for {@code IgniteDataStreamerImpl}.
 */
public class DataStreamerImplSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of keys to load via data streamer. */
    private static final int KEYS_COUNT = 1000;

    /** Started grid counter. */
    private static int cnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        // Forth node goes without cache.
        if (cnt < 4)
            cfg.setCacheConfiguration(cacheConfiguration());

        cnt++;

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullPointerExceptionUponDataStreamerClosing() throws Exception {
        try {
            startGrids(5);

            final CyclicBarrier barrier = new CyclicBarrier(2);

            multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    U.awaitQuiet(barrier);

                    G.stopAll(true);

                    return null;
                }
            }, 1);

            Ignite g4 = grid(4);

            IgniteDataStreamer<Object, Object> dataLdr = g4.dataStreamer(null);

            dataLdr.perNodeBufferSize(32);

            for (int i = 0; i < 100000; i += 2) {
                dataLdr.addData(i, i);
                dataLdr.removeData(i + 1);
            }

            U.awaitQuiet(barrier);

            info("Closing data streamer.");

            try {
                dataLdr.close(true);
            }
            catch (IllegalStateException ignore) {
                // This is ok to ignore this exception as test is racy by it's nature -
                // grid is stopping in different thread.
            }
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * Data streamer should correctly load entries from HashMap in case of grids with more than one node
     *  and with GridOptimizedMarshaller that requires serializable.
     *
     * @throws Exception If failed.
     */
    public void testAddDataFromMap() throws Exception {
        try {
            cnt = 0;

            startGrids(2);

            Ignite g0 = grid(0);

            IgniteDataStreamer<Integer, String> dataLdr = g0.dataStreamer(null);

            Map<Integer, String> map = U.newHashMap(KEYS_COUNT);

            for (int i = 0; i < KEYS_COUNT; i ++)
                map.put(i, String.valueOf(i));

            dataLdr.addData(map);

            dataLdr.close();

            Random rnd = new Random();

            IgniteCache<Integer, String> c = g0.cache(null);

            for (int i = 0; i < KEYS_COUNT; i ++) {
                Integer k = rnd.nextInt(KEYS_COUNT);

                String v = c.get(k);

                assertEquals(k.toString(), v);
            }
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * Gets cache configuration.
     *
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        return cacheCfg;
    }

    /**
     *
     */
    private static class TestObject implements Serializable {
        /** */
        private int val;

        /**
         */
        private TestObject() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        private TestObject(int val) {
            this.val = val;
        }

        public Integer val() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof TestObject && ((TestObject)obj).val == val;
        }
    }
}