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
import java.io.StringWriter;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Assert;

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

    /** Next nodes after MAX_CACHE_COUNT start without cache */
    private static final int MAX_CACHE_COUNT = 4;

    /** Started grid counter. */
    private static int cnt;

    /** No nodes filter. */
    private static volatile boolean noNodesFilter;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        if (cnt < MAX_CACHE_COUNT)
            cfg.setCacheConfiguration(cacheConfiguration());

        cnt++;

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullPointerExceptionUponDataStreamerClosing() throws Exception {
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
        catch (CacheException | IllegalStateException ignore) {
            // This is ok to ignore this exception as test is racy by it's nature -
            // grid is stopping in different thread.
        }
    }

    /**
     * Data streamer should correctly load entries from HashMap in case of grids with more than one node
     * and with GridOptimizedMarshaller that requires serializable.
     *
     * @throws Exception If failed.
     */
    public void testAddDataFromMap() throws Exception {
        cnt = 0;

        startGrids(2);

        Ignite g0 = grid(0);

        IgniteDataStreamer<Integer, String> dataLdr = g0.dataStreamer(null);

        Map<Integer, String> map = U.newHashMap(KEYS_COUNT);

        for (int i = 0; i < KEYS_COUNT; i++)
            map.put(i, String.valueOf(i));

        dataLdr.addData(map);

        dataLdr.close();

        Random rnd = new Random();

        IgniteCache<Integer, String> c = g0.cache(null);

        for (int i = 0; i < KEYS_COUNT; i++) {
            Integer k = rnd.nextInt(KEYS_COUNT);

            String v = c.get(k);

            assertEquals(k.toString(), v);
        }
    }

    /**
     * Test logging on {@code DataStreamer.addData()} method when cache have no data nodes
     *
     * @throws Exception If fail.
     */
    public void testNoDataNodesOnClose() throws Exception {
        boolean failed = false;

        cnt = 0;

        noNodesFilter = true;

        try {
            Ignite ignite = startGrid(1);

            try (IgniteDataStreamer<Integer, String> streamer = ignite.dataStreamer(null)) {
                streamer.addData(1, "1");
            }
            catch (CacheException ex) {
                failed = true;
            }
        }
        finally {
            noNodesFilter = false;

            assertTrue(failed);
        }
    }

    /**
     * Test logging on {@code DataStreamer.addData()} method when cache have no data nodes
     *
     * @throws Exception If fail.
     */
    public void testNoDataNodesOnFlush() throws Exception {
        boolean failed = false;

        cnt = 0;

        noNodesFilter = true;

        try {
            Ignite ignite = startGrid(1);

            IgniteFuture fut = null;

            try (IgniteDataStreamer<Integer, String> streamer = ignite.dataStreamer(null)) {
                fut = streamer.addData(1, "1");

                streamer.flush();
            }
            catch (IllegalStateException ex) {
                try {
                    fut.get();

                    fail("DataStreamer ignores failed streaming.");
                }
                catch (CacheServerNotFoundException ignored) {
                    // No-op.
                }

                failed = true;
            }
        }
        finally {
            noNodesFilter = false;

            assertTrue(failed);
        }
    }

    /**
     * Cluster topology mismatch shall result in DataStreamer retrying cache update with the latest topology and
     * no error logged to the console.
     *
     * @throws Exception if failed
     */
    public void testRetryWhenTopologyMismatch() throws Exception {
        final int KEY = 1;
        final String VAL = "1";
        final AtomicReference<Throwable> testFailure = new AtomicReference<>(null);
        final AtomicLong firstReqId = new AtomicLong(Long.MIN_VALUE);

        StringWriter logWriter = new StringWriter();
        Logger.getRootLogger().addAppender(new WriterAppender(new SimpleLayout(), logWriter));

        startGrids(MAX_CACHE_COUNT - 1); // cache-enabled nodes

        try (Ignite ignite = startGrid(MAX_CACHE_COUNT);
             IgniteDataStreamer<Integer, String> streamer = ignite.dataStreamer(null)) {
            final GridCacheContext gridCacheCtx = ((IgniteKernal)ignite).internalCache().context();
            final AffinityTopologyVersion validTop = gridCacheCtx.topology().topologyVersion();

            new MockUp<DataStreamerRequest>() {
                @Mock AffinityTopologyVersion topologyVersion(Invocation invocation) {
                    try {
                        DataStreamerRequest req = invocation.getInvokedInstance();

                        assertTrue(req.entries().size() > 0);

                        // Simulate situation when this client node have not received the last "node joined" topology
                        // update causing topology mismatch
                        AffinityTopologyVersion staleTop = new AffinityTopologyVersion(
                            validTop.topologyVersion() - 1,
                            validTop.minorTopologyVersion());

                        // Analyze request ID: we expect DataStreamer to retry updating cache and look for the request
                        // ID increment. Thus, we return stale topology on the first request making the server fail
                        // and then return valid topology on retry.
                        if (firstReqId.get() == Long.MIN_VALUE)
                            firstReqId.set(req.requestId());

                        return firstReqId.get() != req.requestId() ? validTop : staleTop;
                    }
                    catch (Throwable e) {
                        testFailure.set(e);
                        return validTop;
                    }
                }
            };

            streamer.addData(KEY, VAL);
        }

        if (testFailure.get() != null)
            Assert.fail(testFailure.get().getMessage());

        assertFalse(logWriter.toString().contains("DataStreamer will retry data transfer at stable topology"));
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

        if (noNodesFilter)
            cacheCfg.setNodeFilter(F.alwaysFalse());

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