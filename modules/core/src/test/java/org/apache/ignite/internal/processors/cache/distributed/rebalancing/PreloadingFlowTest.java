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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;

/**
 * Measures time required for preloading data.
 */
public class PreloadingFlowTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_SIZE = 2_000_000;

    /** */
    private static final int ITERATIONS = 10;

    /** */
    private CountDownLatch preloadStartSync;

    /** */
    public CacheAtomicityMode cacheAtomicityMode;

    /** */
    public boolean persistenceEnabled;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TimeUnit.MINUTES.toMillis(30);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(persistenceEnabled)));

        cfg.setRebalanceThreadPoolSize(4);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(cacheAtomicityMode)
            .setRebalanceBatchesPrefetchCount(8));

        boolean supplyNode = getTestIgniteInstanceIndex(igniteInstanceName) == 0;

        cfg.setCommunicationSpi(new TestCommunicationSpi(supplyNode ? preloadStartSync = new CountDownLatch(1) : null));

        return cfg;
    }

    /**
     *
     */
    @Before
    public void setup() throws Exception {
        cleanPersistenceDir();
    }

    /**
     *
     */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void measurePreloadTime() throws Exception {
        List<String> results = new ArrayList<>();

        boolean[] persistenceModes = new boolean[] {false, true};

        try {
            for (boolean persistence : persistenceModes) {
                persistenceEnabled = persistence;

                for (CacheAtomicityMode atomicity : CacheAtomicityMode.values()) {
                    cacheAtomicityMode = atomicity;

                    long totalTime = 0;

                    for (int i = 0; i < ITERATIONS; i++) {
                        long time = preload(CACHE_SIZE, i == 0);

                        log.info("*** iter=" + i + ", persistence=" + persistence +
                            ", atomicity=" + atomicity + ", time=" + time);

                        totalTime += time;
                    }

                    results.add(String.format("  Average time: %d (persistence=%b, atomicity=%s)",
                        totalTime / ITERATIONS, persistenceEnabled, cacheAtomicityMode));
                }
            }
        }
        finally {
            log.info("*****************************************************************************************");

            for (String result : results)
                log.info(result);

            log.info("*****************************************************************************************");
        }
    }

    /**
     *
     */
    private long preload(int size, boolean validateCacheAfterLoad) throws Exception {
        long time;

        try {
            Ignite node = startGrid(0);

            node.cluster().active(true);

            log.info("Load cache data.");

            Map<Integer, TestObject> data = prepare(size);

            loadCache(node, DEFAULT_CACHE_NAME, data);

            awaitPartitionMapExchange();

            IgniteEx node2 = startGrid(1);

            node.cluster().setBaselineTopology(node2.cluster().nodes());

            IgniteInternalCache<Integer, TestObject> cache = node2.cachex(DEFAULT_CACHE_NAME);

            boolean rebalanceStarted = GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    return !cache.context().preloader().rebalanceFuture().isDone();
                }
            }, 10_000);

            assertTrue(rebalanceStarted);

            preloadStartSync.countDown();

            IgniteInternalFuture<Boolean> fut = cache.context().preloader().rebalanceFuture();

            long start = U.currentTimeMillis();

            fut.get(30, TimeUnit.SECONDS);

            time = U.currentTimeMillis() - start;

            if (validateCacheAfterLoad) {
                log.info("Validation.");

                stopGrid(0);

                awaitPartitionMapExchange();

                node2.cache(DEFAULT_CACHE_NAME);

                for (Map.Entry<Integer, TestObject> e : data.entrySet())
                    assertEquals(e.getValue(), cache.get(e.getKey()));
            }
        }
        finally {
            tearDown();
        }

        return time;
    }

    /**
     * @param cnt Count of entries.
     * @return Preapred data.
     */
    private Map<Integer, TestObject> prepare(int cnt) {
        Map<Integer, TestObject> data = new LinkedHashMap<>(U.capacity(cnt));

        byte[] bytes = new byte[50];

        for (int i = 0; i < cnt; i++)
            data.put(i, new TestObject("val-" + i, "version-" + i, i, i * Integer.MAX_VALUE, bytes));

        return data;
    }

    /**
     *
     */
    private void loadCache(Ignite node, String name, Map<Integer, TestObject> data) {
        try (IgniteDataStreamer<Integer, TestObject> streamer = node.dataStreamer(name)) {
            streamer.addData(data);
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private final CountDownLatch latch;

        /**
         * @param latch Preloading start latch.
         */
        private TestCommunicationSpi(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(
            final ClusterNode node,
            final Message msg,
            final IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            try {
                boolean supplyMsg = msg instanceof GridIoMessage &&
                    ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage;

                if (supplyMsg && latch != null)
                    U.await(latch, 10, TimeUnit.SECONDS);

                super.sendMessage(node, msg, ackC);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException(e);
            }
        }
    }

    /**
     * Test object.
     */
    public static class TestObject {
        /** */
        private String field1;

        /** */
        private String field2;

        /** */
        private int field3;

        /** */
        private long field4;

        /** */
        private byte[] field5;

        /**
         * Default constructor.
         */
        public TestObject() {
            // No-op.
        }

        /**
         *
         */
        public TestObject(String field1, String field2, int field3, long field4, byte[] field5) {
            this.field1 = field1;
            this.field2 = field2;
            this.field3 = field3;
            this.field4 = field4;
            this.field5 = field5;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject obj = (TestObject)o;

            return field3 == obj.field3 &&
                field4 == obj.field4 &&
                Objects.equals(field1, obj.field1) &&
                Objects.equals(field2, obj.field2) &&
                Arrays.equals(field5, obj.field5);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = Objects.hash(field1, field2, field3, field4);

            res = 31 * res + Arrays.hashCode(field5);

            return res;
        }
    }
}
