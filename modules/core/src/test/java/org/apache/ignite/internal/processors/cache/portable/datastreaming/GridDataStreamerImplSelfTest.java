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

package org.apache.ignite.internal.processors.cache.portable.datastreaming;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.portable.*;
import org.apache.ignite.portable.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Tests for {@code IgniteDataStreamerImpl}.
 */
public class GridDataStreamerImplSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of keys to load via data streamer. */
    private static final int KEYS_COUNT = 1000;

    /** Flag indicating should be cache configured with portables or not.  */
    private static boolean portables;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        if (portables) {
            PortableMarshaller marsh = new PortableMarshaller();

            cfg.setMarshaller(marsh);
        }

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setNearConfiguration(null);
        cacheCfg.setBackups(0);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        return cacheCfg;
    }

    /**
     * Data streamer should correctly load entries from HashMap in case of grids with more than one node
     *  and with GridOptimizedMarshaller that requires serializable.
     *
     * @throws Exception If failed.
     */
    public void testAddDataFromMap() throws Exception {
        try {
            portables = false;

            startGrids(2);

            awaitPartitionMapExchange();

            Ignite g0 = grid(0);

            IgniteDataStreamer<Integer, String> dataLdr = g0.dataStreamer(null);

            Map<Integer, String> map = U.newHashMap(KEYS_COUNT);

            for (int i = 0; i < KEYS_COUNT; i ++)
                map.put(i, String.valueOf(i));

            dataLdr.addData(map);

            dataLdr.close();

            checkDistribution(grid(0));

            checkDistribution(grid(1));

            // Check several random keys in cache.
            Random rnd = new Random();

            IgniteCache<Integer, String> c0 = g0.cache(null);

            for (int i = 0; i < 100; i ++) {
                Integer k = rnd.nextInt(KEYS_COUNT);

                String v = c0.get(k);

                assertEquals(k.toString(), v);
            }
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * Data streamer should add portable object that weren't registered explicitly.
     *
     * @throws Exception If failed.
     */
    public void testAddMissingPortable() throws Exception {
        try {
            portables = true;

            startGrids(2);

            awaitPartitionMapExchange();

            Ignite g0 = grid(0);

            IgniteDataStreamer<Integer, TestObject2> dataLdr = g0.dataStreamer(null);

            dataLdr.perNodeBufferSize(1);
            dataLdr.autoFlushFrequency(1L);

            Map<Integer, TestObject2> map = U.newHashMap(KEYS_COUNT);

            for (int i = 0; i < KEYS_COUNT; i ++)
                map.put(i, new TestObject2(i));

            dataLdr.addData(map).get();

            dataLdr.close();
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * Data streamer should correctly load portable entries from HashMap in case of grids with more than one node
     *  and with GridOptimizedMarshaller that requires serializable.
     *
     * @throws Exception If failed.
     */
    public void testAddPortableDataFromMap() throws Exception {
        try {
            portables = true;

            startGrids(2);

            awaitPartitionMapExchange();

            Ignite g0 = grid(0);

            IgniteDataStreamer<Integer, TestObject> dataLdr = g0.dataStreamer(null);

            Map<Integer, TestObject> map = U.newHashMap(KEYS_COUNT);

            for (int i = 0; i < KEYS_COUNT; i ++)
                map.put(i, new TestObject(i));

            dataLdr.addData(map);

            dataLdr.close(false);

            checkDistribution(grid(0));

            checkDistribution(grid(1));

            // Read random keys. Take values as TestObject.
            Random rnd = new Random();

            IgniteCache<Integer, TestObject> c = g0.cache(null);

            for (int i = 0; i < 100; i ++) {
                Integer k = rnd.nextInt(KEYS_COUNT);

                TestObject v = c.get(k);

                assertEquals(k, v.val());
            }

            // Read random keys. Take values as PortableObject.
            IgniteCache<Integer, PortableObject> c2 = ((IgniteCacheProxy)c).keepPortable();

            for (int i = 0; i < 100; i ++) {
                Integer k = rnd.nextInt(KEYS_COUNT);

                PortableObject v = c2.get(k);

                assertEquals(k, v.field("val"));
            }
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * Check that keys correctly destributed by nodes after data streamer.
     *
     * @param g Grid to check.
     */
    private void checkDistribution(Ignite g) {
        ClusterNode n = g.cluster().localNode();
        IgniteCache c = g.cache(null);

        // Check that data streamer correctly split data by nodes.
        for (int i = 0; i < KEYS_COUNT; i ++) {
            if (g.affinity(null).isPrimary(n, i))
                assertNotNull(c.localPeek(i, CachePeekMode.ONHEAP));
            else
                assertNull(c.localPeek(i, CachePeekMode.ONHEAP));
        }
    }

    /**
     */
    private static class TestObject implements PortableMarshalAware, Serializable {
        /** */
        private int val;

        /**
         *
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

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            writer.writeInt("val", val);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            val = reader.readInt("val");
        }
    }

    /**
     */
    private static class TestObject2 implements PortableMarshalAware, Serializable {
        /** */
        private int val;

        /**
         */
        private TestObject2() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        private TestObject2(int val) {
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
            return obj instanceof TestObject2 && ((TestObject2)obj).val == val;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            writer.writeInt("val", val);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            val = reader.readInt("val");
        }
    }
}
