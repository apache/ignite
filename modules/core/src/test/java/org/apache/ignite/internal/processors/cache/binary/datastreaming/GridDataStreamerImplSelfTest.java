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

package org.apache.ignite.internal.processors.cache.binary.datastreaming;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for {@code IgniteDataStreamerImpl}.
 */
public class GridDataStreamerImplSelfTest extends GridCommonAbstractTest {
    /** Number of keys to load via data streamer. */
    private static final int KEYS_COUNT = 1000;

    /** Flag indicating should be cache configured with binary or not.  */
    private static boolean binaries;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (binaries) {
            BinaryMarshaller marsh = new BinaryMarshaller();

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
    @Test
    public void testAddDataFromMap() throws Exception {
        try {
            binaries = false;

            startGrids(2);

            awaitPartitionMapExchange();

            Ignite g0 = grid(0);

            IgniteDataStreamer<Integer, String> dataLdr = g0.dataStreamer(DEFAULT_CACHE_NAME);

            Map<Integer, String> map = U.newHashMap(KEYS_COUNT);

            for (int i = 0; i < KEYS_COUNT; i++)
                map.put(i, String.valueOf(i));

            dataLdr.addData(map);

            dataLdr.close();

            checkDistribution(grid(0));

            checkDistribution(grid(1));

            // Check several random keys in cache.
            Random rnd = new Random();

            IgniteCache<Integer, String> c0 = g0.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 100; i++) {
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
     * Data streamer should add binary object that weren't registered explicitly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAddMissingBinary() throws Exception {
        try {
            binaries = true;

            startGrids(2);

            awaitPartitionMapExchange();

            Ignite g0 = grid(0);

            IgniteDataStreamer<Integer, TestObject2> dataLdr = g0.dataStreamer(DEFAULT_CACHE_NAME);

            dataLdr.perNodeBufferSize(1);
            dataLdr.autoFlushFrequency(1L);

            Map<Integer, TestObject2> map = U.newHashMap(KEYS_COUNT);

            for (int i = 0; i < KEYS_COUNT; i++)
                map.put(i, new TestObject2(i));

            dataLdr.addData(map).get();

            dataLdr.close();
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * Data streamer should correctly load binary entries from HashMap in case of grids with more than one node
     *  and with GridOptimizedMarshaller that requires serializable.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAddBinaryDataFromMap() throws Exception {
        try {
            binaries = true;

            startGrids(2);

            awaitPartitionMapExchange();

            Ignite g0 = grid(0);

            IgniteDataStreamer<Integer, TestObject> dataLdr = g0.dataStreamer(DEFAULT_CACHE_NAME);

            Map<Integer, TestObject> map = U.newHashMap(KEYS_COUNT);

            for (int i = 0; i < KEYS_COUNT; i++)
                map.put(i, new TestObject(i));

            dataLdr.addData(map);

            dataLdr.close(false);

            checkDistribution(grid(0));

            checkDistribution(grid(1));

            // Read random keys. Take values as TestObject.
            Random rnd = new Random();

            IgniteCache<Integer, TestObject> c = g0.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 100; i++) {
                Integer k = rnd.nextInt(KEYS_COUNT);

                TestObject v = c.get(k);

                assertEquals(k, v.val());
            }

            // Read random keys. Take values as BinaryObject.
            IgniteCache<Integer, BinaryObject> c2 = ((IgniteCacheProxy)c).keepBinary();

            for (int i = 0; i < 100; i++) {
                Integer k = rnd.nextInt(KEYS_COUNT);

                BinaryObject v = c2.get(k);

                assertEquals(k, v.field("val"));
            }
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     *  Tries to propagate cache with binary objects created using the builder.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAddBinaryCreatedWithBuilder() throws Exception {
        try {
            binaries = true;

            startGrids(2);

            awaitPartitionMapExchange();

            Ignite g0 = grid(0);

            IgniteDataStreamer<Integer, BinaryObject> dataLdr = g0.dataStreamer(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 500; i++) {
                BinaryObjectBuilder obj = g0.binary().builder("NoExistedClass");

                obj.setField("id", i);
                obj.setField("name", "name = " + i);

                dataLdr.addData(i, obj.build());
            }

            dataLdr.close(false);

            assertEquals(500, g0.cache(DEFAULT_CACHE_NAME).size(CachePeekMode.ALL));
            assertEquals(500, grid(1).cache(DEFAULT_CACHE_NAME).size(CachePeekMode.ALL));
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * Check that keys correctly distributed by nodes after data streamer.
     *
     * @param g Grid to check.
     */
    private void checkDistribution(Ignite g) {
        ClusterNode n = g.cluster().localNode();
        IgniteCache<Object, Object> c = g.cache(DEFAULT_CACHE_NAME);

        // Check that data streamer correctly split data by nodes.
        for (int i = 0; i < KEYS_COUNT; i++) {
            if (g.affinity(DEFAULT_CACHE_NAME).isPrimary(n, i))
                assertNotNull(c.localPeek(i));
            else
                assertNull(c.localPeek(i));
        }
    }

    /**
     */
    private static class TestObject implements Binarylizable, Serializable {
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
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("val", val);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            val = reader.readInt("val");
        }
    }

    /**
     */
    private static class TestObject2 implements Binarylizable, Serializable {
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
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("val", val);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            val = reader.readInt("val");
        }
    }
}
