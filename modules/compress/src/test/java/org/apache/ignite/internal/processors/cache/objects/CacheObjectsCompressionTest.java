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

package org.apache.ignite.internal.processors.cache.objects;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CacheObjectsTransformationConfiguration;
import org.apache.ignite.configuration.CacheObjectsTransformer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheObjectTransformedEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.cache.TransformedCacheObject;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.GridUnsafe.NATIVE_BYTE_ORDER;

/**
 * Leak test.
 */
public class CacheObjectsCompressionTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "data";

    /** Nodes count. */
    private static final int NODES = 3;

    /** Key. */
    private static int key;

    /** Event queue. */
    private final ConcurrentLinkedDeque<CacheObjectTransformedEvent> evtQueue = new ConcurrentLinkedDeque<>();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration());
        cfg.setIncludeEventTypes(EventType.EVT_CACHE_OBJECT_TRANSFORMED);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setName("region")
                        .setMetricsEnabled(true)
                        .setMaxSize(1000L * 1024 * 1024)
                        .setInitialSize(1000L * 1024 * 1024))
                .setMetricsEnabled(true));

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Data cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(CACHE_NAME);
        cfg.setBackups(NODES);
        cfg.setReadFromBackup(true);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheObjectsTransformationConfiguration(
            new CacheObjectsTransformationConfiguration().setActiveTransformer(new TestCacheObjectsTransformer()));

        // TODO atomic caches

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void test() throws Exception {
        Ignite ignite = startGrids(NODES);

        awaitPartitionMapExchange();

        ignite.events().remoteListen((uuid, evt) -> {
                assertTrue(evt instanceof CacheObjectTransformedEvent);

                evtQueue.add((CacheObjectTransformedEvent)evt);

                return true;
            },
            null,
            EventType.EVT_CACHE_OBJECT_TRANSFORMED);

        String str = "Ololo";

        putAndCheck(str, false, false);

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 100; i++)
            sb.append("AAAAAAAAAA");

        String str2 = sb.toString();

        putAndCheck(str2, false, true);

        putAndCheck(new ShortData(0), true, false);

        Map<Integer, Object> map = new HashMap<>();

        map.put(1, new Data(str, null, 42));
        map.put(2, new Data(str, null, 42));
        map.put(42, new Data(str, null, 42));

        Data data = new Data(str, map, 42);

        putAndCheck(data, true, true);

        Map<Integer, Object> map2 = new HashMap<>();

        map2.put(1, new Data(str2, null, 47));
        map2.put(2, new Data(str2, null, 47));
        map2.put(42, new Data(str2, null, 47));

        Data data2 = new Data(str, map2, 47);

        putAndCheck(data2, true, true);

        BinaryObjectBuilder builder = ignite.binary().builder(Data.class.getName());

        builder.setField("str", str2);
        builder.setField("map", map);
        builder.setField("i", 42);

        putAndCheck(builder.build(), true, true);

        builder.setField("str", str);

        putAndCheck(builder.build(), true, true);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testUsedMemorySize() throws Exception {
        Ignite ignite = startGrids(NODES);

        awaitPartitionMapExchange();

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 1000; i++)
            sb.append("AAAAAAAAAA");

        String str = sb.toString();

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 1_000; i++) {
            if (i % 100 == 0)
                log.info(">>");

            cache.put(i, str);
        }

        DataRegionMetrics metrics = ignite.dataRegionMetrics("region");

        float memSpend = metrics.getTotalUsedSize() * metrics.getPagesFillFactor();

        log.info(String.valueOf(memSpend));
    }

    /**
     *
     */
    private void putAndCheck(Object obj, boolean binarizable, boolean transformable) {
        if (obj instanceof BinaryObject)
            assertFalse(obj instanceof TransformedCacheObject);

        Ignite node = grid(0);

        IgniteCache<Integer, Object> cache = node.getOrCreateCache(CACHE_NAME);

        cache.put(++key, obj); // TODO keys

        checkPut(transformable);
        checkEventsAbsent();
        checkGet(obj, binarizable, transformable);

        try (IgniteDataStreamer<Integer, Object> stmr = node.dataStreamer(CACHE_NAME)) {
            stmr.addData(++key, obj);

            stmr.flush();
        }

        while (!evtQueue.isEmpty())
            checkPut(transformable);

        checkGet(obj, binarizable, transformable);
    }

    /**
     *
     */
    private void checkPut(boolean transformable) {
        CacheObjectTransformedEvent evt = event();

        assertFalse(evt.isRestore());

        if (transformable)
            assertTrue(evt.toString(), evt.getTransformed().length < evt.getOriginal().length);
        else
            assertNull(evt.toString(), evt.getTransformed());
    }

    /**
     *
     */
    private void checkGet(
        Object obj,
        boolean binarizable,
        boolean transformable) {
        if (obj instanceof BinaryObject) {
            assertTrue(binarizable);

            obj = ((BinaryObject)obj).deserialize();
        }

        for (Ignite node : G.allGrids()) {
            getWithCheck(node, obj, binarizable, transformable, false);
            getWithCheck(node, obj, binarizable, transformable, true);
        }

        checkEventsAbsent();
    }

    /**
     *
     */
    private void getWithCheck(
        Ignite node,
        Object expected,
        boolean binarizable,
        boolean transformable,
        boolean keepBinary) {
        IgniteCache<Integer, Object> cache = node.getOrCreateCache(CACHE_NAME);

        if (keepBinary)
            cache = cache.withKeepBinary();

        Object obj = cache.get(key);

        if (keepBinary && binarizable) {
            assertFalse(obj.getClass().getName(), obj instanceof TransformedCacheObject);

            obj = ((BinaryObject)obj).deserialize();
        }

        if (transformable) {
            CacheObjectTransformedEvent evt = event();

            assertTrue(evt.toString(), evt.isRestore());
        }

        assertEquals(expected, obj);
    }

    /**
     *
     */
    private void checkEventsAbsent() {
        assertTrue(evtQueue.size() + " unhandled events", evtQueue.isEmpty());
    }

    /**
     *
     */
    private CacheObjectTransformedEvent event() {
        CacheObjectTransformedEvent evt = null;

        while (evt == null)
            evt = evtQueue.poll();

        return evt;
    }

    /**
     *
     */
    private static final class Data {
        /** String. */
        String str;

        /** Map. */
        Map<Integer, Object> map;

        /** I. */
        Integer i;

        /**
         * @param str String.
         * @param map Map.
         * @param i I.
         */
        public Data(String str, Map<Integer, Object> map, Integer i) {
            this.str = str;
            this.map = map;
            this.i = i;
        }

        /**
         * @return String.
         */
        public String string() {
            return str;
        }

        /**
         * @param str New string.
         */
        public void string(String str) {
            this.str = str;
        }

        /**
         * @return Map.
         */
        public Map<Integer, Object> map() {
            return map;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Data data = (Data)o;
            return Objects.equals(str, data.str) && Objects.equals(map, data.map) && Objects.equals(i, data.i);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(str, map, i);
        }
    }

    /**
     *
     */
    private static final class ShortData {
        private final Integer i;

        /**
         * @param i I.
         */
        public ShortData(Integer i) {
            this.i = i;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ShortData data = (ShortData)o;
            return Objects.equals(i, data.i);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(i);
        }
    }

    /**
     *
     */
    private static final class TestCacheObjectsTransformer implements CacheObjectsTransformer {
        /** A bit more than max page size. */
        private static final ThreadLocalByteBuffer buf = new ThreadLocalByteBuffer( 1 << 10);

        /** {@inheritDoc} */
        @Override public byte[] transform(byte[] bytes) throws IgniteCheckedException {
            int length = bytes.length;

            ByteBuffer src = ByteBuffer.allocateDirect(bytes.length);

            src.put(bytes, 0, bytes.length);
            src.flip();

            ByteBuffer transformed = buf.get();

            int overhead = 4;

            if (transformed.capacity() - overhead < src.remaining())
                transformed = allocateDirectBuffer(src.remaining()); // TODO customize

            buf.set(transformed);

            transformed.limit(src.capacity());
            transformed.position(overhead);

            try {
                Zstd.compress(transformed, src, 1); // TODO customize
            }
            catch (ZstdException ex) {
                throw new IgniteCheckedException(ex);
            }

            transformed.flip();
            transformed.putInt(length);
            transformed.position(0);

            byte[] res = new byte[transformed.remaining()];

            transformed.get(res);

            return res;
        }

        /** {@inheritDoc} */
        @Override public byte[] restore(byte[] bytes) {
            ByteBuffer src = ByteBuffer.allocateDirect(bytes.length);

            src.put(bytes, 0, bytes.length);
            src.flip();

            int length = src.getInt();

            ByteBuffer restored = buf.get();

            if (restored.capacity() < length)
                restored = allocateDirectBuffer(length);

            buf.set(restored);

            Zstd.decompress(restored, src); // TODO customize

            restored.flip();

            byte[] res = new byte[restored.remaining()];

            restored.get(res);

            return res;
        }

        /**
         */
        static final class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
            /** */
            final int size;

            /**
             * @param size Size.
             */
            ThreadLocalByteBuffer(int size) {
                this.size = size;
            }

            /** {@inheritDoc} */
            @Override protected ByteBuffer initialValue() {
                return allocateDirectBuffer(size);
            }

            /** {@inheritDoc} */
            @Override public ByteBuffer get() {
                ByteBuffer buf = super.get();

                buf.clear();

                return buf;
            }
        }

        /**
         *
         */
        static ByteBuffer allocateDirectBuffer(int cap) {
            return ByteBuffer.allocateDirect(cap).order(NATIVE_BYTE_ORDER);
        }
    }
}
