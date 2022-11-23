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

package org.apache.ignite.internal.processors.cache.transform;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CacheObjectsTransformationConfiguration;
import org.apache.ignite.configuration.CacheObjectsTransformer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheObjectTransformedEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.cache.TransformedCacheObject;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Leak test.
 */
public class CacheObjectsTransformationTest extends GridCommonAbstractTest {
    /** Cache name. */
    protected static final String CACHE_NAME = "data";

    /** Nodes count. */
    protected static final int NODES = 3;

    /** Key. */
    protected static int key;

    /** Event queue. */
    protected final ConcurrentLinkedDeque<CacheObjectTransformedEvent> evtQueue = new ConcurrentLinkedDeque<>();

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
            new CacheObjectsTransformationConfiguration()
                .setActiveTransformer(new ControllableCacheObjectsTransformer()));

        // TODO atomic caches

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        Ignite ignite = startGrids(NODES);

        awaitPartitionMapExchange();

        ignite.events().remoteListen(
            (uuid, evt) -> {
                assertTrue(evt instanceof CacheObjectTransformedEvent);

                evtQueue.add((CacheObjectTransformedEvent)evt);

                return true;
            },
            null,
            EventType.EVT_CACHE_OBJECT_TRANSFORMED);

        String str = "test";

        putAndCheck(str, false);
    }

    /**
     * @param obj Object.
     * @param binarizable Binarizable.
     */
    private void putAndCheck(Object obj, boolean binarizable) {
        putAndCheck(obj, binarizable, true);

        try {
            ControllableCacheObjectsTransformer.fail = true;

            putAndCheck(obj, binarizable, false);
        }
        finally {
            ControllableCacheObjectsTransformer.fail = false;
        }
    }

    /**
     *
     */
    protected void putAndCheck(Object obj, boolean binarizable, boolean transformable) {
        if (obj instanceof BinaryObject)
            assertFalse(obj instanceof TransformedCacheObject);

        Ignite node = grid(0);

        IgniteCache<Integer, Object> cache = node.getOrCreateCache(CACHE_NAME);

        cache.put(++key, obj); // TODO putAll

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

        if (transformable){
            assertFalse(evt.toString(), Arrays.equals(evt.getOriginal(), evt.getTransformed()));
            assertNotNull(evt.toString(), evt.getTransformed());
        }
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
    private static final class ControllableCacheObjectsTransformer implements CacheObjectsTransformer {
        /** Fail. */
        private static boolean fail;

        /** {@inheritDoc} */
        @Override public byte[] transform(byte[] bytes) throws IgniteCheckedException {
            if (fail)
                throw new IgniteCheckedException("Failed.");

            byte[] res = new byte[bytes.length];

            for (int i = 0; i < bytes.length; i++)
                res[i] = (byte)(bytes[i] + 1);

            return res;
        }

        /** {@inheritDoc} */
        @Override public byte[] restore(byte[] bytes) {
            byte[] res = new byte[bytes.length];

            for (int i = 0; i < bytes.length; i++)
                res[i] = (byte)(bytes[i] - 1);

            return res;
        }
    }

    /**
     *
     */
    protected static final class BinarizableData {
        /** String. */
        String str;

        /** Map. */
        Map<Integer, Object> map;

        /** Int. */
        Integer i;

        /**
         * @param str String.
         * @param map Map.
         * @param i I.
         */
        public BinarizableData(String str, Map<Integer, Object> map, Integer i) {
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
            BinarizableData data = (BinarizableData)o;
            return Objects.equals(str, data.str) && Objects.equals(map, data.map) && Objects.equals(i, data.i);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(str, map, i);
        }
    }
}
