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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheObjectTransformedEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.transform.CacheObjectTransformerAdapter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class AbstractCacheObjectsTransformationTest extends GridCommonAbstractTest {
    /** Cache name. */
    protected static final String CACHE_NAME = "data";

    /** Nodes count. */
    protected static final int NODES = 3;

    /** Key. */
    protected int key;

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
    protected CacheConfiguration<?, ?> cacheConfiguration() {
        CacheConfiguration<?, ?> cfg = defaultCacheConfiguration();

        cfg.setName(CACHE_NAME);
        cfg.setBackups(NODES);
        cfg.setReadFromBackup(true);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setAffinity(new RendezvousAffinityFunction(false, 1)); // Simplifies event calculation.

        return cfg;
    }

    /**
     *
     */
    protected Ignite prepareCluster() throws Exception {
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

        return ignite;
    }

    /**
     *
     */
    protected void putAndGet(Object val, boolean transformableVal, boolean reversed) {
        boolean binarizableVal = !(val instanceof String || val instanceof Integer || val instanceof Object[] ||
            val instanceof int[] || val instanceof Collection);

        Object k = reversed ? val : ++key;
        Object v = reversed ? ++key : val;

        putWithCheck(k, v, !reversed && binarizableVal, transformableVal);
        getWithCheck(k, v, transformableVal);
    }

    /**
     *
     */
    private void putWithCheck(Object key, Object val, boolean binarizableVal, boolean transformableVal) {
        Ignite node = backupNode(0, CACHE_NAME); // Any key, besause of single partition.

        IgniteCache<Object, Object> cache = node.getOrCreateCache(CACHE_NAME);

        cache.put(key, val);

        int transformed = transformableVal ? 1 : 0;
        int transformCancelled = transformableVal ? 0 : 1;
        int restored = transformableVal && binarizableVal ? NODES : 0; // Binary array is required (e.g. to wait for proper Metadata)

        checkEvents(transformed, transformCancelled, restored, transformableVal);
    }

    /**
     *
     */
    private void getWithCheck(Object key, Object expVal, boolean transformableVal) {
        for (Ignite node : G.allGrids()) {
            for (boolean keepBinary : new boolean[] {true, false})
                getWithCheck(node, key, expVal, transformableVal, keepBinary);
        }
    }

    /**
     *
     */
    private void getWithCheck(Ignite node, Object key, Object expVal, boolean transformableVal, boolean keepBinary) {
        IgniteCache<Object, Object> cache = node.getOrCreateCache(CACHE_NAME);

        if (keepBinary)
            cache = cache.withKeepBinary();

        Object obj = cache.get(key);

        CacheObjectContext coCtx = ((IgniteCacheProxy<?, ?>)cache).context().cacheObjectContext();

        expVal = coCtx.unwrapBinaryIfNeeded(expVal, false, true, null);
        obj = coCtx.unwrapBinaryIfNeeded(obj, false, true, null);

        assertEqualsArraysAware(expVal, obj);

        int restored = transformableVal ? 1 : 0; // Value restored.

        checkEvents(0, 0, restored, transformableVal);
    }

    /**
     *
     */
    private void checkEvents(int transformed, int transformCancelled, int restored, boolean transformableVal) {
        for (int i = transformed + transformCancelled + restored; i > 0; i--) {
            CacheObjectTransformedEvent evt = event();

            if (evt.isRestore())
                restored--;
            else {
                boolean arrEqual = Arrays.equals(evt.getOriginal(), evt.getTransformed());

                assertEquals(transformableVal, !arrEqual);

                if (!arrEqual)
                    transformed--;
                else
                    transformCancelled--;
            }
        }

        assertEquals(0, transformed);
        assertEquals(0, transformCancelled);
        assertEquals(0, restored);

        checkEventsAbsent();
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
    protected static final class BinarizableData {
        /** String. */
        String str;

        /** List. */
        List<Object> list;

        /** Int. */
        Integer i;

        /** Data. */
        BinarizableData data;

        /** */
        public BinarizableData(String str, List<Object> list, Integer i) {
            this.str = str;
            this.list = list;
            this.i = i;
        }

        /** */
        public BinarizableData(String str, List<Object> list, Integer i, BinarizableData data) {
            this.str = str;
            this.list = list;
            this.i = i;
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            BinarizableData data = (BinarizableData)o;

            return Objects.equals(str, data.str) && Objects.equals(list, data.list) && Objects.equals(i, data.i);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(str, list, i);
        }
    }

    /**
     *
     */
    protected static final class ControllableCacheObjectTransformer extends CacheObjectTransformerAdapter {
        /** Shift. */
        private static volatile int shift;

        /** Transformation counter. */
        public static final Map<Integer, AtomicInteger> tCntr = new ConcurrentHashMap<>();

        /** Restoration counter. */
        public static final Map<Integer, AtomicInteger> rCntr = new ConcurrentHashMap<>();

        /**
         *
         */
        public static void transformationShift(int shift) {
            ControllableCacheObjectTransformer.shift = shift;
        }

        /**
         *
         */
        public static boolean failOnTransformation() {
            return shift == 0;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer transform(ByteBuffer original) {
            if (failOnTransformation())
                return null;

            tCntr.computeIfAbsent(shift, key -> new AtomicInteger()).incrementAndGet();

            ByteBuffer transformed = byteBuffer(original.remaining() + 4);

            transformed.putInt(shift);

            while (original.hasRemaining())
                transformed.put((byte)(original.get() + shift));

            transformed.flip();

            return transformed;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer restore(ByteBuffer transformed) {
            ByteBuffer restored = byteBuffer(transformed.remaining() - 4);

            int origShift = transformed.getInt();

            rCntr.computeIfAbsent(origShift, key -> new AtomicInteger()).incrementAndGet();

            while (transformed.hasRemaining())
                restored.put((byte)(transformed.get() - origShift));

            restored.flip();

            return restored;
        }
    }
}
