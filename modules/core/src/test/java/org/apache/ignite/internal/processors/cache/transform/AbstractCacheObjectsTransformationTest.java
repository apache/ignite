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
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheObjectTransformedEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.transform.CacheObjectTransformerAdapter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Leak test.
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
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

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
    protected void putAndCheck(Object val, boolean transformableKey, boolean transformableVal, boolean reversed) {
        boolean binarizableVal = !(val instanceof String || val instanceof Integer || val instanceof Object[] ||
            val instanceof int[] || val instanceof Collection);

        boolean binarizableColVal = (val instanceof Object[] && !(val instanceof String[] || val instanceof int[])) ||
            (val instanceof Collection && !(
                ((Iterable<?>)val).iterator().next() instanceof String ||
                    ((Iterable<?>)val).iterator().next() instanceof Integer)
            );

        boolean binaryVal = val instanceof BinaryObject;
        boolean binaryColVal = val instanceof BinaryObject[] ||
            (val instanceof Collection && ((Iterable<?>)val).iterator().next() instanceof BinaryObject);

        if (binaryVal)
            assertTrue(binarizableVal);

        if (binaryColVal)
            assertTrue(binarizableColVal);

        assertFalse(binaryVal && binaryColVal);
        assertFalse(binarizableVal && binarizableColVal);

        Ignite node = backupNode(0, CACHE_NAME); // Any key, besause of single partition.

        IgniteCache<Object, Object> cache = node.getOrCreateCache(CACHE_NAME);

        Object k = reversed ? val : ++key;
        Object v = reversed ? ++key : val;

        cache.put(k, v);

        checkPut(!reversed && binarizableVal, reversed ? transformableKey : transformableVal);

        checkGet(
            k,
            v,
            !reversed && binaryVal,
            !reversed && binaryColVal,
            !reversed && binarizableVal,
            !reversed && binarizableColVal,
            reversed ? transformableKey : transformableVal);
    }

    /**
     *
     */
    private void checkPut(
        boolean binarizableVal,
        boolean transformableVal) {
        int transformed = transformableVal ? 1 : 0;
        int transformCancelled = transformableVal ? 0 : 1;
        int restored = transformableVal && binarizableVal ? NODES : 0; // Binary array is required (e.g. to wait for proper Metadata)

        checkEvents(transformed, transformCancelled, restored);
    }

    /**
     *
     */
    private void checkGet(
        Object key,
        Object expVal,
        boolean binaryExpVal,
        boolean binaryColExpVal,
        boolean binarizableVal,
        boolean binarizableColVal,
        boolean transformableVal) {
        for (Ignite node : G.allGrids()) {
            for (boolean keepBinary : new boolean[] {true, false})
                getWithCheck(
                    node,
                    key,
                    expVal,
                    binaryExpVal,
                    binaryColExpVal,
                    binarizableVal,
                    binarizableColVal,
                    transformableVal,
                    keepBinary);
        }
    }

    /**
     *
     */
    private void getWithCheck(
        Ignite node,
        Object key,
        Object expVal,
        boolean binaryExpVal,
        boolean binaryColExpVal,
        boolean binarizableVal,
        boolean binarizableColVal,
        boolean transformableVal,
        boolean keepBinary) {
        IgniteCache<Object, Object> cache = node.getOrCreateCache(CACHE_NAME);

        if (keepBinary)
            cache = cache.withKeepBinary();

        Object obj = cache.get(key);

        // Need to deserialize the expectation to compare with the deserialized get result.
        if (!keepBinary && (binaryExpVal || binaryColExpVal))
            expVal = deserializeBinary(expVal);

        // Deserializing the get result to compare with the non-binary expectation.
        if (keepBinary && ((binarizableVal && !binaryExpVal) || (binarizableColVal && !binaryColExpVal)))
            obj = deserializeBinary(obj);

        assertEqualsArraysAware(expVal, obj);

        int transformed = 0;
        int transformCancelled = 0;
        int restored = transformableVal ? 1 : 0; // Value restored.

        checkEvents(transformed, transformCancelled, restored);
    }

    /**
     *
     */
    private void checkEvents(int transformed, int transformCancelled, int restored) {
        for (int i = transformed + transformCancelled + restored; i > 0; i--) {
            CacheObjectTransformedEvent evt = event();

            if (evt.isRestore())
                restored--;
            else if (evt.getTransformed() != null) {
                transformed--;

                assertFalse(evt.toString(), Arrays.equals(evt.getOriginal(), evt.getTransformed()));
            }
            else
                transformCancelled--;
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
    private Object deserializeBinary(Object obj) {
        Object res;

        if (obj instanceof Object[])
            res = deserializeBinaryArray((Object[])obj);
        else if (obj instanceof Collection)
            res = deserializeBinaryCollection((Collection<Object>)obj);
        else
            res = ((BinaryObject)obj).deserialize();

        return res;
    }

    /**
     *
     */
    private Object[] deserializeBinaryArray(Object[] objs) {
        Object[] des = new Object[objs.length];

        for (int i = 0; i < objs.length; i++)
            des[i] = ((BinaryObject)objs[i]).deserialize();

        return des;
    }

    /**
     *
     */
    private Collection<Object> deserializeBinaryCollection(Collection<Object> objSet) {
        return objSet.stream()
            .map(obj -> ((BinaryObject)obj).deserialize())
            .collect(Collectors.toList());
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
        @Override protected ByteBuffer transform(ByteBuffer original) {
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
        @Override protected ByteBuffer restore(ByteBuffer transformed) {
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
