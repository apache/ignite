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

package org.apache.ignite.internal.util.collection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link NonBlockingHashMapLong}.
 */
public class NonBlockingHashMapLongTest {
    /** Keys covering the special zero key, negative keys and the extremes. */
    private static final long[] EDGE_KEYS = {0L, 1L, -1L, 42L, Long.MIN_VALUE, Long.MAX_VALUE, 1L << 32, -(1L << 40)};

    /** */
    @Test
    public void testPutGetRemove() {
        NonBlockingHashMapLong<String> map = new NonBlockingHashMapLong<>();

        assertTrue(map.isEmpty());

        for (long k : EDGE_KEYS) {
            assertNull(map.get(k));
            assertFalse(map.containsKey(k));
            assertNull(map.put(k, "v" + k));
            assertEquals("v" + k, map.get(k));
            assertTrue(map.containsKey(k));
        }

        assertEquals(EDGE_KEYS.length, map.size());

        for (long k : EDGE_KEYS)
            assertEquals("v" + k, map.put(k, "u" + k));

        assertEquals(EDGE_KEYS.length, map.size());

        for (long k : EDGE_KEYS) {
            assertEquals("u" + k, map.remove(k));
            assertNull(map.get(k));
            assertNull(map.remove(k));
        }

        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }

    /** */
    @Test
    public void testConditionalOperations() {
        NonBlockingHashMapLong<String> map = new NonBlockingHashMapLong<>();

        for (long k : EDGE_KEYS) {
            assertNull(map.replace(k, "x")); // Absent: no replace.
            assertFalse(map.replace(k, "x", "y"));
            assertFalse(map.remove(k, "x"));
            assertNull(map.get(k));

            assertNull(map.putIfAbsent(k, "a"));
            assertEquals("a", map.putIfAbsent(k, "b"));
            assertEquals("a", map.get(k));

            assertEquals("a", map.replace(k, "c"));
            assertFalse(map.replace(k, "a", "d"));
            assertTrue(map.replace(k, "c", "d"));
            assertEquals("d", map.get(k));

            assertFalse(map.remove(k, "c"));
            assertTrue(map.remove(k, "d"));
            assertNull(map.get(k));
        }

        assertEquals(0, map.size());
    }

    /** */
    @Test
    public void testComputeIfAbsent() {
        NonBlockingHashMapLong<String> map = new NonBlockingHashMapLong<>();

        AtomicInteger calls = new AtomicInteger();

        for (long k : EDGE_KEYS) {
            assertEquals("v" + k, map.computeIfAbsent(k, key -> {
                calls.incrementAndGet();

                return "v" + key;
            }));

            assertEquals("v" + k, map.computeIfAbsent(k, key -> {
                throw new AssertionError("Must not be called for a present key: " + key);
            }));
        }

        assertEquals(EDGE_KEYS.length, calls.get());
        assertNull(map.computeIfAbsent(12345L, key -> null));
        assertFalse(map.containsKey(12345L));
        assertEquals(EDGE_KEYS.length, map.size());
    }

    /** */
    @Test
    public void testNullValuesRejected() {
        NonBlockingHashMapLong<String> map = new NonBlockingHashMapLong<>();

        for (long k : new long[] {0L, 7L}) {
            assertNpe(() -> map.put(k, null));
            assertNpe(() -> map.putIfAbsent(k, null));
            assertNpe(() -> map.replace(k, null));
            assertNpe(() -> map.remove(k, null));
            assertNpe(() -> map.replace(k, "a", null));
        }

        assertFalse(map.containsValue(null));
        assertTrue(map.isEmpty());
    }

    /**
     * @param r Action expected to throw {@link NullPointerException}.
     */
    private static void assertNpe(Runnable r) {
        try {
            r.run();
        }
        catch (NullPointerException expected) {
            return;
        }

        throw new AssertionError("NullPointerException expected.");
    }

    /** */
    @Test
    public void testResizeKeepsAllEntries() {
        NonBlockingHashMapLong<Long> map = new NonBlockingHashMapLong<>(2);

        int cnt = 200_000;

        // Non-sequential keys to exercise the hash and the reprobe path.
        for (int i = 0; i < cnt; i++) {
            long k = (long)i * 0x9E3779B97F4A7C15L;

            assertNull(map.put(k, Long.valueOf(i)));
        }

        assertEquals(cnt, map.size());

        for (int i = 0; i < cnt; i++)
            assertEquals(Long.valueOf(i), map.get((long)i * 0x9E3779B97F4A7C15L));

        for (int i = 0; i < cnt; i += 2)
            assertEquals(Long.valueOf(i), map.remove((long)i * 0x9E3779B97F4A7C15L));

        assertEquals(cnt / 2, map.size());

        long[] keys = map.keySetLong();

        assertEquals(cnt / 2, keys.length);

        for (long k : keys)
            assertEquals(1, map.get(k) & 1);

        // Churn on a fixed key set triggers same-size resizes which clean out the dead slots.
        for (int round = 0; round < 50; round++) {
            for (int i = 0; i < 1000; i++) {
                map.put(-i - 1, Long.valueOf(round));
                map.remove(-i - 1);
            }
        }

        assertEquals(cnt / 2, map.size());

        map.clear();

        assertEquals(0, map.size());
        assertNull(map.get(0x9E3779B97F4A7C15L));
    }

    /** */
    @Test
    public void testViewsAndIteration() {
        NonBlockingHashMapLong<String> map = new NonBlockingHashMapLong<>();

        Map<Long, String> expected = new HashMap<>();

        for (long k : EDGE_KEYS) {
            map.put(k, "v" + k);
            expected.put(k, "v" + k);
        }

        assertEquals(expected, map);
        assertEquals(expected.hashCode(), map.hashCode());
        assertEquals(expected.keySet(), map.keySet());
        assertEquals(expected.entrySet(), map.entrySet());
        assertEquals(new HashSet<>(expected.values()), new HashSet<>(map.values()));

        long[] keys = map.keySetLong();

        Arrays.sort(keys);

        long[] expKeys = EDGE_KEYS.clone();

        Arrays.sort(expKeys);

        assertTrue(Arrays.equals(expKeys, keys));

        // Non-boxing iteration.
        Set<Long> seen = new HashSet<>();

        NonBlockingHashMapLong<String>.LongKeyIterator iter =
            (NonBlockingHashMapLong<String>.LongKeyIterator)map.keySet().iterator();

        while (iter.hasNext())
            seen.add(iter.nextLong());

        assertEquals(expected.keySet(), seen);

        // Removal through the views.
        Iterator<Map.Entry<Long, String>> entryIter = map.entrySet().iterator();

        Map.Entry<Long, String> first = entryIter.next();

        entryIter.remove();

        assertFalse(map.containsKey(first.getKey()));
        assertEquals(EDGE_KEYS.length - 1, map.size());

        assertTrue(map.values().remove("v42"));
        assertFalse(map.containsKey(42L));

        map.keySet().removeIf(k -> k < 0);

        for (long k : map.keySetLong())
            assertTrue(k >= 0);

        // Entry.setValue writes through.
        Map.Entry<Long, String> e = map.entrySet().iterator().next();

        e.setValue("changed");

        assertEquals("changed", map.get(e.getKey().longValue()));

        map.entrySet().clear();

        assertTrue(map.isEmpty());
        assertFalse(map.keySet().iterator().hasNext());
        assertFalse(map.values().iterator().hasNext());
    }

    /** */
    @Test
    public void testBoxedMapApi() {
        ConcurrentMap<Long, String> map = new NonBlockingHashMapLong<>();

        assertNull(map.put(1L, "a"));
        assertEquals("a", map.get(1L));
        assertTrue(map.containsKey(1L));
        assertFalse(map.containsKey("1"));
        assertNull(map.get("1"));
        assertNull(map.remove("1"));
        assertEquals("a", map.putIfAbsent(1L, "b"));
        assertTrue(map.replace(1L, "a", "c"));
        assertEquals("c", map.replace(1L, "d"));
        assertTrue(map.containsValue("d"));
        assertFalse(map.containsValue("c"));
        assertFalse(map.remove(1L, "c"));
        assertTrue(map.remove(1L, "d"));
        assertTrue(map.isEmpty());
    }

    /** */
    @SuppressWarnings("unchecked")
    @Test
    public void testSerializationAndClone() throws Exception {
        NonBlockingHashMapLong<String> map = new NonBlockingHashMapLong<>();

        for (long k : EDGE_KEYS)
            map.put(k, "v" + k);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(map);
        }

        NonBlockingHashMapLong<String> copy;

        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            copy = (NonBlockingHashMapLong<String>)in.readObject();
        }

        assertEquals(map, copy);
        assertEquals(EDGE_KEYS.length, copy.size());

        NonBlockingHashMapLong<String> cloned = map.clone();

        assertEquals(map, cloned);

        cloned.put(100L, "x");

        assertFalse(map.containsKey(100L));
        assertSame(map.get(1L), cloned.get(1L));
    }

    /** */
    @Test
    public void testConcurrentDisjointKeys() throws Exception {
        NonBlockingHashMapLong<Integer> map = new NonBlockingHashMapLong<>();

        int threads = 8;
        int perThread = 50_000;

        AtomicInteger threadIdx = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            int t = threadIdx.getAndIncrement();

            long base = (long)t << 32;

            for (int i = 0; i < perThread; i++)
                assertNull(map.put(base + i, Integer.valueOf(t)));

            for (int i = 0; i < perThread; i++)
                assertEquals(Integer.valueOf(t), map.get(base + i));

            for (int i = 0; i < perThread; i += 2)
                assertEquals(Integer.valueOf(t), map.remove(base + i));
        }, threads, "putter");

        assertEquals(threads * perThread / 2, map.size());

        for (int t = 0; t < threads; t++) {
            long base = (long)t << 32;

            for (int i = 0; i < perThread; i++)
                assertEquals(i % 2 == 0 ? null : Integer.valueOf(t), map.get(base + i));
        }
    }

    /** */
    @Test
    public void testConcurrentContendedKeys() throws Exception {
        NonBlockingHashMapLong<Integer> map = new NonBlockingHashMapLong<>();

        int keys = 64;
        int iterations = 100_000;

        AtomicInteger wins = new AtomicInteger();
        AtomicInteger removals = new AtomicInteger();

        Integer one = 1;

        GridTestUtils.runMultiThreaded(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < iterations; i++) {
                long k = rnd.nextInt(keys) - keys / 2; // Including the zero key.

                if (map.putIfAbsent(k, one) == null)
                    wins.incrementAndGet();
                else if (map.remove(k, one))
                    removals.incrementAndGet();

                Integer v = map.get(k);

                assertTrue(v == null || v == 1);
            }
        }, 8, "churner");

        // Every successful remove was preceded by a successful putIfAbsent, and what remains is what was not removed.
        assertEquals(wins.get() - removals.get(), map.size());

        for (long k : map.keySetLong())
            assertEquals(Integer.valueOf(1), map.get(k));
    }
}
