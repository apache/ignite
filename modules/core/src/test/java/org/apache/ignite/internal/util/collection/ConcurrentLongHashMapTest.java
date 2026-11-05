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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.jsr166.ConcurrentLongHashMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ConcurrentLongHashMap}.
 */
public class ConcurrentLongHashMapTest {
    /** Keys covering the zero key, negative keys and the extremes. */
    private static final long[] EDGE_KEYS = {0L, 1L, -1L, 42L, Long.MIN_VALUE, Long.MAX_VALUE, 1L << 32, -(1L << 40)};

    /** */
    @Test
    public void testPutGetRemove() {
        ConcurrentLongHashMap<String> map = new ConcurrentLongHashMap<>();

        assertTrue(map.isEmpty());

        for (long k : EDGE_KEYS) {
            assertNull(map.get(k));
            assertFalse(map.containsKey(k));
            assertNull(map.put(k, "v" + k));
            assertEquals("v" + k, map.get(k));
            assertTrue(map.containsKey(k));
        }

        assertEquals(EDGE_KEYS.length, map.size());
        assertEquals(EDGE_KEYS.length, map.mappingCount());

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
        ConcurrentLongHashMap<String> map = new ConcurrentLongHashMap<>();

        for (long k : EDGE_KEYS) {
            assertNull(map.replace(k, "x")); // Absent: no replace.
            assertFalse(map.replace(k, "x", "y"));
            assertFalse(map.remove(k, "x"));
            assertNull(map.get(k));
            assertEquals("def", map.getOrDefault(k, "def"));

            assertNull(map.putIfAbsent(k, "a"));
            assertEquals("a", map.putIfAbsent(k, "b"));
            assertEquals("a", map.get(k));
            assertEquals("a", map.getOrDefault(k, "def"));

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
        ConcurrentLongHashMap<String> map = new ConcurrentLongHashMap<>();

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
    public void testCompute() {
        ConcurrentLongHashMap<String> map = new ConcurrentLongHashMap<>();

        for (long k : EDGE_KEYS) {
            // Absent: the function sees null, a null result leaves the map unchanged.
            assertNull(map.compute(k, (key, old) -> {
                assertEquals(k, key);
                assertNull(old);

                return null;
            }));

            assertFalse(map.containsKey(k));

            assertEquals("a", map.compute(k, (key, old) -> "a"));
            assertEquals("a", map.get(k));

            // Present: update in place.
            assertEquals("ab", map.compute(k, (key, old) -> old + "b"));
            assertEquals("ab", map.get(k));

            // Present: a null result removes the mapping.
            assertNull(map.compute(k, (key, old) -> {
                assertEquals("ab", old);

                return null;
            }));

            assertFalse(map.containsKey(k));
        }

        assertEquals(0, map.size());
    }

    /** */
    @Test
    public void testNullValuesRejected() {
        ConcurrentLongHashMap<String> map = new ConcurrentLongHashMap<>();

        for (long k : new long[] {0L, 7L}) {
            assertNpe(() -> map.put(k, null));
            assertNpe(() -> map.putIfAbsent(k, null));
            assertNpe(() -> map.replace(k, null));
            assertNpe(() -> map.remove(k, null));
            assertNpe(() -> map.replace(k, "a", null));
            assertNpe(() -> map.replace(k, null, "a"));
        }

        assertNpe(() -> map.containsValue(null));
        assertNpe(() -> map.forEach(null));
        assertNpe(() -> map.computeIfAbsent(1L, null));
        assertNpe(() -> map.compute(1L, null));
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
        ConcurrentLongHashMap<Long> map = new ConcurrentLongHashMap<>(2);

        int cnt = 200_000;

        // Non-sequential keys to exercise the hash.
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

        long[] keys = keys(map);

        assertEquals(cnt / 2, keys.length);

        for (long k : keys)
            assertEquals(1, map.get(k) & 1);

        // Put/remove churn on a fixed key set must not change the live set.
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

    /**
     * Keys whose halves are equal all hash to the same bin, so a bin grows well past the threshold at which the JDK
     * would convert it to a tree. The port grows the table while it is small and keeps long lists afterwards.
     */
    @Test
    public void testCollidingKeys() {
        ConcurrentLongHashMap<Integer> map = new ConcurrentLongHashMap<>();

        int cnt = 2000;

        for (int i = 1; i <= cnt; i++)
            assertNull(map.put(((long)i << 32) | i, i));

        assertEquals(cnt, map.size());

        for (int i = 1; i <= cnt; i++)
            assertEquals(Integer.valueOf(i), map.get(((long)i << 32) | i));

        for (int i = 1; i <= cnt; i += 3)
            assertEquals(Integer.valueOf(i), map.remove(((long)i << 32) | i));

        for (int i = 1; i <= cnt; i++)
            assertEquals(i % 3 == 1 ? null : Integer.valueOf(i), map.get(((long)i << 32) | i));

        assertEquals(cnt - (cnt + 2) / 3, map.size());
        assertEquals(cnt - (cnt + 2) / 3, keys(map).length);
    }

    /** */
    @Test
    public void testValuesAndIteration() {
        ConcurrentLongHashMap<String> map = new ConcurrentLongHashMap<>();

        Map<Long, String> expected = new HashMap<>();

        for (long k : EDGE_KEYS) {
            map.put(k, "v" + k);
            expected.put(k, "v" + k);
        }

        Collection<String> vals = map.values();

        assertEquals(EDGE_KEYS.length, vals.size());
        assertFalse(vals.isEmpty());
        assertEquals(new HashSet<>(expected.values()), new HashSet<>(vals));
        assertTrue(vals.contains("v42"));
        assertFalse(vals.contains("v43"));
        assertTrue(map.containsValue("v0"));

        long[] keys = keys(map);

        Arrays.sort(keys);

        long[] expKeys = EDGE_KEYS.clone();

        Arrays.sort(expKeys);

        assertTrue(Arrays.equals(expKeys, keys));

        // Non-boxing traversal.
        Map<Long, String> seen = new HashMap<>();

        map.forEach((k, v) -> assertNull(seen.put(k, v)));

        assertEquals(expected, seen);

        // Removal through the key iterator.
        PrimitiveIterator.OfLong keyIter = map.keyIterator();

        long first = keyIter.nextLong();

        keyIter.remove();

        assertFalse(map.containsKey(first));
        assertEquals(EDGE_KEYS.length - 1, map.size());

        // Removal through the values view.
        assertTrue(vals.remove("v42"));
        assertFalse(vals.remove("v42"));
        assertFalse(map.containsKey(42L));

        Iterator<String> valIter = vals.iterator();

        String firstVal = valIter.next();

        valIter.remove();

        assertFalse(map.containsValue(firstVal));
        assertEquals(EDGE_KEYS.length - 3, map.size());

        vals.removeIf(v -> v.startsWith("v-"));

        for (long k : keys(map))
            assertTrue(k >= 0);

        try {
            vals.add("x");

            throw new AssertionError("Values view must not support add.");
        }
        catch (UnsupportedOperationException expected0) {
            // No-op.
        }

        vals.clear();

        assertTrue(map.isEmpty());
        assertFalse(map.keyIterator().hasNext());
        assertFalse(vals.iterator().hasNext());

        // Exhausted iterators.
        try {
            map.keyIterator().nextLong();

            throw new AssertionError("NoSuchElementException expected.");
        }
        catch (java.util.NoSuchElementException expected0) {
            // No-op.
        }
    }

    /** */
    @Test
    public void testConcurrentDisjointKeys() throws Exception {
        ConcurrentLongHashMap<Integer> map = new ConcurrentLongHashMap<>();

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
        ConcurrentLongHashMap<Integer> map = new ConcurrentLongHashMap<>();

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

        for (long k : keys(map))
            assertEquals(Integer.valueOf(1), map.get(k));
    }

    /**
     * Iterators must return every entry that existed when they were created exactly once, even while the table is
     * resized several times underneath them.
     */
    @Test
    public void testIterationExactlyOnceDuringResize() throws Exception {
        for (int round = 0; round < 10; round++) {
            ConcurrentLongHashMap<Long> map = new ConcurrentLongHashMap<>(16);

            int init = 5_000;
            int added = 200_000;

            for (long k = 0; k < init; k++)
                map.put(k, k);

            CountDownLatch started = new CountDownLatch(1);

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
                started.await();

                for (long k = init; k < init + added; k++)
                    map.put(k, k);
            });

            PrimitiveIterator.OfLong keyIter = map.keyIterator();
            Iterator<Long> valIter = map.values().iterator();

            started.countDown();

            Set<Long> seenKeys = new HashSet<>();
            Set<Long> seenVals = new HashSet<>();

            while (keyIter.hasNext())
                assertTrue("Duplicate key from the iterator.", seenKeys.add(keyIter.nextLong()));

            while (valIter.hasNext())
                assertTrue("Duplicate value from the iterator.", seenVals.add(valIter.next()));

            fut.get();

            for (long k = 0; k < init; k++) {
                assertTrue("Key present at iterator creation was not returned: " + k, seenKeys.contains(k));
                assertTrue("Value present at iterator creation was not returned: " + k, seenVals.contains(k));
            }

            assertEquals(init + added, map.size());
        }
    }

    /**
     * @param map Map.
     * @return Keys of the map in iteration order.
     */
    private static long[] keys(ConcurrentLongHashMap<?> map) {
        long[] keys = new long[map.size()];

        int i = 0;

        for (PrimitiveIterator.OfLong it = map.keyIterator(); it.hasNext(); )
            keys[i++] = it.nextLong();

        assertEquals(keys.length, i);

        return keys;
    }
}
