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

package org.apache.ignite.internal.util.offheap;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests off-heap map.
 */
public abstract class GridOffHeapPartitionedMapAbstractSelfTest extends GridCommonAbstractTest {
    /** Random. */
    private static final Random RAND = new Random();

    /** Unsafe map. */
    private GridOffHeapPartitionedMap map;

    /** */
    protected float load = 0.75f;

    /** */
    protected int initCap = 100;

    /** */
    protected int concurrency = 16;

    /** */
    protected short lruStripes = 16;

    /** */
    protected GridOffHeapEvictListener evictLsnr;

    /** */
    protected long mem = 20L * 1024 * 1024;

    /** */
    protected int loadCnt = 100000;

    /** */
    protected int parts = 17;

    /**
     *
     */
    protected GridOffHeapPartitionedMapAbstractSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        //resetLog4j(Level.INFO, true, false, "");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (map != null)
            map.destruct();
    }

    /**
     * @return New map.
     */
    protected abstract GridOffHeapPartitionedMap newMap();

    /**
     * @param key Key.
     * @return Hash.
     */
    private int hash(Object key) {
        return hash(key.hashCode());
    }

    /**
     * @param h Hash code.
     * @return Hash.
     */
    private int hash(int h) {
        // Apply base step of MurmurHash; see http://code.google.com/p/smhasher/
        // Despite two multiplies, this is often faster than others
        // with comparable bit-spread properties.
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;

        return (h >>> 16) ^ h;
    }

    /**
     *
     * @return New Object.
     */
    private String string() {
        String key = "";

        for (int i = 0; i < 3; i++)
            key += RAND.nextLong();

        return key;
    }

    /**
     * @param len Length of byte array.
     * @return Random byte array.
     */
    private byte[] bytes(int len) {
        byte[] b = new byte[len];

        RAND.nextBytes(b);

        return b;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInsert() throws Exception {
        map = newMap();

        for (int p = 0; p < parts; p++) {
            for (int i = 0; i < 10; i++) {
                String key = string();
                String val = string();

                map.insert(p, hash(key), key.getBytes(), val.getBytes());

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertEquals(val, new String(map.get(p, hash(key), key.getBytes())));
                assertEquals(10 * p + (i + 1), map.size());
            }
        }

        assertEquals(parts * 10, map.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRehash() throws Exception {
        initCap = 10;

        map = newMap();

        for (int p = 0; p < parts; p++) {
            Map<String, String> kv = new HashMap<>(10);

            for (int i = 0; i < 10; i++)
                kv.put(string(), string());

            for (Map.Entry<String, String> e : kv.entrySet()) {
                String key = e.getKey();
                String val = e.getValue();

                map.insert(p, hash(key), key.getBytes(), val.getBytes());

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertEquals(val, new String(map.get(p, hash(key), key.getBytes())));
            }

            for (Map.Entry<String, String> e : kv.entrySet()) {
                String key = e.getKey();
                String val = e.getValue();

                byte[] valBytes = map.get(p, hash(key), key.getBytes());

                assertNotNull(valBytes);
                assertEquals(val, new String(valBytes));
            }
        }

        assertEquals(parts * 10, map.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPointerAfterRehash() throws Exception {
        initCap = 10;

        map = newMap();

        Map<String, String> kv = new HashMap<>(1000);
        Map<String, Long> ptrs = new HashMap<>(1000);

        for (int i = 0; i < 1000; i++)
            kv.put(string(), string());

        for (Map.Entry<String, String> e : kv.entrySet()) {
            String key = e.getKey();
            String val = e.getValue();

            assertTrue(map.put(1, hash(key), key.getBytes(), val.getBytes()));

            IgniteBiTuple<Long, Integer> ptr = map.valuePointer(1, hash(key), key.getBytes());

            assertNotNull(ptr);

            assertEquals((Integer)val.getBytes().length, ptr.get2());

            assertFalse(map.put(1, hash(key), key.getBytes(), val.getBytes()));

            ptrs.put(key, ptr.get1());
        }

        for (Map.Entry<String, String> e : kv.entrySet()) {
            String key = e.getKey();

            IgniteBiTuple<Long, Integer> ptr = map.valuePointer(1, hash(key), key.getBytes());

            assertNotNull(ptr);

            assertEquals(ptrs.get(key), ptr.get1());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutRandomKeys() throws Exception {
        map = newMap();

        AffinityFunction aff = new RendezvousAffinityFunction(parts, null);

        getTestResources().inject(aff);

        GridByteArrayWrapper[] keys = new GridByteArrayWrapper[512];
        Random rnd = new Random();

        for (int i = 0; i < keys.length; i++) {
            byte[] key = new byte[rnd.nextInt(64) + 1];
            rnd.nextBytes(key);

            keys[i] = new GridByteArrayWrapper(key);
        }

        for (int i = 0; i < 10000; i++) {
            int idx = rnd.nextInt(keys.length);

            GridByteArrayWrapper key = keys[idx];

            map.put(aff.partition(key), key.hashCode(), key.array(), new byte[64]);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGet() throws Exception {
        map = newMap();

        for (int p = 0; p < parts; p++) {
            for (int i = 0; i < 10; i++) {
                String key = string();
                String val = string();

                map.insert(p, hash(key), key.getBytes(), val.getBytes());

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertEquals(val, new String(map.get(p, hash(key), key.getBytes())));
                assertEquals(10 * p + (i + 1), map.size());
            }
        }

        assertEquals(parts * 10, map.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPut1() throws Exception {
        map = newMap();

        for (int p = 0; p < parts; p++) {
            for (int i = 0; i < 10; i++) {
                String key = string();
                String val = string();

                assertTrue(map.put(p, hash(key), key.getBytes(), val.getBytes()));

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertEquals(val, new String(map.get(p, hash(key), key.getBytes())));
                assertEquals(10 * p + i + 1, map.size());
            }
        }

        assertEquals(parts * 10, map.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPut2() throws Exception {
        map = newMap();

        for (int p = 0; p < parts; p++) {
            for (int i = 0; i < 10; i++) {
                String key = string();

                String val1 = string();
                String val2 = string();

                assertTrue(map.put(p, hash(key), key.getBytes(), val1.getBytes()));

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertEquals(val1, new String(map.get(p, hash(key), key.getBytes())));
                assertEquals(10 * p + i + 1, map.size());

                assertFalse(map.put(p, hash(key), key.getBytes(), val2.getBytes()));

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertEquals(val2, new String(map.get(p, hash(key), key.getBytes())));
                assertEquals(10 * p + i + 1, map.size());
            }
        }

        assertEquals(parts * 10, map.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemove() throws Exception {
        map = newMap();

        for (int p = 0; p < parts; p++) {
            for (int i = 0; i < 10; i++) {
                String key = string();
                String val = string();

                assertTrue(map.put(p, hash(key), key.getBytes(), val.getBytes()));

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertNotNull(map.get(p, hash(key), key.getBytes()));
                assertEquals(new String(map.get(p, hash(key), key.getBytes())), val);
                assertEquals(1, map.size());

                byte[] val2 = map.remove(p, hash(key), key.getBytes());

                assertNotNull(val2);
                assertEquals(val, new String(val2));
                assertFalse(map.contains(p, hash(key), key.getBytes()));
                assertNull(map.get(p, hash(key), key.getBytes()));
                assertEquals(0, map.size());
            }
        }

        assertEquals(0, map.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemovex() throws Exception {
        map = newMap();

        for (int p = 0; p < parts; p++) {
            for (int i = 0; i < 10; i++) {
                String key = string();
                String val = string();

                assertTrue(map.put(p, hash(key), key.getBytes(), val.getBytes()));

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertNotNull(map.get(p, hash(key), key.getBytes()));
                assertEquals(new String(map.get(p, hash(key), key.getBytes())), val);
                assertEquals(1, map.size());

                boolean rmvd = map.removex(p, hash(key), key.getBytes());

                assertTrue(rmvd);
                assertFalse(map.contains(p, hash(key), key.getBytes()));
                assertNull(map.get(p, hash(key), key.getBytes()));
                assertEquals(0, map.size());
            }
        }

        assertEquals(0, map.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIterator() throws Exception {
        initCap = 10;

        map = newMap();

        final AtomicInteger rehashes = new AtomicInteger();
        final AtomicInteger releases = new AtomicInteger();

        map.eventListener(new GridOffHeapEventListener() {
            @Override public void onEvent(GridOffHeapEvent evt) {
                switch (evt) {
                    case REHASH:
                        rehashes.incrementAndGet();
                        break;
                    case RELEASE:
                        releases.incrementAndGet();
                        break;

                    default: // No-op.
                }
            }
        });

        int max = 1024;

        Map<String, String> m = new HashMap<>(max * parts);

        for (int p = 0; p < parts; p++) {
            for (int i = 0; i < max; i++) {
                String key = string();
                String val = string();

                // info("Storing [i=" + i + ", key=" + key + ", val=" + val + ']');

                assertTrue(map.put(p, hash(key), key.getBytes(), val.getBytes()));

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertNotNull(map.get(p, hash(key), key.getBytes()));
                assertEquals(new String(map.get(p, hash(key), key.getBytes())), val);

                m.put(key, val);
            }
        }

        int cnt = 0;

        try (GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> it = map.iterator()) {
            while (it.hasNext()) {
                IgniteBiTuple<byte[], byte[]> t = it.next();

                String k = new String(t.get1());
                String v = new String(t.get2());

                // info("Entry [k=" + k + ", v=" + v + ']');

                assertEquals(m.get(k), v);

                cnt++;
            }
        }

        assertEquals(map.size(), cnt);
        assertEquals(max * parts, map.size());

        info("Stats [size=" + map.size() + ", rehashes=" + rehashes + ", releases=" + releases + ']');

        assertTrue(rehashes.get() > 0);
        assertEquals(rehashes.get(), releases.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIteratorMultithreaded() throws Exception {
        initCap = 10;

        map = newMap();

        final AtomicInteger rehashes = new AtomicInteger();
        final AtomicInteger releases = new AtomicInteger();

        map.eventListener(new GridOffHeapEventListener() {
            @Override public void onEvent(GridOffHeapEvent evt) {
                switch (evt) {
                    case REHASH:
                        rehashes.incrementAndGet();
                        break;
                    case RELEASE:
                        releases.incrementAndGet();
                        break;

                    default: // No-op.
                }
            }
        });

        final int max = 1024;

        final ConcurrentMap<String, String> m = new ConcurrentHashMap<>(max * parts);

        final AtomicInteger part = new AtomicInteger();

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int p = part.getAndIncrement();

                for (int i = 0; i < max; i++) {
                    String key = string();
                    String val = string();

                    // info("Storing [i=" + i + ", key=" + key + ", val=" + val + ']');

                    String old = m.putIfAbsent(key, val);

                    if (old != null)
                        val = old;

                    assertTrue(map.put(p, hash(key), key.getBytes(), val.getBytes()));
                    assertTrue(map.contains(p, hash(key), key.getBytes()));
                    assertNotNull(map.get(p, hash(key), key.getBytes()));

                    assertEquals(new String(map.get(p, hash(key), key.getBytes())), val);
                }

                try (GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> it = map.iterator()) {
                    while (it.hasNext()) {
                        IgniteBiTuple<byte[], byte[]> t = it.next();

                        String k = new String(t.get1());
                        String v = new String(t.get2());

                        // info("Entry [k=" + k + ", v=" + v + ']');

                        assertEquals(m.get(k), v);
                    }
                }

                return null;
            }
        }, parts);

        int cnt = 0;

        try (GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> it = map.iterator()) {
            while (it.hasNext()) {
                IgniteBiTuple<byte[], byte[]> t = it.next();

                String k = new String(t.get1());
                String v = new String(t.get2());

                // info("Entry [k=" + k + ", v=" + v + ']');

                assertEquals(m.get(k), v);

                cnt++;
            }
        }

        assertEquals(map.size(), cnt);
        assertEquals(max * parts, map.size());

        info("Stats [size=" + map.size() + ", rehashes=" + rehashes + ", releases=" + releases + ']');

        assertTrue(rehashes.get() > 0);
        assertEquals(rehashes.get(), releases.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIteratorRemoveMultithreaded() throws Exception {
        initCap = 10;

        map = newMap();

        final int max = 1024;

        final Map<String, String> m = new ConcurrentHashMap<>(max * parts);

        for (int i = 0; i < max; i++) {
            String key = string();
            String val = string();

            m.put(key, val);

            map.put(0, hash(key), key.getBytes(), val.getBytes());
        }

        final AtomicBoolean running = new AtomicBoolean(true);

        IgniteInternalFuture<?> iterFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    while (running.get()) {
                        try (GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> it = map.iterator()) {
                            while (it.hasNext()) {
                                IgniteBiTuple<byte[], byte[]> tup = it.next();

                                String key = new String(tup.get1());
                                String val = new String(tup.get2());

                                String exp = m.get(key);

                                assertEquals(exp, val);
                            }
                        }
                    }
                }
                catch (IgniteCheckedException e) {
                    fail("Unexpected exception caught: " + e);
                }
            }
        }, 1);

        for (String key : m.keySet())
            map.remove(0, hash(key), key.getBytes());

        running.set(false);

        iterFut.get();

        map.destruct();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionIterator() throws Exception {
        initCap = 10;

        map = newMap();

        final AtomicInteger rehashes = new AtomicInteger();
        final AtomicInteger releases = new AtomicInteger();

        map.eventListener(new GridOffHeapEventListener() {
            @Override public void onEvent(GridOffHeapEvent evt) {
                switch (evt) {
                    case REHASH:
                        rehashes.incrementAndGet();
                        break;
                    case RELEASE:
                        releases.incrementAndGet();
                        break;

                    default: // No-op.
                }
            }
        });

        int max = 1024;

        for (int p = 0; p < parts; p++) {
            Map<String, String> m = new HashMap<>(max);

            for (int i = 0; i < max; i++) {
                String key = string();
                String val = string();

                // info("Storing [i=" + i + ", key=" + key + ", val=" + val + ']');

                assertTrue(map.put(p, hash(key), key.getBytes(), val.getBytes()));

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertNotNull(map.get(p, hash(key), key.getBytes()));
                assertEquals(new String(map.get(p, hash(key), key.getBytes())), val);

                m.put(key, val);

                int cnt = 0;

                try (GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> it = map.iterator(p)) {
                    while (it.hasNext()) {
                        IgniteBiTuple<byte[], byte[]> t = it.next();

                        String k = new String(t.get1());
                        String v = new String(t.get2());

                        // info("Entry [k=" + k + ", v=" + v + ']');

                        assertEquals(m.get(k), v);

                        cnt++;
                    }
                }

                assertEquals(map.size(), p * max + cnt);
            }
        }

        assertEquals(max * parts, map.size());

        info("Stats [size=" + map.size() + ", rehashes=" + rehashes + ", releases=" + releases + ']');

        assertTrue(rehashes.get() > 0);
        assertEquals(rehashes.get(), releases.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionIteratorMultithreaded() throws Exception {
        initCap = 10;

        map = newMap();

        final AtomicInteger rehashes = new AtomicInteger();
        final AtomicInteger releases = new AtomicInteger();

        map.eventListener(new GridOffHeapEventListener() {
            @Override public void onEvent(GridOffHeapEvent evt) {
                switch (evt) {
                    case REHASH:
                        rehashes.incrementAndGet();
                        break;
                    case RELEASE:
                        releases.incrementAndGet();
                        break;

                    default: // No-op.
                }
            }
        });

        final int max = 64;

        int threads = 5;

        final Map<String, String> m = new ConcurrentHashMap<>(max);

        multithreaded(new Callable() {
            @Override public Object call() throws Exception {
                for (int p = 0; p < parts; p++) {
                    for (int i = 0; i < max; i++) {
                        String key = string();
                        String val = string();

                        // info("Storing [i=" + i + ", key=" + key + ", val=" + val + ']');

                        m.put(key, val);

                        assertTrue(map.put(p, hash(key), key.getBytes(), val.getBytes()));
                        assertTrue(map.contains(p, hash(key), key.getBytes()));
                        assertNotNull(map.get(p, hash(key), key.getBytes()));
                        assertEquals(new String(map.get(p, hash(key), key.getBytes())), val);

                        try (GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> it = map.iterator(p)) {
                            while (it.hasNext()) {
                                IgniteBiTuple<byte[], byte[]> t = it.next();

                                String k = new String(t.get1());
                                String v = new String(t.get2());

                                // info("Entry [k=" + k + ", v=" + v + ']');

                                assertEquals(m.get(k), v);
                            }
                        }
                    }
                }

                return null;
            }
        }, threads);

        assertEquals(max * threads * parts, map.size());

        info("Stats [size=" + map.size() + ", rehashes=" + rehashes + ", releases=" + releases + ']');

        assertTrue(rehashes.get() > 0);
        assertEquals(rehashes.get(), releases.get());
    }

    /**
     *
     */
    @Test
    public void testInsertLoad() {
        mem = 0; // Disable LRU.

        loadCnt = 50000;

        map = newMap();

        int cnt = 0;

        for (int p = 0; p < parts; p++) {
            Map<String, String> m = new HashMap<>();

            for (int i = 0; i < loadCnt; i++)
                m.put(string(), string());

            for (Map.Entry<String, String> e : m.entrySet()) {
                String key = e.getKey();
                String val = e.getValue();

                try {
                    map.insert(p, hash(key), key.getBytes(), val.getBytes());
                }
                catch (GridOffHeapOutOfMemoryException ex) {
                    error("Map put failed for count: " + cnt, ex);

                    throw ex;
                }

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertNotNull(map.get(p, hash(key), key.getBytes()));
                assertEquals(new String(map.get(p, hash(key), key.getBytes())), val);
                assertEquals(++cnt, map.size());
            }
        }
    }


    /**
     *
     */
    @Test
    public void testPutLoad() {
        mem = 0; // Disable LRU.

        loadCnt = 50000;

        map = newMap();

        int cnt = 0;

        for (int p = 0; p < parts; p++) {
            Map<String, String> m = new HashMap<>();

            for (int i = 0; i < loadCnt; i++)
                m.put(string(), string());

            for (Map.Entry<String, String> e : m.entrySet()) {
                String key = e.getKey();
                String val = e.getValue();

                try {
                    assertTrue(map.put(p, hash(key), key.getBytes(), val.getBytes()));
                    assertTrue(map.contains(p, hash(key), key.getBytes()));
                    assertFalse(map.put(p, hash(key), key.getBytes(), val.getBytes()));
                }
                catch (GridOffHeapOutOfMemoryException ex) {
                    error("Map put failed for count: " + cnt, ex);

                    throw ex;
                }

                assertTrue(map.contains(p, hash(key), key.getBytes()));
                assertNotNull(map.get(p, hash(key), key.getBytes()));
                assertEquals(new String(map.get(p, hash(key), key.getBytes())), val);
                assertEquals(++cnt, map.size());
            }
        }
    }

    /**
     *
     */
    @Test
    public void testLru1() {
        lruStripes = 1;
        mem = 10;

        final AtomicInteger evictCnt = new AtomicInteger();

        evictLsnr = new GridOffHeapEvictListener() {
            @Override public void onEvict(int part, int hash, byte[] k, byte[] v) {
                String key = new String(k);

                info("Evicted key: " + key);

                evictCnt.incrementAndGet();
            }

            @Override public boolean removeEvicted() {
                return true;
            }
        };

        map = newMap();

        for (int p = 0; p < parts; p++) {
            for (int i = 0; i < 10; i++) {
                String key = string();

                byte[] keyBytes = key.getBytes();
                byte[] valBytes = bytes(100);

                map.insert(p, hash(key), keyBytes, valBytes);

                info("Evicted: " + evictCnt);

                assertEquals(1, evictCnt.get());
                assertEquals(0, map.size());

                assertTrue(evictCnt.compareAndSet(1, 0));
            }
        }
    }

    /**
     *
     */
    @Test
    public void testLru2() {
        mem = 1000 + 64 * 16 * parts; // Add segment size.

        lruStripes = 6;
        concurrency = 8;

        final AtomicInteger evictCnt = new AtomicInteger();

        for (int p = 0; p < parts; p++) {
            evictLsnr = new GridOffHeapEvictListener() {
                @Override public void onEvict(int part, int hash, byte[] k, byte[] v) {
                    evictCnt.incrementAndGet();
                }

                @Override public boolean removeEvicted() {
                    return true;
                }
            };

            map = newMap();

            for (int i = 0; i < 10000; i++) {
                String key = string();

                byte[] keyBytes = key.getBytes();
                byte[] valBytes = bytes(100);

                map.insert(p, hash(key), keyBytes, valBytes);
            }

            assertTrue(evictCnt.get() > 10 * parts);
            assertTrue("Invalid map free size [size=" + map.freeSize() + ", evictCnt=" + evictCnt + ']',
                map.freeSize() >= 0);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLruMultithreaded() throws Exception {
        mem = 1000 + 64 * 16 * parts; // Add segment size.

        lruStripes = 3;
        concurrency = 8;

        final AtomicInteger evictCnt = new AtomicInteger();

        final AtomicInteger cnt = new AtomicInteger();

        evictLsnr = new GridOffHeapEvictListener() {
            @Override public void onEvict(int part, int hash, byte[] k, byte[] v) {
                evictCnt.incrementAndGet();
            }

            @Override public boolean removeEvicted() {
                return true;
            }
        };

        map = newMap();

        assertEquals(0, map.allocatedSize());

        multithreaded(new Runnable() {
            @Override public void run() {
                for (int p = 0; p < parts; p++) {
                    for (int i = 0; i < 10000; i++) {
                        String key = string();

                        byte[] keyBytes = key.getBytes();
                        byte[] valBytes = bytes(100);

                        assert !map.contains(p, hash(key), keyBytes);

                        map.insert(p, hash(key), keyBytes, valBytes);

                        int n;

                        if ((n = cnt.incrementAndGet()) % 100000 == 0)
                            info("Inserted entries: " + n);
                    }
                }
            }
        }, 10);

        assert map.allocatedSize() <= mem;

        info("Map stats [evicted=" + evictCnt + ", size=" + map.size() + ", allocated=" + map.allocatedSize() +
            ", freeSize=" + map.freeSize() + ']');

        assertTrue("Invalid map free size [size=" + map.freeSize() + ", evictCnt=" + evictCnt + ']',
            map.freeSize() >= 0);
    }

    /**
     *
     */
    @SuppressWarnings("TooBroadScope")
    @Test
    public void testValuePointerEvict() {
        mem = 90;

        final GridTuple<String> evicted = new GridTuple<>();

        evictLsnr = new GridOffHeapEvictListener() {
            @Override public void onEvict(int part, int hash, byte[] k, byte[] v) {
                String key = new String(k);

                evicted.set(key);
            }

            @Override public boolean removeEvicted() {
                return true;
            }
        };

        map = newMap();

        final String k1 = "k1";
        final String k2 = "k2";

        map.put(1, k1.hashCode(), k1.getBytes(), bytes(20));

        assertNull(evicted.get());

        assertNotNull(map.valuePointer(1, k1.hashCode(), k1.getBytes()));

        assertNull(evicted.get());

        map.put(1, k2.hashCode(), k2.getBytes(), bytes(20));

        assertEquals(k2, evicted.get());

        evicted.set(null);

        assertTrue(map.contains(1, k1.hashCode(), k1.getBytes()));

        map.put(1, k1.hashCode(), k1.getBytes(), bytes(20));

        assertNull(evicted.get());

        map.put(1, k2.hashCode(), k2.getBytes(), bytes(20));

        assertEquals(k1, evicted.get());

        assertNull(map.valuePointer(1, k1.hashCode(), k1.getBytes()));

        evicted.set(null);

        assertNotNull(map.valuePointer(1, k2.hashCode(), k2.getBytes()));

        assertNull(evicted.get());

        map.put(1, k1.hashCode(), k1.getBytes(), bytes(21));

        assertEquals(k1, evicted.get());

        evicted.set(null);

        map.put(1, k2.hashCode(), k2.getBytes(), bytes(21));
    }

    /**
     *
     */
    @SuppressWarnings("TooBroadScope")
    @Test
    public void testValuePointerEnableEviction() {
        mem = 90;

        final GridTuple<String> evicted = new GridTuple<>();

        evictLsnr = new GridOffHeapEvictListener() {
            @Override public void onEvict(int part, int hash, byte[] k, byte[] v) {
                String key = new String(k);

                evicted.set(key);
            }

            @Override public boolean removeEvicted() {
                return true;
            }
        };

        map = newMap();

        final String k1 = "k1";
        final String k2 = "k2";

        map.put(1, k1.hashCode(), k1.getBytes(), bytes(20));

        assertNull(evicted.get());

        assertNotNull(map.valuePointer(1, k1.hashCode(), k1.getBytes()));

        assertNull(evicted.get());

        map.put(1, k2.hashCode(), k2.getBytes(), bytes(20));

        assertEquals(k2, evicted.get());

        evicted.set(null);

        assertTrue(map.contains(1, k1.hashCode(), k1.getBytes()));

        map.enableEviction(1, k1.hashCode(), k1.getBytes());

        assertNull(evicted.get());

        map.put(1, k2.hashCode(), k2.getBytes(), bytes(20));

        assertEquals(k1, evicted.get());

        assertNull(map.valuePointer(1, k1.hashCode(), k1.getBytes()));

        evicted.set(null);

        assertNotNull(map.valuePointer(1, k2.hashCode(), k2.getBytes()));

        assertNull(evicted.get());

        map.put(1, k1.hashCode(), k1.getBytes(), bytes(21));

        assertEquals(k1, evicted.get());

        evicted.set(null);

        map.put(1, k2.hashCode(), k2.getBytes(), bytes(21));
    }

    /**
     *
     */
    @Test
    public void testValuePointerRemove() {
        map = newMap();

        final String k = "k1";

        map.put(1, k.hashCode(), k.getBytes(), bytes(10));

        assertNotNull(map.valuePointer(1, k.hashCode(), k.getBytes()));

        map.remove(1, k.hashCode(), k.getBytes());

        assertNull(map.valuePointer(1, k.hashCode(), k.getBytes()));
    }
}
