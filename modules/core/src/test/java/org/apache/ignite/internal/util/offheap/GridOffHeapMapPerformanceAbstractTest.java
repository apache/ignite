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
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests off-heap map.
 */
public abstract class GridOffHeapMapPerformanceAbstractTest extends GridCommonAbstractTest {
    /** Random. */
    private static final Random RAND = new Random();

    /** */
    protected static final int LOAD_CNT = 1024 * 1024;

    /** Sample map. */
    private static Map<String, T3<String, byte[], byte[]>> kvMap =
        new HashMap<>(LOAD_CNT);

    /** Unsafe map. */
    private GridOffHeapMap<String> map;

    /** */
    protected float load = 0.75f;

    /** */
    protected int initCap = 1024 * 1024 * 1024;

    /** */
    protected int concurrency = 16;

    /** */
    protected short lruStripes = 16;

    /** */
    protected GridOffHeapEvictListener evictClo;

    /** */
    protected long mem = 12L * 1024L * 1024L * 1024L;

    /** */
    protected long dur = 60 * 1000;//2 * 60 * 60 * 1000;

    /**
     *
     */
    protected GridOffHeapMapPerformanceAbstractTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        map = newMap();

        if (kvMap.isEmpty())
            for (int i = 0; i < LOAD_CNT; i++) {
                String k = string();
                String v = string();

                kvMap.put(k,  new T3<>(v, k.getBytes(), v.getBytes()));
            }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (map != null)
            map.destruct();
    }

    /**
     * @return New map.
     */
    protected abstract <K> GridOffHeapMap<K> newMap();

    /**
     * @param key Key.
     * @return Hash.
     */
    private int hash(Object key) {
        return hash(key.hashCode());
    }

    /**
     * @param h Hashcode.
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
     * Test plain hash map.
     */
    public void testHashMapPutRemove() {
        Map<String, String> map = new HashMap<>(LOAD_CNT);

        info("Starting standard HashMap performance test...");

        long cnt = 0;

        long start = System.currentTimeMillis();

        boolean rmv = false;

        boolean done = false;

        while (!done) {
            for (Map.Entry<String, T3<String, byte[], byte[]>> e : kvMap.entrySet()) {
                String key = e.getKey();
                T3<String, byte[], byte[]> t = e.getValue();

                try {
                    if (rmv)
                        map.remove(key);
                    else
                        map.put(key, t.get1());
                }
                catch (GridOffHeapOutOfMemoryException ex) {
                    error("Map put failed for count: " + cnt, ex);

                    throw ex;
                }

                if (cnt > 0 && cnt % 10000000 == 0) {
                    long cur = System.currentTimeMillis();

                    long throughput = cnt * 1000 / (cur - start);

                    X.println("Insert [cnt=" + cnt + ", ops/sec=" + throughput + ']');

                    if ((cur - start) > dur) {
                        done = true;

                        break;
                    }
                }

                cnt++;
            }

            rmv = !rmv;
        }
    }

    /**
     *
     */
    public void testInsertRemoveLoad() {
        info("Starting insert performance test...");

        long cnt = 0;

        long start = System.currentTimeMillis();

        boolean rmv = false;

        boolean done = false;

        while (!done) {
            for (Map.Entry<String, T3<String, byte[], byte[]>> e : kvMap.entrySet()) {
                String key = e.getKey();
                T3<String, byte[], byte[]> t = e.getValue();

                try {
                    if (rmv)
                        map.remove(hash(key), t.get2());
                    else
                        map.insert(hash(key), t.get2(), t.get3());
                }
                catch (GridOffHeapOutOfMemoryException ex) {
                    error("Map put failed for count: " + cnt, ex);

                    throw ex;
                }

                if (cnt > 0 && cnt % 10000000 == 0) {
                    long cur = System.currentTimeMillis();

                    long throughput = cnt * 1000 / (cur - start);

                    X.println("Insert [cnt=" + cnt + ", ops/sec=" + throughput + ']');

                    if ((cur - start) > dur) {
                        done = true;

                        break;
                    }
                }

                cnt++;
            }

            rmv = !rmv;
        }
    }


    /**
     *
     */
    public void testPutRemoveLoad() {
        info("Starting put performance test...");

        long cnt = 0;

        long start = System.currentTimeMillis();

        boolean rmv = false;

        boolean done = false;

        while (!done) {
            for (Map.Entry<String, T3<String, byte[], byte[]>> e : kvMap.entrySet()) {
                String key = e.getKey();
                T3<String, byte[], byte[]> t = e.getValue();

                try {
                    if (rmv)
                        map.remove(hash(key), t.get2());
                    else
                        map.put(hash(key), t.get2(), t.get3());
                }
                catch (GridOffHeapOutOfMemoryException ex) {
                    error("Map put failed for count: " + cnt, ex);

                    throw ex;
                }

                if (cnt > 0 && cnt % 10000000 == 0) {
                    long cur = System.currentTimeMillis();

                    long throughput = cnt * 1000 / (cur - start);

                    X.println("Put [cnt=" + cnt + ", ops/sec=" + throughput + ']');

                    if ((cur - start) > dur) {
                        done = true;

                        break;
                    }
                }

                cnt++;
            }

            rmv = cnt % 3 == 0;
        }
    }
}