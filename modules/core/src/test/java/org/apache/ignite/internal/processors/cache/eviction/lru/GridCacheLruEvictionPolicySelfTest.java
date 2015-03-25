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

package org.apache.ignite.internal.processors.cache.eviction.lru;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.cache.eviction.lru.*;
import org.apache.ignite.internal.processors.cache.eviction.*;

import java.util.*;

/**
 * LRU Eviction test.
 */
@SuppressWarnings( {"TypeMayBeWeakened"})
public class GridCacheLruEvictionPolicySelfTest extends
    GridCacheEvictionAbstractTest<LruEvictionPolicy<String, String>> {
    /**
     * @throws Exception If failed.
     */
    public void testPolicy() throws Exception {
        startGrid();

        try {
            MockEntry e1 = new MockEntry("1", "1");
            MockEntry e2 = new MockEntry("2", "2");
            MockEntry e3 = new MockEntry("3", "3");
            MockEntry e4 = new MockEntry("4", "4");
            MockEntry e5 = new MockEntry("5", "5");

            LruEvictionPolicy<String, String> p = policy();

            p.setMaxSize(3);

            p.onEntryAccessed(false, e1);

            check(p.queue(), e1);

            p.onEntryAccessed(false, e2);

            check(p.queue(), e1, e2);

            p.onEntryAccessed(false, e3);

            check(p.queue(), e1, e2, e3);

            assert !e1.isEvicted();
            assert !e2.isEvicted();
            assert !e3.isEvicted();

            assertEquals(3, p.getCurrentSize());

            p.onEntryAccessed(false, e4);

            check(p.queue(), e2, e3, e4);

            assertEquals(3, p.getCurrentSize());

            assert e1.isEvicted();
            assert !e2.isEvicted();
            assert !e3.isEvicted();
            assert !e4.isEvicted();

            p.onEntryAccessed(false, e5);

            check(p.queue(), e3, e4, e5);

            assertEquals(3, p.getCurrentSize());

            assert e2.isEvicted();
            assert !e3.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(false, e1 = new MockEntry("1", "1"));

            check(p.queue(), e4, e5, e1);

            assertEquals(3, p.getCurrentSize());

            assert e3.isEvicted();
            assert !e1.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(false, e5);

            assertEquals(3, p.getCurrentSize());

            check(p.queue(), e4, e1, e5);

            assert !e1.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(false, e1);

            assertEquals(3, p.getCurrentSize());

            check(p.queue(), e4, e5, e1);

            assert !e1.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(false, e5);

            assertEquals(3, p.getCurrentSize());

            check(p.queue(), e4, e1, e5);

            assert !e1.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(true, e1);

            assertEquals(2, p.getCurrentSize());

            assert !e1.isEvicted();
            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(true, e4);

            assertEquals(1, p.getCurrentSize());

            assert !e4.isEvicted();
            assert !e5.isEvicted();

            p.onEntryAccessed(true, e5);

            assertEquals(0, p.getCurrentSize());

            assert !e5.isEvicted();

            info(p);
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMemory() throws Exception {
        startGrid();

        try {
            LruEvictionPolicy<String, String> p = policy();

            int max = 10;

            p.setMaxSize(max);

            int cnt = 11;

            for (int i = 0; i < cnt; i++)
                p.onEntryAccessed(false, new MockEntry(Integer.toString(i), Integer.toString(i)));

            info(p);

            assertEquals(max, p.getCurrentSize());
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMiddleAccess() throws Exception {
        startGrid();

        try {
            LruEvictionPolicy<String, String> p = policy();

            int max = 8;

            p.setMaxSize(max);

            MockEntry entry1 = new MockEntry("1", "1");
            MockEntry entry2 = new MockEntry("2", "2");
            MockEntry entry3 = new MockEntry("3", "3");

            p.onEntryAccessed(false, entry1);
            p.onEntryAccessed(false, entry2);
            p.onEntryAccessed(false, entry3);

            MockEntry[] freqUsed = new MockEntry[] {
                new MockEntry("4", "4"),
                new MockEntry("5", "5"),
                new MockEntry("6", "6"),
                new MockEntry("7", "7"),
                new MockEntry("8", "7")
            };

            for (MockEntry e : freqUsed)
                p.onEntryAccessed(false, e);

            for (MockEntry e : freqUsed)
                assert !e.isEvicted();

            int cnt = 1001;

            for (int i = 0; i < cnt; i++)
                p.onEntryAccessed(false, entry(freqUsed, i % freqUsed.length));

            info(p);

            assertEquals(max, p.getCurrentSize());
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandom() throws Exception {
        startGrid();

        try {
            LruEvictionPolicy<String, String> p = policy();

            int max = 10;

            p.setMaxSize(max);

            Random rand = new Random();

            int keys = 31;

            MockEntry[] lrus = new MockEntry[keys];

            for (int i = 0; i < lrus.length; i++)
                lrus[i] = new MockEntry(Integer.toString(i));

            int runs = 500000;

            for (int i = 0; i < runs; i++) {
                boolean rmv = rand.nextBoolean();

                int j = rand.nextInt(lrus.length);

                MockEntry e = entry(lrus, j);

                if (rmv)
                    lrus[j] = new MockEntry(Integer.toString(j));

                p.onEntryAccessed(rmv, e);
            }

            info(p);

            assert p.getCurrentSize() <= max;
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllowEmptyEntries() throws Exception {
        try {
            startGrid();

            MockEntry e1 = new MockEntry("1");

            MockEntry e2 = new MockEntry("2");

            MockEntry e3 = new MockEntry("3");

            MockEntry e4 = new MockEntry("4");

            MockEntry e5 = new MockEntry("5");

            LruEvictionPolicy<String, String> p = policy();

            p.setMaxSize(10);

            p.onEntryAccessed(false, e1);

            assertFalse(e1.isEvicted());

            p.onEntryAccessed(false, e2);

            assertFalse(e1.isEvicted());
            assertFalse(e2.isEvicted());

            p.onEntryAccessed(false, e3);

            assertFalse(e1.isEvicted());
            assertFalse(e3.isEvicted());

            p.onEntryAccessed(false, e4);

            assertFalse(e1.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e4.isEvicted());

            p.onEntryAccessed(false, e5);

            assertFalse(e1.isEvicted());
            assertFalse(e3.isEvicted());
            assertFalse(e5.isEvicted());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        mode = CacheMode.LOCAL;
        syncCommit = true;
        plcMax = 100;

        Ignite ignite = startGrid();

        try {
            IgniteCache<Integer, Integer> cache = ignite.cache(null);

            int cnt = 500;

            int min = Integer.MAX_VALUE;

            int minIdx = 0;

            for (int i = 0; i < cnt; i++) {
                cache.put(i, i);

                int cacheSize = cache.size();

                if (i > plcMax && cacheSize < min) {
                    min = cacheSize;
                    minIdx = i;
                }
            }

            assert min >= plcMax : "Min cache size is too small: " + min;

            info("Min cache size [min=" + min + ", idx=" + minIdx + ']');
            info("Current cache size " + cache.size());

            min = Integer.MAX_VALUE;

            minIdx = 0;

            // Touch.
            for (int i = cnt; --i > cnt - plcMax;) {
                cache.get(i);

                int cacheSize = cache.size();

                if (cacheSize < min) {
                    min = cacheSize;
                    minIdx = i;
                }
            }

            info("----");
            info("Min cache size [min=" + min + ", idx=" + minIdx + ']');
            info("Current cache size " + cache.size());

            assert min >= plcMax : "Min cache size is too small: " + min;
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected LruEvictionPolicy<String, String> createPolicy(int plcMax) {
        return new LruEvictionPolicy<>(plcMax);
    }

    @Override protected LruEvictionPolicy<String, String> createNearPolicy(int nearMax) {
        return new LruEvictionPolicy<>(nearMax);
    }

    /** {@inheritDoc} */
    @Override protected void checkNearPolicies(int endNearPlcSize) {
        for (int i = 0; i < gridCnt; i++)
            for (EvictableEntry<String, String> e : nearPolicy(i).queue())
                assert !e.isCached() : "Invalid near policy size: " + nearPolicy(i).queue();
    }

    /** {@inheritDoc} */
    @Override protected void checkPolicies(int plcMax) {
        for (int i = 0; i < gridCnt; i++)
            assert policy(i).queue().size() <= plcMax;
    }
}
