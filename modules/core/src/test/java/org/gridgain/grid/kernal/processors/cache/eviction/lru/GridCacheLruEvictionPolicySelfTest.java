/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.eviction.lru;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.gridgain.grid.kernal.processors.cache.eviction.*;

import java.util.*;

/**
 * LRU Eviction test.
 */
@SuppressWarnings( {"TypeMayBeWeakened"})
public class GridCacheLruEvictionPolicySelfTest extends
    GridCacheEvictionAbstractTest<GridCacheLruEvictionPolicy<String, String>> {
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

            GridCacheLruEvictionPolicy<String, String> p = policy();

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
            GridCacheLruEvictionPolicy<String, String> p = policy();

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
            GridCacheLruEvictionPolicy<String, String> p = policy();

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
            GridCacheLruEvictionPolicy<String, String> p = policy();

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

            e1.setValue("val");

            MockEntry e2 = new MockEntry("2");

            MockEntry e3 = new MockEntry("3");

            e3.setValue("val");

            MockEntry e4 = new MockEntry("4");

            MockEntry e5 = new MockEntry("5");

            e5.setValue("val");

            GridCacheLruEvictionPolicy<String, String> p = policy();

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
        mode = GridCacheMode.LOCAL;
        syncCommit = true;
        plcMax = 100;

        Grid grid = startGrid();

        try {
            GridCache<Integer, Integer> cache = grid.cache(null);

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
            info("Current cache key size " + cache.size());
            info("Current cache entry set size " + cache.entrySet().size());

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
            info("Current cache key size " + cache.size());
            info("Current cache entry set size " + cache.entrySet().size());

            assert min >= plcMax : "Min cache size is too small: " + min;
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected GridCacheLruEvictionPolicy<String, String> createPolicy(int plcMax) {
        return new GridCacheLruEvictionPolicy<>(plcMax);
    }

    @Override protected GridCacheLruEvictionPolicy<String, String> createNearPolicy(int nearMax) {
        return new GridCacheLruEvictionPolicy<>(nearMax);
    }

    /** {@inheritDoc} */
    @Override protected void checkNearPolicies(int endNearPlcSize) {
        for (int i = 0; i < gridCnt; i++)
            for (GridCacheEntry<String, String> e : nearPolicy(i).queue())
                assert !e.isCached() : "Invalid near policy size: " + nearPolicy(i).queue();
    }

    /** {@inheritDoc} */
    @Override protected void checkPolicies(int plcMax) {
        for (int i = 0; i < gridCnt; i++)
            assert policy(i).queue().size() <= plcMax;
    }
}
