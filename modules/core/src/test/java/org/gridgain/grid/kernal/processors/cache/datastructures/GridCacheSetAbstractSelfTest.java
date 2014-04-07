/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.testframework.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Cache set tests.
 */
public abstract class GridCacheSetAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final String SET_NAME = "testSet";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            GridKernal grid = (GridKernal)grid(i);

            GridCacheDataStructuresManager ds = grid.internalCache(null).context().dataStructures();

            Map map = GridTestUtils.getFieldValue(ds, "setHndMap");

            assertEquals("Handler not removed for grid " + i, 0, map.size());

            map = GridTestUtils.getFieldValue(ds, "locSetIterMap");

            assertEquals("Iterator not removed for grid " + i, 0, map.size());
        }

        cache().dataStructures().removeSet(SET_NAME);

        assertNull(cache().dataStructures().set(SET_NAME, false, false));

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        if (cacheMode() == PARTITIONED) {
            GridCacheConfiguration ccfg1 = cacheConfiguration(gridName);

            GridCacheConfiguration ccfg2 = cacheConfiguration(gridName);

            ccfg2.setName("noBackupsCache");
            ccfg2.setBackups(0);

            cfg.setCacheConfiguration(ccfg1, ccfg2);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setPreloadMode(SYNC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateRemove() throws Exception {
        testCreateRemove(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateRemoveCollocated() throws Exception {
        testCreateRemove(true);
    }

    /**
     * @param collocated Collocation flag.
     * @throws Exception If failed.
     */
    private void testCreateRemove(boolean collocated) throws Exception {
        for (int i = 0; i < gridCount(); i++)
            assertNull(cache(i).dataStructures().set(SET_NAME, collocated, false));

        Set<Integer> set = cache().dataStructures().set(SET_NAME, collocated, true);

        assertNotNull(set);
        assertTrue(set.isEmpty());
        assertEquals(0, set.size());

        for (int i = 0; i < gridCount(); i++)
            assertNotNull(cache(i).dataStructures().set(SET_NAME, collocated, false));

        assertTrue(cache().dataStructures().removeSet(SET_NAME));

        for (int i = 0; i < gridCount(); i++) {
            assertNull(cache(i).dataStructures().set(SET_NAME, collocated, false));
            assertFalse(cache(i).dataStructures().removeSet(SET_NAME));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testApi() throws Exception {
        testApi(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testApiCollocated() throws Exception {
        testApi(true);
    }

    /**
     * @param collocated Collocation flag.
     * @throws Exception If failed.
     */
    private void testApi(boolean collocated) throws Exception {
        assertNotNull(cache().dataStructures().set(SET_NAME, collocated, true));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertFalse(set.contains(1));
            assertEquals(0, set.size());
            assertTrue(set.isEmpty());
        }

        // Add, isEmpty.

        assertTrue(cache().dataStructures().set(SET_NAME, collocated, false).add(1));

        for (int i = 0; i < gridCount(); i++) {
            assertEquals(0, cache(i).size());

            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertEquals(1, set.size());
            assertFalse(set.isEmpty());
            assertTrue(set.contains(1));

            assertFalse(set.add(1));

            assertFalse(set.contains(100));
        }

        // Remove.

        assertTrue(cache().dataStructures().set(SET_NAME, collocated, true).remove(1));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertEquals(0, set.size());
            assertTrue(set.isEmpty());

            assertFalse(set.contains(1));

            assertFalse(set.remove(1));
        }

        // Contains all.

        Collection<Integer> col1 = new ArrayList<>();
        Collection<Integer> col2 = new ArrayList<>();

        final int ITEMS = 100;

        for (int i = 0; i < ITEMS; i++) {
            assertTrue(cache(i % gridCount()).dataStructures().set(SET_NAME, collocated, false).add(i));

            col1.add(i);
            col2.add(i);
        }

        col2.add(ITEMS);

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertEquals(ITEMS, set.size());
            assertTrue(set.containsAll(col1));
            assertFalse(set.containsAll(col2));
        }

        // To array.

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertArrayContent(set.toArray(), ITEMS);
            assertArrayContent(set.toArray(new Integer[ITEMS]), ITEMS);
        }

        // Remove all.

        Collection<Integer> rmvCol = new ArrayList<>();

        for (int i = ITEMS - 10; i < ITEMS; i++)
            rmvCol.add(i);

        assertTrue(cache().dataStructures().set(SET_NAME, collocated, false).removeAll(rmvCol));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertFalse(set.removeAll(rmvCol));

            for (Integer val : rmvCol)
                assertFalse(set.contains(val));

            assertArrayContent(set.toArray(), ITEMS - 10);
            assertArrayContent(set.toArray(new Integer[ITEMS - 10]), ITEMS - 10);
        }

        // Add all.

        assertTrue(cache().dataStructures().set(SET_NAME, collocated, false).addAll(rmvCol));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertEquals(ITEMS, set.size());

            assertFalse(set.addAll(rmvCol));

            for (Integer val : rmvCol)
                assertTrue(set.contains(val));
        }

        // Retain all.

        assertTrue(cache().dataStructures().set(SET_NAME, collocated, false).retainAll(rmvCol));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertEquals(rmvCol.size(), set.size());

            assertFalse(set.retainAll(rmvCol));

            for (int val = 0; val < 10; val++)
                assertFalse(set.contains(val));

            for (int val : rmvCol)
                assertTrue(set.contains(val));
        }

        // Clear.

        cache().dataStructures().set(SET_NAME, collocated, false).clear();

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertEquals(0, set.size());
            assertTrue(set.isEmpty());
            assertFalse(set.contains(0));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIterator() throws Exception {
        testIterator(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorCollocated() throws Exception {
        testIterator(true);
    }

    /**
     * @param collocated Collocation flag.
     * @throws Exception If failed.
     */
    private void testIterator(boolean collocated) throws Exception {
        Set<Integer> set0 = cache().dataStructures().set(SET_NAME, collocated, true);

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertFalse(set.iterator().hasNext());
        }

        int cnt = 0;

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            for (int j = 0; j < 100; j++)
                assertTrue(set.add(cnt++));
        }

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertSetContent(set, cnt);
        }

        // Try to do not use hasNext.

        Collection<Integer> data = new HashSet<>(cnt);

        Iterator<Integer> iter = set0.iterator();

        for (int i = 0; i < cnt; i++)
            assertTrue(data.add(iter.next()));

        assertFalse(iter.hasNext());

        assertEquals(cnt, data.size());

        for (int i = 0; i < cnt; i++)
            assertTrue(data.contains(i));

        set0.clear();

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertFalse(set.iterator().hasNext());
        }

        for (int i = 0; i < 10; i++)
            assertTrue(set0.add(i));

        iter = set0.iterator();

        while (iter.hasNext()) {
            Integer val = iter.next();

            if (val % 2 == 0)
                iter.remove();
        }

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertEquals(i % 2 != 0, set.contains(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoinsAndLeaves() throws Exception {
        testNodeJoinsAndLeaves(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoinsAndLeavesCollocated() throws Exception {
        testNodeJoinsAndLeaves(true);
    }

    /**
     * @param collocated Collocation flag.
     * @throws Exception If failed.
     */
    private void testNodeJoinsAndLeaves(boolean collocated) throws Exception {
        if (cacheMode() == LOCAL)
            return;

        Set<Integer> set0 = cache().dataStructures().set(SET_NAME, collocated, true);

        final int ITEMS = 10_000;

        for (int i = 0; i < ITEMS; i++)
            set0.add(i);

        startGrid(gridCount());

        try {
            Set<Integer> set1 = cache().dataStructures().set(SET_NAME, collocated, false);

            assertNotNull(set1);

            for (int i = 0; i < gridCount() + 1; i++) {
                Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

                assertEquals(ITEMS, set.size());

                assertSetContent(set, ITEMS);
            }
        }
        finally {
            stopGrid(gridCount());
        }

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, collocated, false);

            assertSetContent(set, ITEMS);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollocation() throws Exception {
        if (cacheMode() != PARTITIONED)
            return;

        final String setName = SET_NAME + "collocated";

        Set<Integer> set0 = grid(0).cache("noBackupsCache").dataStructures().set(setName, true, true);

        try {
            for (int i = 0; i < 1000; i++)
                set0.add(i);

            assertEquals(1000, set0.size());

            UUID setNodeId = null;

            for (int i = 0; i < gridCount(); i++) {
                GridKernal grid = (GridKernal)grid(i);

                Iterator<GridCacheEntryEx<Object, Object>> entries =
                        grid.context().cache().internalCache("noBackupsCache").map().allEntries0().iterator();

                if (entries.hasNext()) {
                    if (setNodeId == null)
                        setNodeId = grid.localNode().id();
                    else
                        fail("For collocated set all items should be stored on single node.");
                }
            }

            assertNotNull(setNodeId);
        }
        finally {
            grid(0).cache("noBackupsCache").dataStructures().removeSet(setName);
        }
    }

    /**
     * @param set Set.
     * @param size Expected size.
     */
    private void assertSetContent(Set<Integer> set, int size) {
        Collection<Integer> data = new HashSet<>(size);

        for (Integer val : set)
            assertTrue(data.add(val));

        assertEquals(size, data.size());

        for (int val = 0; val < size; val++)
            assertTrue(data.contains(val));
    }

    /**
     * @param arr Array.
     * @param size Expected size.
     */
    private void assertArrayContent(Object[] arr, int size) {
        assertEquals(size, arr.length);

        for (int i = 0; i < size; i++) {
            boolean found = false;

            for (Object obj : arr) {
                if (obj.equals(i)) {
                    found = true;

                    break;
                }
            }

            assertTrue(found);
        }
    }
}
