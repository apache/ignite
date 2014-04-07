/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.lang.GridFunc;

import java.util.*;

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
        cache().dataStructures().removeSet(SET_NAME);

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateRemove() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            assertNull(cache(i).dataStructures().set(SET_NAME, false));

        Set<Integer> set = cache().dataStructures().set(SET_NAME, true);

        assertNotNull(set);
        assertTrue(set.isEmpty());
        assertEquals(0, set.size());

        for (int i = 0; i < gridCount(); i++)
            assertNotNull(cache(i).dataStructures().set(SET_NAME, false));

        assertTrue(cache().dataStructures().removeSet(SET_NAME));

        for (int i = 0; i < gridCount(); i++) {
            assertNull(cache(i).dataStructures().set(SET_NAME, false));
            assertFalse(cache(i).dataStructures().removeSet(SET_NAME));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testApi() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            assertFalse(cache(i).dataStructures().set(SET_NAME, true).contains(1));

        assertTrue(cache().dataStructures().set(SET_NAME, false).add(1));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, false);

            assertEquals(1, set.size());
            assertTrue(set.contains(1));

            assertFalse(set.add(1));

            assertFalse(set.contains(100));
        }

        assertTrue(cache().dataStructures().set(SET_NAME, true).remove(1));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, false);

            assertEquals(0, set.size());
            assertTrue(set.isEmpty());

            assertFalse(set.contains(1));

            assertFalse(set.remove(1));
        }

        Collection<Integer> col1 = new ArrayList<>();
        Collection<Integer> col2 = new ArrayList<>();

        final int ITEMS = 100;

        for (int i = 0; i < ITEMS; i++) {
            assertTrue(cache(i % gridCount()).dataStructures().set(SET_NAME, false).add(i));

            col1.add(i);
            col2.add(i);
        }

        col2.add(ITEMS);

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, false);

            assertEquals(ITEMS, set.size());
            assertTrue(set.containsAll(col1));
            assertFalse(set.containsAll(col2));
        }

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, false);

            assertArrayContent(set.toArray(), ITEMS);
            assertArrayContent(set.toArray(new Integer[ITEMS]), ITEMS);
        }

        Collection<Integer> rmvCol = new ArrayList<>();

        for (int i = ITEMS - 10; i < ITEMS; i++)
            rmvCol.add(i);

        assertTrue(cache().dataStructures().set(SET_NAME, true).removeAll(rmvCol));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, false);

            assertFalse(set.removeAll(rmvCol));

            for (Integer val : rmvCol)
                assertFalse(set.contains(val));

            assertArrayContent(set.toArray(), ITEMS - 10);
            assertArrayContent(set.toArray(new Integer[ITEMS - 10]), ITEMS - 10);
        }

        assertTrue(cache().dataStructures().set(SET_NAME, true).addAll(rmvCol));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, false);

            assertEquals(ITEMS, set.size());

            assertFalse(set.addAll(rmvCol));

            for (Integer val : rmvCol)
                assertTrue(set.contains(val));
        }

        assertTrue(cache().dataStructures().set(SET_NAME, true).retainAll(rmvCol));

        for (int i = 0; i < gridCount(); i++) {
            Set<Integer> set = cache(i).dataStructures().set(SET_NAME, false);

            assertEquals(rmvCol.size(), set.size());

            assertFalse(set.retainAll(rmvCol));
        }
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

            assertTrue("Not found item " + i, found);
        }
    }
}
