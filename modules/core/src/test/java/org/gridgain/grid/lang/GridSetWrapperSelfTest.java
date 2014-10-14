/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.util.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Set wrapper test.
 */
@GridCommonTest(group = "Lang")
public class GridSetWrapperSelfTest extends GridCommonAbstractTest {

    /** @throws Exception If failed. */
    public void testEmptySet() throws Exception {
        checkCollectionEmptiness(new GridSetWrapper<>(new HashMap<String, Object>()));
    }

    /** @throws Exception If failed. */
    public void testMultipleValuesSet() throws Exception {
        Set<String> set = new GridSetWrapper<>(new HashMap<String, Object>());

        set.add("v1");
        set.add("v2");
        set.add("v3");
        set.add("v4");
        set.add("v5");
        set.add("v6");

        assert !set.isEmpty();

        assertEquals(6, set.size());

        set.add("v5");
        set.add("v6");

        assertEquals(6, set.size());

        assert set.contains("v1");
        assert set.contains("v2");
        assert set.contains("v3");
        assert set.contains("v4");
        assert set.contains("v5");
        assert set.contains("v6");
        assert !set.contains("v7");

        Iterator<String> iter = set.iterator();

        for (int i = 0; i < 6; i++)
            assert iter.next().contains("v");

        assert !iter.hasNext();
    }

    /** @throws Exception If failed. */
    public void testSetRemove() throws Exception {
        Collection<String> set = new GridSetWrapper<>(new HashMap<String, Object>());

        // Put 1 element.
        set.add("v1");

        assert set.remove("v1");
        assert !set.remove("v2");

        checkCollectionEmptiness(set);

        // Put 2 elements.
        set.add("v1");
        set.add("v2");

        assert set.remove("v1");
        assert set.remove("v2");
        assert !set.remove("v3");

        checkCollectionEmptiness(set);

        // Put more than 5 elements.
        set.add("v1");
        set.add("v2");
        set.add("v3");
        set.add("v4");
        set.add("v5");
        set.add("v6");

        assert set.remove("v1");
        assert set.remove("v2");
        assert set.remove("v3");
        assert set.remove("v4");
        assert set.remove("v5");
        assert set.remove("v6");
        assert !set.remove("v7");

        checkCollectionEmptiness(set);
    }

    /** @throws Exception If failed. */
    public void testSetRemoveAll() throws Exception {
        Collection<String> set = new GridSetWrapper<>(new HashMap<String, Object>());

        set.add("v1");
        set.add("v2");
        set.add("v3");
        set.add("v4");
        set.add("v5");

        set.removeAll(GridUtils.addAll(new HashSet<String>(), "v2", "v4", "v5"));

        assertEquals(2, set.size());

        assert set.contains("v1");
        assert !set.contains("v2");
        assert set.contains("v3");
        assert !set.contains("v4");
        assert !set.contains("v5");
    }

    /** @throws Exception If failed. */
    public void testSetClear() throws Exception {
        Collection<String> set = new GridSetWrapper<>(new HashMap<String, Object>());

        set.add("v1");
        set.add("v2");
        set.add("v3");
        set.add("v4");
        set.add("v5");
        set.add("v6");

        assertEquals(6, set.size());

        set.clear();

        checkCollectionEmptiness(set);
    }

    /** @throws Exception If failed. */
    public void testIterator() throws Exception {
        Set<String> set = new GridSetWrapper<>(new HashMap<String, Object>());

        set.add("v1");
        set.add("v2");
        set.add("v3");
        set.add("v4");
        set.add("v5");
        set.add("v6");

        Iterator<String> iter = set.iterator();

        assert iter.hasNext();

        String e = iter.next();

        assert e.contains("v");

        iter.next();

        iter.remove();

        assertEquals(5, set.size());

        assert iter.next() != null;
        assert iter.next() != null;

        iter.remove();

        assertEquals(4, set.size());

        assert iter.next() != null;

        assert iter.next() != null;

        assert !iter.hasNext();
    }

    /**
     * Checks set emptiness.
     *
     * @param c Set to check.
     * @throws Exception If failed.
     */
    private void checkCollectionEmptiness(Collection<?> c) throws Exception {
        assert c.isEmpty();

        assert !c.contains("Some value");

        assertEquals(0, c.size());

        assert !c.iterator().hasNext();

        try {
            c.iterator().next();

            fail("NoSuchElementException must have been thrown.");
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }

        try {
            c.iterator().remove();

            fail("IllegalStateException must have been thrown.");
        }
        catch (IllegalStateException e) {
            info("Caught expected exception: " + e);
        }
    }
}
