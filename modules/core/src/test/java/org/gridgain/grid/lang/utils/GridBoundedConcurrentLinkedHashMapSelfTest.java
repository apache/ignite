/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang.utils;

import org.gridgain.grid.util.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Test for {@link GridBoundedConcurrentLinkedHashMap}.
 */
public class GridBoundedConcurrentLinkedHashMapSelfTest extends GridCommonAbstractTest {
    /** Bound. */
    private static final int MAX = 3;

    /**
     * @throws Exception If failed.
     */
    public void testBound() throws Exception {
        Map<Integer, Integer> map = new GridBoundedConcurrentLinkedHashMap<>(MAX);

        for  (int i = 1; i <= 10; i++) {
            map.put(i, i);

            if (i <= MAX)
                assert map.size() == i;
            else
                assert map.size() == MAX;
        }

        assert map.size() == MAX;

        Iterator<Integer> it = map.values().iterator();

        assert it.next() == 8;
        assert it.next() == 9;
        assert it.next() == 10;
    }
}
