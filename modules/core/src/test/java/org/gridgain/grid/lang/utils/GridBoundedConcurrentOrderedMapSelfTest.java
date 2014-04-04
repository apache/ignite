/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang.utils;

import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Test for {@link GridBoundedConcurrentOrderedMap}.
 */
@GridCommonTest(group = "Lang")
public class GridBoundedConcurrentOrderedMapSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testEvictionSingleElement() {
        SortedMap<Integer,String> m = new GridBoundedConcurrentOrderedMap<>(1);

        m.put(0, "0");

        assertEquals(1, m.size());

        for (int i = 1; i <= 10; i++) {
            m.put(i, Integer.toString(i));

            assertEquals(1, m.size());
        }

        assertEquals(1, m.size());
        assertEquals(Integer.valueOf(10), m.lastKey());
    }

    /**
     *
     */
    public void testEvictionListener() {
        GridBoundedConcurrentOrderedMap<Integer,String> m = new GridBoundedConcurrentOrderedMap<>(1);

        final AtomicInteger evicted = new AtomicInteger();

        m.evictionListener(new CI2<Integer, String>() {
            @Override public void apply(Integer k, String v) {
                assertEquals(Integer.toString(k), v);
                assertEquals(evicted.getAndIncrement(), k.intValue());
            }
        });

        m.put(0, "0");

        assertEquals(1, m.size());

        for (int i = 1; i <= 10; i++) {
            m.put(i, Integer.toString(i));

            assertEquals(1, m.size());
        }

        assertEquals(1, m.size());
        assertEquals(10, m.lastKey().intValue());
        assertEquals(10, evicted.get());
    }
}
