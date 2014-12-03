/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.eviction.random;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.random.*;
import org.gridgain.grid.kernal.processors.cache.eviction.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Random eviction policy test.
 */
public class GridCacheRandomEvictionPolicySelfTest extends
    GridCacheEvictionAbstractTest<GridCacheRandomEvictionPolicy<String, String>> {
    /**
     * @throws Exception If failed.
     */
    public void testMemory() throws Exception {
        try {
            Grid g = startGrid(0);

            int max = 10;

            policy(0).setMaxSize(max);

            int keys = 31;

            for (int i = 0; i < keys; i++) {
                String s = Integer.toString(i);

                g.cache(null).put(s, s);
            }

            assert g.cache(null).size() <= max;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandom() throws Exception {
        try {
            Grid g = startGrid(0);

            int max = 10;

            policy(0).setMaxSize(max);

            Random rand = new Random();

            int keys = 31;

            String[] t = new String[keys];

            for (int i = 0; i < t.length; i++)
                t[i] = Integer.toString(i);

            int runs = 10000;

            for (int i = 0; i < runs; i++) {
                boolean rmv = rand.nextBoolean();

                int j = rand.nextInt(t.length);

                if (rmv)
                    g.cache(null).remove(t[j]);
                else
                    g.cache(null).put(t[j], t[j]);

                if (i % 1000 == 0)
                    info("Stats [cntr=" + i + ", total=" + runs + ']');
            }

            assert g.cache(null).size() <= max;

            info(policy(0));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllowEmptyEntries() throws Exception {
        try {
            startGrid();

            GridCache<String, String> c = cache();

            MockEntry e1 = new MockEntry("1", c);

            e1.setValue("val");

            MockEntry e2 = new MockEntry("2", c);

            MockEntry e3 = new MockEntry("3", c);

            e3.setValue("val");

            MockEntry e4 = new MockEntry("4", c);

            MockEntry e5 = new MockEntry("5", c);

            e5.setValue("val");

            GridCacheRandomEvictionPolicy<String, String> p = policy();

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
    public void testRandomMultiThreaded() throws Exception {
        try {
            final Grid g = startGrid(0);

            int max = 10;

            policy(0).setMaxSize(max);

            final Random rand = new Random();

            int keys = 31;

            final String[] t = new String[keys];

            for (int i = 0; i < t.length; i++)
                t[i] = Integer.toString(i);

            multithreaded(new Callable() {
                @Nullable @Override public Object call() throws GridException {
                    int runs = 3000;

                    for (int i = 0; i < runs; i++) {
                        boolean rmv = rand.nextBoolean();

                        int j = rand.nextInt(t.length);

                        if (rmv)
                            g.cache(null).remove(t[j]);
                        else
                            g.cache(null).put(t[j], t[j]);

                        if (i != 0 && i % 1000 == 0)
                            info("Stats [cntr=" + i + ", total=" + runs + ']');
                    }

                    return null;
                }
            }, 10);

            assert g.cache(null).size() <= max;

            info(policy(0));
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override public void testPartitionedNearDisabled() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void _testPartitionedNearEnabled() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testPartitionedNearDisabledMultiThreaded() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testPartitionedNearDisabledBackupSyncMultiThreaded() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testPartitionedNearEnabledMultiThreaded() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testPartitionedNearEnabledBackupSyncMultiThreaded() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected GridCacheRandomEvictionPolicy<String, String> createPolicy(int plcMax) {
        return new GridCacheRandomEvictionPolicy<>(plcMax);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheRandomEvictionPolicy<String, String> createNearPolicy(int nearMax) {
        return new GridCacheRandomEvictionPolicy<>(plcMax);
    }

    /** {@inheritDoc} */
    @Override protected void checkNearPolicies(int nearMax) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void checkPolicies(int plcMax) {
        // No-op.
    }
}
