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
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Set failover tests.
 */
public class GridCacheSetFailoverAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final String SET_NAME = "testFailoverSet";

    /** */
    private static final long TEST_DURATION = 60_000;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setBackups(1);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setStore(null);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setDistributionMode(PARTITIONED_ONLY);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_DURATION + 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testNodeRestart() throws Exception {
        GridCacheSet<Integer> set = cache().dataStructures().set(SET_NAME, false, true);

        final int ITEMS = 10_000;

        Collection<Integer> items = new ArrayList<>(ITEMS);

        for (int i = 0; i < ITEMS; i++)
            items.add(i);

        set.addAll(items);

        assertEquals(ITEMS, set.size());

        AtomicBoolean stop = new AtomicBoolean();

        GridFuture<?> killFut = startNodeKiller(stop);

        long stopTime = System.currentTimeMillis() + TEST_DURATION;

        try {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (System.currentTimeMillis() < stopTime) {
                for (int i = 0; i < 10; i++) {
                    try {
                        int size = set.size();

                        // TODO: GG-7952, check for equality when GG-7952 fixed.
                        assertTrue(size > 0);
                    }
                    catch (GridRuntimeException ignore) {
                        // No-op.
                    }

                    try {
                        Iterator<Integer> iter = set.iterator();

                        int cnt = 0;

                        while (iter.hasNext()) {
                            assertNotNull(iter.next());

                            cnt++;
                        }

                        // TODO: GG-7952, check for equality when GG-7952 fixed.
                        assertTrue(cnt > 0);
                    }
                    catch (GridRuntimeException ignore) {
                        // No-op.
                    }

                    int val = rnd.nextInt(ITEMS);

                    assertTrue("Not contains: " + val, set.contains(val));

                    val = ITEMS + rnd.nextInt(ITEMS);

                    assertFalse("Contains: " + val, set.contains(val));
                }

                log.info("Remove set.");

                boolean rmv = cache().dataStructures().removeSet(SET_NAME);

                assertTrue(rmv);

                log.info("Create new set.");

                set = cache().dataStructures().set(SET_NAME, false, true);

                set.addAll(items);
            }
        }
        finally {
            stop.set(true);
        }

        killFut.get();

        boolean rmv = cache().dataStructures().removeSet(SET_NAME);

        assertTrue(rmv);

        if (false) { // TODO GG-8962: enable check when fixed.
            int cnt = 0;

            Set<GridUuid> setIds = new HashSet<>();

            for (int i = 0; i < gridCount(); i++) {
                Iterator<GridCacheEntryEx<Object, Object>> entries =
                    ((GridKernal)grid(i)).context().cache().internalCache().map().allEntries0().iterator();

                while (entries.hasNext()) {
                    GridCacheEntryEx<Object, Object> entry = entries.next();

                    if (entry.hasValue()) {
                        cnt++;

                        if (entry.key() instanceof GridCacheSetItemKey) {
                            GridCacheSetItemKey setItem = (GridCacheSetItemKey)entry.key();

                            if (setIds.add(setItem.setId()))
                                log.info("Unexpected set item [setId=" + setItem.setId() +
                                    ", grid: " + grid(i).name() +
                                    ", entry=" + entry + ']');
                        }
                    }
                }
            }

            assertEquals("Found unexpected cache entries", 0, cnt);
        }
    }

    /**
     * Starts thread restarting random node.
     *
     * @param stop Stop flag.
     * @return Future completing when thread finishes.
     */
    private GridFuture<?> startNodeKiller(final AtomicBoolean stop) {
        return GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    int idx = rnd.nextInt(1, gridCount());

                    U.sleep(rnd.nextLong(2000, 3000));

                    log.info("Killing node: " + idx);

                    stopGrid(idx);

                    U.sleep(rnd.nextLong(500, 1000));

                    startGrid(idx);
                }

                return null;
            }
        });
    }
}
