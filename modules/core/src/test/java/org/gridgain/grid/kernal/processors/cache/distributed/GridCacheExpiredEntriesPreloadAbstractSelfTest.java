/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Tests preloading of expired entries.
 */
public abstract class GridCacheExpiredEntriesPreloadAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setPreloadMode(SYNC);
        cfg.setStore(null);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testExpiredEntriesPreloading() throws Exception {
        GridCache<String, Integer> cache0 = cache(0);

        final int KEYS_NUM = 3;

        for (int i = 0; i < KEYS_NUM; i++)
            cache0.put(String.valueOf(i), 0);

        for (int i = 0; i < KEYS_NUM; i++) {
            GridCacheEntry<String, Integer> entry = cache0.entry(String.valueOf(i));

            entry.timeToLive(100);
            entry.setValue(i);
        }

        // Allow entries to expire.
        U.sleep(1000);

        // Ensure entries expiration.
        for (int i = 0; i < KEYS_NUM; i++)
            assert cache0.get(String.valueOf(i)) == null;

        // Start another node.
        Ignite g1 = startGrid(1);

        final GridCacheAdapter<String, Integer> cache1 = ((GridKernal)g1).context().cache().internalCache();

        cache1.preloader().syncFuture().get();

        Collection<GridEvent> evts = g1.events().localQuery(F.<GridEvent>alwaysTrue(), EVT_CACHE_PRELOAD_OBJECT_LOADED);

        assertEquals("Expected all entries are preloaded.", KEYS_NUM, evts.size());

        boolean rmv = GridTestUtils.waitForCondition(new PAX() {
            @Override public boolean applyx() {
                return cache1.isEmpty();
            }
        }, 10_000);

        assertTrue("Expired entries were not removed.", rmv);
    }
}
