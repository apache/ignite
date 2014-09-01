/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.eclipse.jetty.util.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Test for TRANSFORM events recording.
 */
@SuppressWarnings("ConstantConditions")
public class GridCacheTransformEventSelfTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int GRID_CNT = 3;

    /** Backups count for partitioned cache. */
    private static final int BACKUP_CNT = 1;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Closure name. */
    private static final String CLO_NAME = Transformer.class.getName();

    /** Key. */
    private static final int KEY = 1;

    /** Nodes. */
    private Grid[] grids;

    /** Node IDs. */
    private UUID[] ids;

    /** Caches. */
    private GridCache<Integer, Integer>[] caches;

    /** Recorded events.*/
    private ConcurrentHashSet<GridCacheEvent> evts;

    /** Cache mode. */
    private GridCacheMode cacheMode;

    /** Atomicity mode. */
    private GridCacheAtomicityMode atomicityMode;

    /** TX concurrency. */
    private GridCacheTxConcurrency txConcurrency;

    /** TX isolation. */
    private GridCacheTxIsolation txIsolation;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setName(CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);

        ccfg.setDefaultTxConcurrency(txConcurrency);
        ccfg.setDefaultTxIsolation(txIsolation);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(BACKUP_CNT);

        cfg.setCacheConfiguration(ccfg);

        cfg.setIncludeEventTypes(EVT_CACHE_OBJECT_READ);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        grids = null;
        ids = null;
        caches = null;

        evts=  null;
    }

    /**
     * Initialization routine.
     *
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param txConcurrency TX concurrency.
     * @param txIsolation TX isolation.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void initialize(GridCacheMode cacheMode, GridCacheAtomicityMode atomicityMode,
        GridCacheTxConcurrency txConcurrency, GridCacheTxIsolation txIsolation) throws Exception {
        this.cacheMode = cacheMode;
        this.atomicityMode = atomicityMode;
        this.txConcurrency = txConcurrency;
        this.txIsolation = txIsolation;

        evts = new ConcurrentHashSet<>();

        startGrids(GRID_CNT);

        grids = new Grid[GRID_CNT];
        ids = new UUID[GRID_CNT];
        caches = new GridCache[GRID_CNT];

        for (int i = 0; i < GRID_CNT; i++) {
            grids[i] = grid(i);

            ids[i] = grids[i].localNode().id();

            caches[i] = grids[i].cache(CACHE_NAME);

            caches[i].put(KEY, 1);

            grids[i].events().localListen(new GridPredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    evts.add((GridCacheEvent)evt);

                    return true;
                }
            }, EVT_CACHE_OBJECT_READ);
        }
    }

    /**
     * Test ATOMIC cache with
     * @throws Exception
     */
    public void testAtomicLocalSingle() throws Exception {
        initialize(LOCAL, ATOMIC, null, null);

        caches[0].transform(KEY, new Transformer());

        checkEventNodeIdsStrict(ids[0]);
    }

    /**
     * Ensure that events were recorded on the given nodes.
     *
     * @param ids Event IDs.
     */
    private void checkEventNodeIdsStrict(UUID... ids) {
        if (ids == null) {
            assertTrue(evts.isEmpty());
        }
        else {
            assertEquals(ids.length, evts.size());

            for (UUID id : ids) {
                boolean found = false;

                for (GridCacheEvent evt : evts) {
                    if (F.eq(id, evt.eventNode().id())) {
                        assertEquals(CLO_NAME, evt.closureClassName());

                        found = true;

                        break;
                    }
                }

                if (!found) {
                    GridCache<Integer, Integer> affectedCache = null;

                    for (int i = 0; i < GRID_CNT; i++) {
                        if (F.eq(this.ids[i], id)) {
                            affectedCache = caches[i];

                            break;
                        }
                    }

                    GridCacheEntry<Integer, Integer> entry = affectedCache.entry(KEY);

                    fail("Expected transform event was not triggered on the node [nodeId=" + id +
                        ", primary=" + entry.primary() + ", backup=" + entry.backup() + ']');
                }
            }
        }
    }

    /**
     * Transform closure.
     */
    private static class Transformer implements GridClosure<Integer, Integer>, Serializable {
        /** {@inheritDoc} */
        @Override public Integer apply(Integer val) {
            return ++val;
        }
    }
}
