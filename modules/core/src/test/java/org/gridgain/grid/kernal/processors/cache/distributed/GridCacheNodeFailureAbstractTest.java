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
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.GridGainState.*;
import static org.gridgain.grid.GridSystemProperties.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Tests for node failure in transactions.
 */
public abstract class GridCacheNodeFailureAbstractTest extends GridCommonAbstractTest {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** */
    private static final Integer KEY = 1;

    /** */
    private static final String VALUE = "test";

    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Grid instances. */
    private static final List<Ignite> IGNITEs = new ArrayList<>();

    /**
     * Start grid by default.
     */
    protected GridCacheNodeFailureAbstractTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setDeploymentMode(GridDeploymentMode.SHARED);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            IGNITEs.add(startGrid(i));
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        IGNITEs.clear();
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            if (GridGain.state(IGNITEs.get(i).name()) == STOPPED) {
                info("Restarting grid: " + i);

                IGNITEs.set(i, startGrid(i));
            }

            GridCacheEntry e = cache(i).entry(KEY);

            assert !e.isLocked() : "Entry is locked for grid [idx=" + i + ", entry=" + e + ']';
        }
    }

    /**
     * @param i Grid index.
     * @return Cache.
     */
    @Override protected <K, V> GridCache<K, V> cache(int i) {
        return IGNITEs.get(i).cache(null);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticReadCommitted() throws Throwable {
        // TODO:  GG-7437.
        if (cache(0).configuration().getCacheMode() == GridCacheMode.REPLICATED)
            return;

        checkTransaction(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticRepeatableRead() throws Throwable {
        checkTransaction(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testPessimisticSerializable() throws Throwable {
        checkTransaction(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If check failed.
     */
    private void checkTransaction(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) throws Throwable {
        int idx = RAND.nextInt(GRID_CNT);

        info("Grid will be stopped: " + idx);

        Ignite g = grid(idx);

        GridCache<Integer, String> cache = cache(idx);

        GridCacheTx tx = cache.txStart(concurrency, isolation);

        try {
            cache.put(KEY, VALUE);

            int checkIdx = (idx + 1) % G.allGrids().size();

            info("Check grid index: " + checkIdx);

            GridFuture<?> f = waitForLocalEvent(grid(checkIdx).events(), new P1<GridEvent>() {
                @Override public boolean apply(GridEvent e) {
                    info("Received grid event: " + e);

                    return true;
                }
            }, EVT_NODE_LEFT);

            stopGrid(idx);

            f.get();

            U.sleep(getInteger(GG_TX_SALVAGE_TIMEOUT, 3000));

            GridCache<Integer, String> checkCache = cache(checkIdx);

            boolean locked = false;

            for (int i = 0; !locked && i < 3; i++) {
                locked = checkCache.lock(KEY, -1);

                if (!locked)
                    U.sleep(500);
                else
                    break;
            }

            try {
                assert locked : "Failed to lock key on cache [idx=" + checkIdx + ", key=" + KEY + ']';
            }
            finally {
                checkCache.unlockAll(F.asList(KEY));
            }
        }
        catch (GridCacheTxOptimisticException e) {
            U.warn(log, "Optimistic transaction failure (will rollback) [msg=" + e.getMessage() + ", tx=" + tx + ']');

            if (G.state(g.name()) == GridGainState.STARTED)
                tx.rollback();

            assert concurrency == OPTIMISTIC && isolation == SERIALIZABLE;
        }
        catch (Throwable e) {
            error("Transaction failed (will rollback): " + tx, e);

            if (G.state(g.name()) == GridGainState.STARTED)
                tx.rollback();

            throw e;
        }
    }

    /**
     * @throws Exception If check failed.
     */
    public void testLock() throws Exception {
        int idx = 0;

        info("Grid will be stopped: " + idx);

        info("Primary node for key [id=" + grid(idx).mapKeyToNode(null, KEY) + ", key=" + KEY + ']');

        GridCache<Integer, String> cache = cache(idx);

        // TODO:  GG-7437.
        if (cache.configuration().getCacheMode() == GridCacheMode.REPLICATED)
            return;

        cache.put(KEY, VALUE);

        assert cache.lock(KEY, -1);

        int checkIdx = 1;

        info("Check grid index: " + checkIdx);

        GridCache<Integer, String> checkCache = cache(checkIdx);

        assert !checkCache.lock(KEY, -1);

        GridCacheEntry e = checkCache.entry(KEY);

        assert e.isLocked() : "Entry is not locked for grid [idx=" + checkIdx + ", entry=" + e + ']';

        GridFuture<?> f = waitForLocalEvent(grid(checkIdx).events(), new P1<GridEvent>() {
            @Override public boolean apply(GridEvent e) {
                info("Received grid event: " + e);

                return true;
            }
        }, EVT_NODE_LEFT);

        stopGrid(idx);

        f.get();

        boolean locked = false;

        for (int i = 0; !locked && i < 3; i++) {
            locked = checkCache.lock(KEY, -1);

            if (!locked)
                U.sleep(1500);
            else
                break;
        }

        assert locked;

        checkCache.unlockAll(F.asList(KEY));

        e = checkCache.entry(KEY);

        assert !e.isLocked() : "Entry is locked for grid [idx=" + checkIdx + ", entry=" + e + ']';
    }
}
