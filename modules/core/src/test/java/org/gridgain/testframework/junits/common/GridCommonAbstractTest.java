/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.junits.common;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.colocated.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.cache.local.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.*;

import javax.net.ssl.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Super class for all common tests.
 */
public abstract class GridCommonAbstractTest extends GridAbstractTest {
    /**
     * @param startGrid If {@code true}, then grid node will be auto-started.
     */
    protected GridCommonAbstractTest(boolean startGrid) {
        super(startGrid);
    }

    /** */
    protected GridCommonAbstractTest() {
        super(false);
    }

    /**
     * @param idx Grid index.
     * @return Cache.
     */
    protected <K, V> GridCache<K, V> cache(int idx) {
        return grid(idx).cachex();
    }

    /**
     * @param idx Grid index.
     * @param name Cache name.
     * @return Cache.
     */
    protected <K, V> GridCache<K, V> cache(int idx, String name) {
        return grid(idx).cachex(name);
    }

    /**
     * @return Cache.
     */
    protected <K, V> GridCache<K, V> cache() {
        return grid().cachex();
    }

    /**
     * @return Cache.
     */
    protected <K, V> GridLocalCache<K, V> local() {
        return (GridLocalCache<K, V>)((GridKernal)grid()).<K, V>internalCache();
    }

    /**
     * @param cache Cache.
     * @return DHT cache.
     */
    protected static <K, V> GridDhtCacheAdapter<K, V> dht(GridCacheProjection<K,V> cache) {
        return nearEnabled(cache) ? near(cache).dht() :
            ((GridKernal)cache.gridProjection().grid()).<K, V>internalCache(cache.name()).context().dht();
    }

    /**
     * @return DHT cache.
     */
    protected <K, V> GridDhtCacheAdapter<K, V> dht() {
        return this.<K, V>near().dht();
    }

    /**
     * @param idx Grid index.
     * @return DHT cache.
     */
    protected <K, V> GridDhtCacheAdapter<K, V> dht(int idx) {
        return this.<K, V>near(idx).dht();
    }

    /**
     * @param idx Grid index.
     * @param cache Cache name.
     * @return DHT cache.
     */
    protected <K, V> GridDhtCacheAdapter<K, V> dht(int idx, String cache) {
        return this.<K, V>near(idx, cache).dht();
    }

    /**
     * @param idx Grid index.
     * @param cache Cache name.
     * @return Colocated cache.
     */
    protected <K, V> GridDhtColocatedCache<K, V> colocated(int idx, String cache) {
        return (GridDhtColocatedCache<K, V>)((GridKernal)grid(idx)).internalCache(cache);
    }

    /**
     * @param cache Cache.
     * @return {@code True} if near cache is enabled.
     */
    protected static <K, V> boolean nearEnabled(GridCacheProjection<K,V> cache) {
        GridCacheConfiguration cfg = ((GridKernal)cache.gridProjection().grid()).
            <K, V>internalCache(cache.name()).context().config();

        return isNearEnabled(cfg);
    }

    /**
     * @param cache Cache.
     * @return Near cache.
     */
    protected static <K, V> GridNearCacheAdapter<K, V> near(GridCacheProjection<K,V> cache) {
        return ((GridKernal)cache.gridProjection().grid()).<K, V>internalCache(cache.name()).context().near();
    }

    /**
     * @param cache Cache.
     * @return Colocated cache.
     */
    protected static <K, V> GridDhtColocatedCache<K, V> colocated(GridCacheProjection<K,V> cache) {
        return ((GridKernal)cache.gridProjection().grid()).<K, V>internalCache(cache.name()).context().colocated();
    }

    /**
     * @return Near cache.
     */
    protected <K, V> GridNearCacheAdapter<K, V> near() {
        return ((GridKernal)grid()).<K, V>internalCache().context().near();
    }

    /**
     * @param idx Grid index.
     * @return Near cache.
     */
    protected <K, V> GridNearCacheAdapter<K, V> near(int idx) {
        return ((GridKernal)grid(idx)).<K, V>internalCache().context().near();
    }

    /**
     * @param idx Grid index.
     * @return Colocated cache.
     */
    protected <K, V> GridDhtColocatedCache<K, V> colocated(int idx) {
        return (GridDhtColocatedCache<K, V>)((GridKernal)grid(idx)).<K, V>internalCache();
    }

    /**
     * @param idx Grid index.
     * @param cache Cache name.
     * @return Near cache.
     */
    protected <K, V> GridNearCacheAdapter<K, V> near(int idx, String cache) {
        return ((GridKernal)grid(idx)).<K, V>internalCache(cache).context().near();
    }

    /** {@inheritDoc} */
    @Override protected final boolean isJunitFrameworkClass() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected final void setUp() throws Exception {
        // Disable SSL hostname verifier.
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override public boolean verify(String s, SSLSession sslSes) {
                return true;
            }
        });

        getTestCounters().incrementStarted();

        super.setUp();
    }

    /** {@inheritDoc} */
    @Override protected final void tearDown() throws Exception {
        getTestCounters().incrementStopped();

        super.tearDown();
    }

    /** {@inheritDoc} */
    @Override protected final Grid startGridsMultiThreaded(int cnt) throws Exception {
        Grid g = super.startGridsMultiThreaded(cnt);

        awaitPartitionMapExchange();

        return g;
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    @SuppressWarnings("BusyWait")
    protected void awaitPartitionMapExchange() throws InterruptedException {
        for (Grid g : G.allGrids()) {
            for (GridCache<?, ?> c : ((GridEx)g).cachesx()) {
                GridCacheConfiguration cfg = c.configuration();

                if (cfg.getCacheMode() == PARTITIONED && cfg.getPreloadMode() != NONE && g.nodes().size() > 1) {
                    GridCacheAffinityFunction aff = cfg.getAffinity();

                    GridDhtCacheAdapter<?, ?> dht = dht(c);

                    GridDhtPartitionTopology<?, ?> top = dht.topology();

                    for (int p = 0; p < aff.partitions(); p++) {
                        long start = 0;

                        for (int i = 0; ; i++) {
                            // Must map on updated version of topology.
                            Collection<GridNode> affNodes = c.affinity().mapPartitionToPrimaryAndBackups(p);

                            int exp = affNodes.size();

                            Collection<GridNode> owners = top.nodes(p, -1);

                            int actual = owners.size();

                            if (affNodes.size() != owners.size() || !affNodes.containsAll(owners)) {
                                LT.warn(log(), null, "Waiting for topology map update [grid=" + g.name() +
                                    ", p=" + p + ", nodes=" + exp + ", owners=" + actual +
                                    ", affNodes=" + affNodes + ", owners=" + owners +
                                    ", locNode=" + g.localNode().id() + ']');

                                if (i == 0)
                                    start = System.currentTimeMillis();

                                Thread.sleep(200); // Busy wait.

                                continue;
                            }

                            if (i > 0)
                                log().warning("Finished waiting for topology map update [grid=" + g.name() +
                                    ", p=" + p + ", duration=" + (System.currentTimeMillis() - start) + "ms]");

                            break;
                        }
                    }
                }
            }
        }
    }
}
