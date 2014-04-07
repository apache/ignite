/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.colocated.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Cache DGC test.
 */
public class GridCacheDgcManagerNonTxNearDisabledSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 4;

    /** */
    private static final int KEYS_CNT = NODES_CNT * 5;

    /** */
    private static final int DGC_FREQ = 5000;

    /** */
    private static final int DGC_SUSPECT_LOCK_TIMEOUT = 1000;

    /** */
    private static final long DFLT_LOCK_TIMEOUT = 10000;

    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (Grid g : G.allGrids())
            for (GridCache<?, ?> cache : g.caches())
                cache.clearAll();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration partCacheCfg = defaultCacheConfiguration();

        partCacheCfg.setName("partitioned");

        partCacheCfg.setCacheMode(PARTITIONED);
        partCacheCfg.setBackups(1);
        partCacheCfg.setPreloadMode(SYNC);
        partCacheCfg.setDgcFrequency(DGC_FREQ);
        partCacheCfg.setDgcSuspectLockTimeout(DGC_SUSPECT_LOCK_TIMEOUT);
        partCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        partCacheCfg.setEvictionPolicy(null);
        partCacheCfg.setNearEvictionPolicy(null);
        partCacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        partCacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(partCacheCfg);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** @throws Exception In failed. */
    public void testDgcPartitioned() throws Exception {
        GridCacheContext<Integer, Integer> cctx =
            ((GridKernal)grid(0)).<Integer, Integer>internalCache("partitioned").context();

        long topVer = cctx.discovery().topologyVersion();

        for (int key = 0; key < KEYS_CNT; key++) {
            // Resolve primary node for key.
            GridNode primaryNode = F.first(cctx.affinity().nodes(new Integer(key), topVer));

            assert primaryNode != null;

            GridKernal grid = (GridKernal)G.grid(primaryNode.id());

            GridDhtColocatedCache<Integer, Integer> cache =
                (GridDhtColocatedCache<Integer, Integer>)grid.<Integer, Integer>internalCache("partitioned");

            // Put key/value to primary and backup nodes.
            cache.put(key, key, null);

            // Lock it on primary and backup nodes.
            cache.lock(key, DFLT_LOCK_TIMEOUT, null);

            GridCacheEntryEx<Integer, Integer> entry = cache.peekEx(key);

            assert entry != null;

            GridCacheMvccCandidate<Integer> dhtOwner = entry.localOwner();

            assert dhtOwner != null;

            // Remove local lock in explicit lock map and dht caches, but remote locks still exist.
            cache.context().mvcc().removeExplicitLock(Thread.currentThread().getId(), key, null);
            assert entry.removeLock(dhtOwner.version());
        }

        info("Finished data population. Going to sleep...");

        // We cannot ensure that entries are locked properly on all nodes,
        // since locks might have already been removed by DGC.

        Thread.sleep(DGC_FREQ * NODES_CNT);

        for (int i = 0; i < NODES_CNT; i++) {
            GridKernal grid = (GridKernal)grid(i);

            GridDhtColocatedCache<Integer, Integer> cache =
                (GridDhtColocatedCache<Integer, Integer>)grid.<Integer, Integer>internalCache("partitioned");

            assert cache.context().mvcc().remoteCandidates().isEmpty();
            assert cache.context().mvcc().localCandidates().isEmpty();

            for (int j = 0; j < KEYS_CNT; j++) {
                assert !cache.isLocked(j) : "Key should not be locked in colocated [nodeIdx=" + i + ", key=" + j + "]";

                GridCacheEntryEx<Integer, Integer> entry = cache.peekEx(j);

                if (entry != null)
                    assert !entry.lockedByAny();
            }
        }
    }
}
