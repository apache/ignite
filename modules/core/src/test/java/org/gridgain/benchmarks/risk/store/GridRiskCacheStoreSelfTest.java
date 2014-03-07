/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.benchmarks.risk.store;

import org.gridgain.benchmarks.risk.affinity.*;
import org.gridgain.benchmarks.risk.jobs.*;
import org.gridgain.benchmarks.risk.model.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.benchmarks.risk.GridRiskMain.*;
import static org.gridgain.benchmarks.risk.model.GridRiskDataEntry.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 *
 */
public class GridRiskCacheStoreSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private GridCacheStore<?, ?> store;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(getCacheConfiguration(null));
        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private GridCacheConfiguration getCacheConfiguration(@Nullable String name) {
        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setName(name);
        cc.setCacheMode(PARTITIONED);
        cc.setStore(store);
        cc.setStartSize(500000);
        cc.setQueryIndexEnabled(false);
        cc.setDistributionMode(PARTITIONED_ONLY);
        cc.setStoreValueBytes(false);
        cc.setBackups(1);

        GridRiskPartitionedAffinityFunction aff = new GridRiskPartitionedAffinityFunction();

        aff.setFactor1(FACTOR1);
        aff.setFactor2(FACTOR2);

        cc.setAffinity(aff);

        return cc;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadFromStoreApproach1() throws Exception {
        store = new GridRiskApproach1CacheStore();

        startGridsMultiThreaded(1);

        try {
            for (int i = 0; i < 10; i++) {
                grid(0).compute().call(new GridRiskLoadCacheJob(null, ENTRIES_COUNT / 10)).get();

                for (Grid g : G.allGrids()) {
                    GridCache<GridRiskAffinityKey, Map<GridRiskDataKey, GridRiskDataEntry>> cache = g.cache(null);

                    X.println("Examining cache [id=" + g.localNode().id() + ", size=" + cache.size() + ']');

                    int totalSize = 0;

                    for (GridRiskAffinityKey affKey : cache.keySet()) {
                        Map<GridRiskDataKey, GridRiskDataEntry> entries = cache.get(affKey);

                        X.println("Examining map [id=" + g.localNode().id() + ", affKey=" + affKey + ", size=" +
                            entries.size() + ']');

                        totalSize += entries.size();
                    }

                    X.println("Total entries count [id=" + g.localNode().id() + ", totalSize=" + totalSize + ']');

                    assert !cache.isEmpty();
                }

                grid(0).compute().call(new Callable<Object>() {
                        @GridInstanceResource
                        private Grid g;

                        @Override public Object call() throws Exception {
                            g.cache(null).clearAll();

                            return null;
                        }
                    }).get();

                for (Grid g : G.allGrids()) {
                    X.println("Examining cache [id=" + g.localNode().id() + ", size=" + g.cache(null).size() + ']');

                    assert g.cache(null).isEmpty();
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadFromStoreApproach2() throws Exception {
        store = new GridRiskApproach2CacheStore();

        startGridsMultiThreaded(1);

        try {
            for (int i = 0; i < 10; i++) {
                grid(0).compute().call(new GridRiskLoadCacheJob(null, ENTRIES_COUNT / 10)).get();

                for (Grid g : G.allGrids()) {
                    assert !g.cache(null).isEmpty();

                    X.println("Examining cache [id=" + g.localNode().id() + ", size=" + g.cache(null).size() + ']');
                }

                grid(0).compute().call(new Callable<Object>() {
                        @GridInstanceResource
                        private Grid g;

                        @Override public Object call() throws Exception {
                            g.cache(null).clearAll();

                            return null;
                        }
                    }).get();

                for (Grid g : G.allGrids()) {
                    assert g.cache(null).isEmpty();

                    X.println("Examining cache [id=" + g.localNode().id() + ", size=" + g.cache(null).size() + ']');
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
